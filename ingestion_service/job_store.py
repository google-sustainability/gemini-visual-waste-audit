# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Firestore job lifecycle state machine and optimistic concurrency helpers."""

from collections.abc import Iterable, Mapping, Sequence
import dataclasses
import datetime
import enum
import hashlib
import logging
from typing import Any, cast

from google.api_core import exceptions as google_exceptions
from google.auth import exceptions as auth_exceptions
from google.cloud import firestore

_LOGGER = logging.getLogger(__name__)


class JobStatus(enum.StrEnum):
  """Firestore lifecycle states for an image audit job."""

  QUEUED = "QUEUED"
  PROCESSING = "PROCESSING"
  PROCESSED = "PROCESSED"
  FAILED_RETRY = "FAILED_RETRY"
  FAILED_PERMANENT = "FAILED_PERMANENT"
  DELETING = "DELETING"


class EnqueueOutcome(enum.StrEnum):
  """Result of reconciling and enqueueing a single prompt version job."""

  QUEUED = "queued"
  SKIPPED = "skipped"
  DEFERRED_IN_FLIGHT = "deferred_in_flight"


class DeleteAction(enum.StrEnum):
  """Reconciliation outcome for an incoming GCS object delete event."""

  DELETE = "delete"
  DEFER_IN_FLIGHT = "defer_in_flight"
  SKIP_STALE_GENERATION = "skip_stale_generation"


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReconcileDecision:
  """Decision returned when comparing an incoming event to Firestore state.

  Attributes:
    outcome: Whether to enqueue a task, skip the event, or defer while a prior
      generation is still running.
    delete_superseded_rows: Whether existing BigQuery rows for `(gcs_uri,
      prompt_version_id)` must be deleted before dispatching a replacement task.
  """

  outcome: EnqueueOutcome
  delete_superseded_rows: bool = False


@dataclasses.dataclass(frozen=True, kw_only=True)
class DeletePlan:
  """Execution plan for handling a GCS object delete CloudEvent.

  Attributes:
    action: Whether to hard-delete, defer while a job runs, or skip a stale
      generation.
    target_docs: Settled Firestore job document snapshots to delete when
      `action` is `DeleteAction.DELETE`.
  """

  action: DeleteAction
  target_docs: tuple[firestore.DocumentSnapshot, ...] = ()


@dataclasses.dataclass(frozen=True, kw_only=True)
class ClaimedQueuedJob:
  """Lease state for a Firestore job document marked `QUEUED` before dispatch.

  Each `(gcs_uri, prompt_version_id)` audit job is stored as a Firestore
  document. Marking it `QUEUED` reserves an in-flight lease before BigQuery
  cleanup and Cloud Tasks creation so concurrent `/ingest` events skip or defer.

  Attributes:
    reference: `DocumentReference` of the leased Firestore job document.
    existed_before_claim: Whether `reference` existed prior to the claim.
    previous_data: Stored fields captured before transitioning to `QUEUED`.
    claimed_keys: Field names written by the `QUEUED` claim payload.
    claim_update_time: Server `update_time` from the `QUEUED` write, used as a
      `last_update_time` precondition on rollback, or `None` if unavailable.
  """

  reference: firestore.DocumentReference
  existed_before_claim: bool
  previous_data: Mapping[str, Any]
  claimed_keys: tuple[str, ...]
  claim_update_time: Any = None


@dataclasses.dataclass(frozen=True, kw_only=True)
class ClaimedDeleteDoc:
  """Lease state for a Firestore job document marked `DELETING`.

  Each `(gcs_uri, prompt_version_id)` audit job is stored as a Firestore
  document. Marking it `DELETING` reserves a temporary lease so concurrent
  `/ingest` or `/delete` events defer while BigQuery rows are purged.

  Attributes:
    reference: `DocumentReference` of the leased Firestore job document.
    previous_data: Stored fields captured before transitioning to `DELETING`.
    claim_update_time: Server `update_time` from the `DELETING` write, used as a
      `last_update_time` precondition on commit or rollback, or `None` if
      unavailable.
  """

  reference: firestore.DocumentReference
  previous_data: Mapping[str, Any]
  claim_update_time: Any = None


_FIRESTORE_BATCH_LIMIT = 500
_CLOUD_TASKS_MAX_ATTEMPTS = 5
_MAX_INT64 = (1 << 63) - 1
_MAX_INT64_DIGITS = len(str(_MAX_INT64))
_RETRY_IN_FLIGHT_WINDOW = datetime.timedelta(minutes=10)
_DELETING_CLAIM_WINDOW = datetime.timedelta(minutes=10)
_MAX_IN_FLIGHT_JOB_AGE = datetime.timedelta(hours=2)
_RETRYABLE_FAILURE_STATUSES = (
    JobStatus.FAILED_RETRY,
    JobStatus.FAILED_PERMANENT,
)
_STALE_RETRY_JOB_FIELDS = (
    "attempts",
    "deleting_at",
    "error_message",
    "last_processed_at",
)


def make_job_doc_id(gcs_uri: str, prompt_version_id: str) -> str:
  """Returns the deterministic SHA-256 Firestore document ID for a job."""
  composite_key = f"{gcs_uri}:{prompt_version_id}"
  return hashlib.sha256(composite_key.encode()).hexdigest()


def parse_generation(raw_value: Any) -> int | None:
  """Parses a positive GCS object generation into an `int`, or `None`.

  Args:
    raw_value: Raw `generation` field from an Eventarc JSON payload or Firestore
      job document.

  Returns:
    The parsed positive 64-bit integer generation, or `None` if `raw_value` is
    unset, empty, a `bool`, non-numeric, non-positive (`<= 0`), or exceeds
    signed 64-bit integer bounds.
  """
  if isinstance(raw_value, bool):
    return None
  if isinstance(raw_value, int):
    return raw_value if 0 < raw_value <= _MAX_INT64 else None
  if isinstance(raw_value, str):
    stripped = raw_value.strip()
    if stripped.isascii() and stripped.isdigit():
      significant = stripped.lstrip("0")
      if not significant or len(significant) > _MAX_INT64_DIGITS:
        return None
      parsed = int(significant)
      return parsed if parsed <= _MAX_INT64 else None
  return None


def _is_timestamp_within_window(
    raw_timestamp: Any,
    *,
    window: datetime.timedelta,
    default_when_missing: bool,
    now: datetime.datetime,
) -> bool:
  """Returns whether `raw_timestamp` is within `window` of `now` (UTC)."""
  if not isinstance(raw_timestamp, datetime.datetime):
    return default_when_missing
  timestamp = raw_timestamp
  if timestamp.tzinfo is None:
    timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
  reference_now = (
      now.replace(tzinfo=datetime.timezone.utc) if now.tzinfo is None else now
  )
  return (reference_now - timestamp) <= window


def _is_retry_backoff_active(
    job_data: Mapping[str, Any], *, now: datetime.datetime
) -> bool:
  """Returns whether a failed `5xx` job still has pending Cloud Task retries."""
  if job_data.get("status") not in _RETRYABLE_FAILURE_STATUSES:
    return False
  attempts = job_data.get("attempts")
  has_remaining_attempts = (
      isinstance(attempts, int)
      and not isinstance(attempts, bool)
      and 1 <= attempts < _CLOUD_TASKS_MAX_ATTEMPTS
  )
  if not has_remaining_attempts:
    return False
  return _is_timestamp_within_window(
      job_data.get("last_processed_at"),
      window=_RETRY_IN_FLIGHT_WINDOW,
      default_when_missing=False,
      now=now,
  )


def _is_job_in_flight(
    job_data: Mapping[str, Any], *, now: datetime.datetime
) -> bool:
  """Returns whether a Firestore job has an active or retrying worker task."""
  status = job_data.get("status")
  if status == JobStatus.QUEUED:
    return _is_timestamp_within_window(
        job_data.get("queued_at"),
        window=_MAX_IN_FLIGHT_JOB_AGE,
        default_when_missing=True,
        now=now,
    )
  if status == JobStatus.PROCESSING:
    return _is_timestamp_within_window(
        job_data.get("last_processed_at"),
        window=_MAX_IN_FLIGHT_JOB_AGE,
        default_when_missing=True,
        now=now,
    )
  return _is_retry_backoff_active(job_data, now=now)


def _is_deleting_claim_active(
    job_data: Mapping[str, Any], *, now: datetime.datetime
) -> bool:
  """Returns whether a job is currently claimed by an active `/delete` call."""
  if job_data.get("status") != JobStatus.DELETING:
    return False
  return _is_timestamp_within_window(
      job_data.get("deleting_at"),
      window=_DELETING_CLAIM_WINDOW,
      default_when_missing=True,
      now=now,
  )


def _is_job_active(
    job_data: Mapping[str, Any], *, now: datetime.datetime
) -> bool:
  """Returns whether a job is already processed or currently in flight."""
  return job_data.get("status") == JobStatus.PROCESSED or _is_job_in_flight(
      job_data, now=now
  )


def reconcile_existing_job(
    job_data: Mapping[str, Any],
    *,
    event_generation: int | None,
    now: datetime.datetime | None = None,
) -> ReconcileDecision:
  """Determines how `/ingest` should handle an existing Firestore job record.

  Args:
    job_data: Stored fields from the existing `(gcs_uri, prompt_version_id)`
      Firestore document.
    event_generation: Positive integer GCS generation from the incoming event,
      or `None` when omitted.
    now: Optional UTC reference timestamp for staleness checks; defaults to
      current UTC wall time.

  Returns:
    A `ReconcileDecision` specifying whether to enqueue, skip, or defer and
    whether superseded BigQuery rows must be removed before task dispatch.

  Raises:
    ValueError: If `event_generation` is not `None` and is a `bool`, `<= 0`, or
      exceeds signed 64-bit integer bounds.
  """
  reference_now = now or datetime.datetime.now(datetime.timezone.utc)
  if event_generation is None:
    if _is_deleting_claim_active(job_data, now=reference_now):
      return ReconcileDecision(outcome=EnqueueOutcome.DEFERRED_IN_FLIGHT)
    if _is_job_active(job_data, now=reference_now):
      return ReconcileDecision(outcome=EnqueueOutcome.SKIPPED)
    return ReconcileDecision(outcome=EnqueueOutcome.QUEUED)

  if isinstance(event_generation, bool) or not (
      0 < event_generation <= _MAX_INT64
  ):
    raise ValueError(
        "event_generation must be a positive 64-bit int or None; got"
        f" {event_generation!r}."
    )

  stored_generation = parse_generation(job_data.get("generation"))
  if stored_generation is not None and event_generation < stored_generation:
    return ReconcileDecision(outcome=EnqueueOutcome.SKIPPED)

  if stored_generation is not None and event_generation == stored_generation:
    if _is_job_active(job_data, now=reference_now) or _is_deleting_claim_active(
        job_data, now=reference_now
    ):
      return ReconcileDecision(outcome=EnqueueOutcome.SKIPPED)
    return ReconcileDecision(outcome=EnqueueOutcome.QUEUED)

  if _is_job_in_flight(
      job_data, now=reference_now
  ) or _is_deleting_claim_active(job_data, now=reference_now):
    return ReconcileDecision(outcome=EnqueueOutcome.DEFERRED_IN_FLIGHT)

  return ReconcileDecision(
      outcome=EnqueueOutcome.QUEUED,
      delete_superseded_rows=True,
  )


def classify_delete_snapshots(
    *,
    doc_snapshots: Iterable[firestore.DocumentSnapshot],
    event_generation: int | None,
    now: datetime.datetime | None = None,
) -> DeletePlan:
  """Classifies Firestore job snapshots for a GCS object delete event.

  Args:
    doc_snapshots: Existing Firestore job documents for the target `gcs_uri`.
    event_generation: GCS generation from the delete event, or `None` if unset.
    now: Optional UTC reference timestamp for staleness checks; defaults to
      current UTC wall time.

  Returns:
    A `DeletePlan` describing whether deletion must be skipped as a retired
    generation, deferred for an in-flight job, or applied to `target_docs`.
  """
  reference_now = now or datetime.datetime.now(datetime.timezone.utc)
  has_in_flight_job = False
  target_docs: list[firestore.DocumentSnapshot] = []
  for doc_snapshot in doc_snapshots:
    if not doc_snapshot.exists:
      continue
    job_data = doc_snapshot.to_dict() or {}
    stored_generation = parse_generation(job_data.get("generation"))
    is_newer_stored_generation = (
        event_generation is not None
        and stored_generation is not None
        and stored_generation > event_generation
    )
    if is_newer_stored_generation:
      return DeletePlan(action=DeleteAction.SKIP_STALE_GENERATION)
    if _is_job_in_flight(
        job_data, now=reference_now
    ) or _is_deleting_claim_active(job_data, now=reference_now):
      has_in_flight_job = True
      continue
    target_docs.append(doc_snapshot)

  if has_in_flight_job:
    return DeletePlan(action=DeleteAction.DEFER_IN_FLIGHT)
  return DeletePlan(
      action=DeleteAction.DELETE,
      target_docs=tuple(target_docs),
  )


def _update_doc(
    *,
    firestore_client: firestore.Client,
    doc_ref: firestore.DocumentReference,
    field_updates: Mapping[str, Any],
    last_update_time: Any,
) -> Any:
  """Updates `doc_ref`, attaching a `last_update_time` guard when available."""
  updates = dict(field_updates)
  if last_update_time is not None:
    option = firestore_client.write_option(last_update_time=last_update_time)
    return doc_ref.update(updates, option=option)
  return doc_ref.update(updates)


def _delete_doc(
    *,
    firestore_client: firestore.Client,
    doc_ref: firestore.DocumentReference,
    last_update_time: Any,
) -> None:
  """Deletes `doc_ref`, attaching a `last_update_time` guard when available."""
  if last_update_time is not None:
    option = firestore_client.write_option(last_update_time=last_update_time)
    doc_ref.delete(option=option)
    return
  doc_ref.delete()


def _batch_update_doc(
    *,
    firestore_client: firestore.Client,
    batch: firestore.WriteBatch,
    doc_ref: firestore.DocumentReference,
    field_updates: Mapping[str, Any],
    last_update_time: Any,
) -> None:
  """Stages a batch update on `doc_ref` with an optional write precondition."""
  updates = dict(field_updates)
  if last_update_time is not None:
    option = firestore_client.write_option(last_update_time=last_update_time)
    batch.update(doc_ref, updates, option=option)
    return
  batch.update(doc_ref, updates)


def _batch_delete_doc(
    *,
    firestore_client: firestore.Client,
    batch: firestore.WriteBatch,
    doc_ref: firestore.DocumentReference,
    last_update_time: Any,
) -> None:
  """Stages a batch delete on `doc_ref` with an optional write precondition."""
  if last_update_time is not None:
    option = firestore_client.write_option(last_update_time=last_update_time)
    batch.delete(doc_ref, option=option)
    return
  batch.delete(doc_ref)


def claim_queued_job(
    *,
    firestore_client: firestore.Client,
    doc_ref: firestore.DocumentReference,
    doc_snapshot: firestore.DocumentSnapshot,
    job_document: Mapping[str, Any],
) -> ClaimedQueuedJob:
  """Marks a Firestore job document `QUEUED` using write preconditions.

  Creates `doc_ref` if missing, or updates it guarded by `doc_snapshot`'s
  `update_time` precondition while clearing stale retry fields so concurrent
  `/ingest` events observe an active `QUEUED` lease before BigQuery cleanup and
  Cloud Tasks dispatch.

  Args:
    firestore_client: Firestore client used to construct write preconditions.
    doc_ref: Target Firestore job `DocumentReference`.
    doc_snapshot: Previously read `DocumentSnapshot` of `doc_ref`.
    job_document: New `QUEUED` job fields to write.

  Returns:
    A `ClaimedQueuedJob` lease record capturing prior fields and the new server
    `update_time` precondition token for `rollback_claimed_job`.

  Raises:
    auth_exceptions.GoogleAuthError: If Firestore authentication fails.
    google_exceptions.GoogleAPIError: If the precondition check or write fails.
  """
  existed_before_claim = doc_snapshot.exists
  previous_data = (doc_snapshot.to_dict() or {}) if existed_before_claim else {}
  claimed_keys = tuple(job_document.keys())
  if not existed_before_claim:
    write_result = doc_ref.create(dict(job_document))
    return ClaimedQueuedJob(
        reference=doc_ref,
        existed_before_claim=False,
        previous_data={},
        claimed_keys=claimed_keys,
        claim_update_time=write_result.update_time,
    )

  field_updates: dict[str, Any] = dict(job_document)
  for stale_field in _STALE_RETRY_JOB_FIELDS:
    field_updates.setdefault(stale_field, firestore.DELETE_FIELD)
  field_updates.setdefault("generation", firestore.DELETE_FIELD)
  write_result = _update_doc(
      firestore_client=firestore_client,
      doc_ref=doc_ref,
      field_updates=field_updates,
      last_update_time=doc_snapshot.update_time,
  )
  return ClaimedQueuedJob(
      reference=doc_ref,
      existed_before_claim=True,
      previous_data=previous_data,
      claimed_keys=claimed_keys,
      claim_update_time=write_result.update_time,
  )


def rollback_claimed_job(
    *,
    firestore_client: firestore.Client,
    claimed_job: ClaimedQueuedJob,
    bq_rows_deleted: bool,
) -> None:
  """Restores `claimed_job` to a retryable state if post-claim dispatch fails.

  Logs and suppresses Firestore RPC or authentication errors encountered during
  rollback so the caller's original dispatch exception can propagate.

  Args:
    firestore_client: Firestore client used to construct write preconditions.
    claimed_job: Claimed job record returned by `claim_queued_job`.
    bq_rows_deleted: Whether superseded BigQuery rows were already deleted.
  """
  doc_ref = claimed_job.reference
  try:
    if not claimed_job.existed_before_claim:
      _delete_doc(
          firestore_client=firestore_client,
          doc_ref=doc_ref,
          last_update_time=claimed_job.claim_update_time,
      )
      return
    if bq_rows_deleted:
      _update_doc(
          firestore_client=firestore_client,
          doc_ref=doc_ref,
          field_updates={
              "status": JobStatus.FAILED_RETRY,
              "attempts": _CLOUD_TASKS_MAX_ATTEMPTS,
          },
          last_update_time=claimed_job.claim_update_time,
      )
      return
    restore_updates: dict[str, Any] = dict(claimed_job.previous_data)
    for key in claimed_job.claimed_keys:
      restore_updates.setdefault(key, firestore.DELETE_FIELD)
    _update_doc(
        firestore_client=firestore_client,
        doc_ref=doc_ref,
        field_updates=restore_updates,
        last_update_time=claimed_job.claim_update_time,
    )
  except (
      auth_exceptions.GoogleAuthError,
      google_exceptions.GoogleAPIError,
  ):
    _LOGGER.exception(
        "Failed to roll back claimed Firestore state for %s.", doc_ref.id
    )


def rollback_deleting_docs(
    *,
    firestore_client: firestore.Client,
    claimed_docs: Sequence[ClaimedDeleteDoc],
    bq_rows_deleted: bool,
) -> None:
  """Restores leased `DELETING` Firestore job documents when deletion fails.

  Isolates each 500-document batch commit so a precondition mismatch or RPC
  failure in one chunk does not skip rolling back subsequent chunks, and logs
  errors without masking the caller's original exception.

  Args:
    firestore_client: Firestore client used to stage rollback batches.
    claimed_docs: Lease records for Firestore job documents marked `DELETING`.
    bq_rows_deleted: Whether BigQuery rows were already deleted before failure.
  """
  for start_index in range(0, len(claimed_docs), _FIRESTORE_BATCH_LIMIT):
    chunk = claimed_docs[start_index : start_index + _FIRESTORE_BATCH_LIMIT]
    try:
      batch = firestore_client.batch()
      for claimed_doc in chunk:
        if bq_rows_deleted:
          restore_updates: dict[str, Any] = {
              "status": JobStatus.FAILED_RETRY,
              "attempts": _CLOUD_TASKS_MAX_ATTEMPTS,
              "deleting_at": firestore.DELETE_FIELD,
          }
        else:
          restore_updates = {
              "status": claimed_doc.previous_data.get(
                  "status", firestore.DELETE_FIELD
              ),
              "deleting_at": claimed_doc.previous_data.get(
                  "deleting_at", firestore.DELETE_FIELD
              ),
          }
        _batch_update_doc(
            firestore_client=firestore_client,
            batch=batch,
            doc_ref=claimed_doc.reference,
            field_updates=restore_updates,
            last_update_time=claimed_doc.claim_update_time,
        )
      batch.commit()
    except (
        auth_exceptions.GoogleAuthError,
        google_exceptions.GoogleAPIError,
    ):
      _LOGGER.exception(
          "Failed to roll back %d DELETING Firestore job claim(s).",
          len(chunk),
      )


def claim_deleting_docs(
    *,
    firestore_client: firestore.Client,
    target_docs: Sequence[firestore.DocumentSnapshot],
    now: datetime.datetime | None = None,
) -> tuple[ClaimedDeleteDoc, ...]:
  """Marks Firestore job documents `DELETING` to reserve a deletion lease.

  Updates each Firestore job document in `target_docs` with
  `status="DELETING"` and `deleting_at` guarded by its snapshot `update_time`
  precondition, so concurrent `/ingest` or `/delete` events defer while
  BigQuery rows are purged. Rolls back already-committed chunks if a later
  500-document batch chunk fails.

  Args:
    firestore_client: Firestore client used to commit batched updates.
    target_docs: Settled Firestore `DocumentSnapshot` job records to mark
      `DELETING`.
    now: Optional UTC timestamp recorded as `deleting_at`; defaults to current
      UTC wall time.

  Returns:
    `ClaimedDeleteDoc` lease records capturing each Firestore job document's
    prior fields and new `update_time` precondition token for commit or
    rollback.

  Raises:
    auth_exceptions.GoogleAuthError: If Firestore authentication fails.
    google_exceptions.GoogleAPIError: If any batch precondition or commit fails.
  """
  claimed_docs: list[ClaimedDeleteDoc] = []
  deleting_at = now or datetime.datetime.now(datetime.timezone.utc)
  try:
    for start_index in range(0, len(target_docs), _FIRESTORE_BATCH_LIMIT):
      chunk = target_docs[start_index : start_index + _FIRESTORE_BATCH_LIMIT]
      batch = firestore_client.batch()
      for doc_snapshot in chunk:
        doc_ref = cast(firestore.DocumentReference, doc_snapshot.reference)
        _batch_update_doc(
            firestore_client=firestore_client,
            batch=batch,
            doc_ref=doc_ref,
            field_updates={
                "status": JobStatus.DELETING,
                "deleting_at": deleting_at,
            },
            last_update_time=doc_snapshot.update_time,
        )
      write_results = batch.commit()
      for index, doc_snapshot in enumerate(chunk):
        claim_update_time = None
        if index < len(write_results):
          claim_update_time = write_results[index].update_time
        claimed_docs.append(
            ClaimedDeleteDoc(
                reference=cast(
                    firestore.DocumentReference, doc_snapshot.reference
                ),
                previous_data=doc_snapshot.to_dict() or {},
                claim_update_time=claim_update_time,
            )
        )
  except Exception:
    if claimed_docs:
      rollback_deleting_docs(
          firestore_client=firestore_client,
          claimed_docs=claimed_docs,
          bq_rows_deleted=False,
      )
    raise
  return tuple(claimed_docs)


def commit_deleted_docs(
    *,
    firestore_client: firestore.Client,
    claimed_docs: Sequence[ClaimedDeleteDoc],
    bq_rows_deleted: bool = True,
) -> None:
  """Deletes leased `DELETING` Firestore job documents in batches.

  Rolls back any undeleted tail chunks if a batch commit fails.

  Args:
    firestore_client: Firestore client used to commit batched deletes.
    claimed_docs: Lease records for Firestore job documents to delete.
    bq_rows_deleted: Whether BigQuery rows were already purged prior to commit,
      controlling rollback state for any uncommitted tail chunks.

  Raises:
    auth_exceptions.GoogleAuthError: If Firestore authentication fails.
    google_exceptions.GoogleAPIError: If any batch precondition or commit fails.
  """
  for start_index in range(0, len(claimed_docs), _FIRESTORE_BATCH_LIMIT):
    try:
      batch = firestore_client.batch()
      for claimed_doc in claimed_docs[
          start_index : start_index + _FIRESTORE_BATCH_LIMIT
      ]:
        _batch_delete_doc(
            firestore_client=firestore_client,
            batch=batch,
            doc_ref=claimed_doc.reference,
            last_update_time=claimed_doc.claim_update_time,
        )
      batch.commit()
    except Exception:
      rollback_deleting_docs(
          firestore_client=firestore_client,
          claimed_docs=claimed_docs[start_index:],
          bq_rows_deleted=bq_rows_deleted,
      )
      raise
