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

from collections.abc import Mapping
import dataclasses
import datetime
import enum
import hashlib
import logging
from typing import Any

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


class EnqueueOutcome(enum.StrEnum):
  """Result of reconciling and enqueueing a single prompt version job."""

  QUEUED = "queued"
  SKIPPED = "skipped"
  DEFERRED_IN_FLIGHT = "deferred_in_flight"


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


_CLOUD_TASKS_MAX_ATTEMPTS = 5
_MAX_INT64 = (1 << 63) - 1
_MAX_INT64_DIGITS = len(str(_MAX_INT64))
_RETRY_IN_FLIGHT_WINDOW = datetime.timedelta(minutes=10)
_MAX_IN_FLIGHT_JOB_AGE = datetime.timedelta(hours=2)
_RETRYABLE_FAILURE_STATUSES = (
    JobStatus.FAILED_RETRY,
    JobStatus.FAILED_PERMANENT,
)
_STALE_RETRY_JOB_FIELDS = ("attempts", "error_message", "last_processed_at")


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
    if _is_job_active(job_data, now=reference_now):
      return ReconcileDecision(outcome=EnqueueOutcome.SKIPPED)
    return ReconcileDecision(outcome=EnqueueOutcome.QUEUED)

  if _is_job_in_flight(job_data, now=reference_now):
    return ReconcileDecision(outcome=EnqueueOutcome.DEFERRED_IN_FLIGHT)

  return ReconcileDecision(
      outcome=EnqueueOutcome.QUEUED,
      delete_superseded_rows=True,
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
