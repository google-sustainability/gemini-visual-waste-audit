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

from collections.abc import Mapping
import datetime
from typing import Any
from unittest import mock

from google.api_core import exceptions as google_exceptions
from google.auth import exceptions as auth_exceptions
from google.cloud import firestore

from ingestion_service import job_store
from absl.testing import absltest
from absl.testing import parameterized

_GCS_URI = "gs://audit-bucket/sample.jpg"
_NOW = datetime.datetime(2026, 11, 5, 14, 0, tzinfo=datetime.timezone.utc)
_NAIVE_RECENT_UTC = datetime.datetime(2026, 11, 5, 13, 59)
_SNAPSHOT_UPDATE_TIME = datetime.datetime(
    2026, 11, 5, 12, 0, tzinfo=datetime.timezone.utc
)
_CLAIM_UPDATE_TIME = datetime.datetime(
    2026, 11, 5, 12, 1, tzinfo=datetime.timezone.utc
)


def _make_snapshot_mock(
    doc_ref: firestore.DocumentReference,
    *,
    exists: bool = False,
    data: Mapping[str, Any] | None = None,
    update_time: datetime.datetime | None = _SNAPSHOT_UPDATE_TIME,
) -> mock.MagicMock:
  snapshot_spec = firestore.DocumentSnapshot(
      reference=doc_ref,
      data={},
      exists=False,
      read_time=None,
      create_time=None,
      update_time=_SNAPSHOT_UPDATE_TIME,
  )
  snapshot_mock = mock.create_autospec(snapshot_spec, spec_set=True)
  snapshot_mock.reference = doc_ref
  snapshot_mock.exists = exists
  snapshot_mock.update_time = update_time
  snapshot_mock.to_dict.return_value = dict(data) if data is not None else None
  return snapshot_mock


class JobStoreTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_firestore = mock.create_autospec(
        firestore.Client, instance=True, spec_set=True
    )
    self.mock_doc_ref = mock.create_autospec(
        firestore.DocumentReference, instance=True, spec_set=True
    )
    self.mock_doc_ref.id = "doc-123"
    self.write_result = firestore.types.WriteResult(
        update_time=_CLAIM_UPDATE_TIME
    )
    self.mock_firestore.write_option.side_effect = firestore.Client.write_option
    self.mock_doc_ref.create.return_value = self.write_result
    self.mock_doc_ref.update.return_value = self.write_result

  def test_make_job_doc_id_hashes_gcs_uri_and_prompt_version(self):
    actual = job_store.make_job_doc_id(_GCS_URI, "v1")

    self.assertEqual(
        actual,
        "e7cf1fc015d8eca3dd4ac9fbc70503221c644acd3b866a80ed24e19cb5822940",
    )
    self.assertNotEqual(actual, job_store.make_job_doc_id(_GCS_URI, "v2"))

  @parameterized.named_parameters(
      dict(testcase_name="positive_int", raw_value=100, expected=100),
      dict(testcase_name="min_positive_int", raw_value=1, expected=1),
      dict(
          testcase_name="max_int64",
          raw_value=(1 << 63) - 1,
          expected=(1 << 63) - 1,
      ),
      dict(testcase_name="int64_overflow", raw_value=1 << 63, expected=None),
      dict(
          testcase_name="int64_overflow_str",
          raw_value=str(1 << 63),
          expected=None,
      ),
      dict(testcase_name="ascii_digit_str", raw_value="  250 ", expected=250),
      dict(
          testcase_name="leading_zeros_str",
          raw_value="000250",
          expected=250,
      ),
      dict(
          testcase_name="oversized_digit_str",
          raw_value="9" * 5000,
          expected=None,
      ),
      dict(
          testcase_name="oversized_zero_str",
          raw_value="0" * 5000,
          expected=None,
      ),
      dict(testcase_name="none_value", raw_value=None, expected=None),
      dict(testcase_name="bool_true", raw_value=True, expected=None),
      dict(testcase_name="bool_false", raw_value=False, expected=None),
      dict(testcase_name="zero_int", raw_value=0, expected=None),
      dict(testcase_name="zero_str", raw_value="0", expected=None),
      dict(testcase_name="negative_int", raw_value=-5, expected=None),
      dict(testcase_name="negative_str", raw_value="-5", expected=None),
      dict(testcase_name="blank_str", raw_value="   ", expected=None),
      dict(testcase_name="non_numeric_str", raw_value="abc", expected=None),
      dict(
          testcase_name="non_ascii_digits",
          raw_value="\u0661\u0660\u0660",
          expected=None,
      ),
  )
  def test_parse_generation_validates_positive_ascii_integers(
      self, raw_value: Any, expected: int | None
  ):
    self.assertEqual(job_store.parse_generation(raw_value), expected)

  @parameterized.named_parameters(
      dict(
          testcase_name="omitted_gen_processed",
          job_data={"status": "PROCESSED", "generation": 100},
          event_generation=None,
      ),
      dict(
          testcase_name="omitted_gen_queued_without_timestamp",
          job_data={"status": "QUEUED"},
          event_generation=None,
      ),
      dict(
          testcase_name="older_event_generation",
          job_data={"status": "FAILED_RETRY", "generation": 100, "attempts": 5},
          event_generation=99,
      ),
      dict(
          testcase_name="duplicate_gen_processed",
          job_data={"status": "PROCESSED", "generation": 100},
          event_generation=100,
      ),
      dict(
          testcase_name="duplicate_gen_processing",
          job_data={"status": "PROCESSING", "generation": 100},
          event_generation=100,
      ),
      dict(
          testcase_name="duplicate_gen_failed_retry_backoff",
          job_data={
              "status": "FAILED_RETRY",
              "generation": 100,
              "attempts": 2,
              "last_processed_at": _NOW - datetime.timedelta(minutes=5),
          },
          event_generation=100,
      ),
      dict(
          testcase_name="duplicate_gen_failed_permanent_backoff",
          job_data={
              "status": "FAILED_PERMANENT",
              "generation": 100,
              "attempts": 2,
              "last_processed_at": _NOW - datetime.timedelta(minutes=5),
          },
          event_generation=100,
      ),
  )
  def test_reconcile_existing_job_skips_active_or_newer_generations(
      self,
      job_data: Mapping[str, Any],
      event_generation: int | None,
  ):
    decision = job_store.reconcile_existing_job(
        job_data, event_generation=event_generation, now=_NOW
    )

    self.assertEqual(
        decision,
        job_store.ReconcileDecision(
            outcome=job_store.EnqueueOutcome.SKIPPED,
            delete_superseded_rows=False,
        ),
    )

  @parameterized.named_parameters(
      dict(
          testcase_name="queued_at_exact_2h_boundary",
          job_data={
              "status": "QUEUED",
              "generation": 100,
              "queued_at": _NOW - datetime.timedelta(hours=2),
          },
      ),
      dict(
          testcase_name="processing_at_exact_2h_boundary",
          job_data={
              "status": "PROCESSING",
              "generation": 100,
              "last_processed_at": _NOW - datetime.timedelta(hours=2),
          },
      ),
      dict(
          testcase_name="failed_retry_at_exact_10m_boundary",
          job_data={
              "status": "FAILED_RETRY",
              "generation": 100,
              "attempts": 4,
              "last_processed_at": _NOW - datetime.timedelta(minutes=10),
          },
      ),
      dict(
          testcase_name="failed_permanent_backoff_naive_utc",
          job_data={
              "status": "FAILED_PERMANENT",
              "generation": 100,
              "attempts": 1,
              "last_processed_at": _NAIVE_RECENT_UTC,
          },
      ),
  )
  def test_reconcile_existing_job_defers_newer_generation_while_in_flight(
      self,
      job_data: Mapping[str, Any],
  ):
    decision = job_store.reconcile_existing_job(
        job_data, event_generation=200, now=_NOW
    )

    self.assertEqual(
        decision,
        job_store.ReconcileDecision(
            outcome=job_store.EnqueueOutcome.DEFERRED_IN_FLIGHT,
            delete_superseded_rows=False,
        ),
    )

  def test_reconcile_existing_job_normalizes_naive_now_to_utc(self):
    naive_now = datetime.datetime(2026, 11, 5, 14, 0)
    job_data = {
        "status": "PROCESSING",
        "generation": 100,
        "last_processed_at": _NOW - datetime.timedelta(hours=1),
    }

    decision = job_store.reconcile_existing_job(
        job_data, event_generation=200, now=naive_now
    )

    self.assertEqual(
        decision,
        job_store.ReconcileDecision(
            outcome=job_store.EnqueueOutcome.DEFERRED_IN_FLIGHT,
            delete_superseded_rows=False,
        ),
    )

  @parameterized.named_parameters(
      dict(testcase_name="zero", invalid_generation=0),
      dict(testcase_name="negative", invalid_generation=-1),
      dict(testcase_name="bool_true", invalid_generation=True),
      dict(testcase_name="int64_overflow", invalid_generation=1 << 63),
  )
  def test_reconcile_existing_job_rejects_invalid_event_generation(
      self, invalid_generation: Any
  ):
    with self.assertRaisesRegex(
        ValueError, "event_generation must be a positive 64-bit int"
    ):
      job_store.reconcile_existing_job(
          {"status": "PROCESSED"},
          event_generation=invalid_generation,
          now=_NOW,
      )

  @parameterized.named_parameters(
      dict(
          testcase_name="omitted_generation_exhausted_retry",
          job_data={"status": "FAILED_RETRY", "attempts": 5},
          event_generation=None,
      ),
      dict(
          testcase_name="same_generation_exhausted_retry",
          job_data={"status": "FAILED_RETRY", "generation": 100, "attempts": 5},
          event_generation=100,
      ),
      dict(
          testcase_name="same_generation_bool_attempts_not_treated_as_int",
          job_data={
              "status": "FAILED_RETRY",
              "generation": 100,
              "attempts": True,
          },
          event_generation=100,
      ),
      dict(
          testcase_name="same_generation_missing_retry_timestamp",
          job_data={"status": "FAILED_RETRY", "generation": 100, "attempts": 2},
          event_generation=100,
      ),
  )
  def test_reconcile_existing_job_requeues_without_bq_delete(
      self,
      job_data: Mapping[str, Any],
      event_generation: int | None,
  ):
    decision = job_store.reconcile_existing_job(
        job_data, event_generation=event_generation, now=_NOW
    )

    self.assertEqual(
        decision,
        job_store.ReconcileDecision(
            outcome=job_store.EnqueueOutcome.QUEUED,
            delete_superseded_rows=False,
        ),
    )

  @parameterized.named_parameters(
      dict(
          testcase_name="settled_processed",
          job_data={"status": "PROCESSED", "generation": 100},
      ),
      dict(
          testcase_name="legacy_missing_generation",
          job_data={"status": "PROCESSED"},
      ),
      dict(
          testcase_name="malformed_stored_generation",
          job_data={"status": "PROCESSED", "generation": "invalid"},
      ),
      dict(
          testcase_name="exhausted_failed_retry",
          job_data={"status": "FAILED_RETRY", "generation": 100, "attempts": 5},
      ),
      dict(
          testcase_name="exhausted_failed_permanent",
          job_data={
              "status": "FAILED_PERMANENT",
              "generation": 100,
              "attempts": 5,
          },
      ),
      dict(
          testcase_name="zero_attempts_failed_retry",
          job_data={"status": "FAILED_RETRY", "generation": 100, "attempts": 0},
      ),
      dict(
          testcase_name="stale_queued_over_2h",
          job_data={
              "status": "QUEUED",
              "generation": 100,
              "queued_at": _NOW - datetime.timedelta(hours=2, microseconds=1),
          },
      ),
      dict(
          testcase_name="stale_processing_over_2h",
          job_data={
              "status": "PROCESSING",
              "generation": 100,
              "last_processed_at": (
                  _NOW - datetime.timedelta(hours=2, microseconds=1)
              ),
          },
      ),
      dict(
          testcase_name="expired_retry_backoff_over_10m",
          job_data={
              "status": "FAILED_RETRY",
              "generation": 100,
              "attempts": 2,
              "last_processed_at": (
                  _NOW - datetime.timedelta(minutes=10, microseconds=1)
              ),
          },
      ),
  )
  def test_reconcile_existing_job_queues_overwrite_with_bq_delete(
      self, job_data: Mapping[str, Any]
  ):
    decision = job_store.reconcile_existing_job(
        job_data, event_generation=200, now=_NOW
    )

    self.assertEqual(
        decision,
        job_store.ReconcileDecision(
            outcome=job_store.EnqueueOutcome.QUEUED,
            delete_superseded_rows=True,
        ),
    )

  def test_claim_queued_job_creates_document_when_snapshot_missing(self):
    snapshot = _make_snapshot_mock(self.mock_doc_ref, exists=False)
    job_document = {"gcs_uri": _GCS_URI, "status": job_store.JobStatus.QUEUED}

    claimed = job_store.claim_queued_job(
        firestore_client=self.mock_firestore,
        doc_ref=self.mock_doc_ref,
        doc_snapshot=snapshot,
        job_document=job_document,
    )

    self.assertEqual(
        claimed,
        job_store.ClaimedQueuedJob(
            reference=self.mock_doc_ref,
            existed_before_claim=False,
            previous_data={},
            claimed_keys=("gcs_uri", "status"),
            claim_update_time=_CLAIM_UPDATE_TIME,
        ),
    )
    self.mock_doc_ref.create.assert_called_once_with(job_document)
    self.mock_doc_ref.update.assert_not_called()

  def test_claim_queued_job_updates_existing_doc_with_precondition(self):
    snapshot = _make_snapshot_mock(
        self.mock_doc_ref,
        exists=True,
        data={"status": "PROCESSED", "generation": 100},
        update_time=_SNAPSHOT_UPDATE_TIME,
    )

    claimed = job_store.claim_queued_job(
        firestore_client=self.mock_firestore,
        doc_ref=self.mock_doc_ref,
        doc_snapshot=snapshot,
        job_document={
            "gcs_uri": _GCS_URI,
            "status": job_store.JobStatus.QUEUED,
            "generation": 200,
        },
    )

    self.assertEqual(
        claimed,
        job_store.ClaimedQueuedJob(
            reference=self.mock_doc_ref,
            existed_before_claim=True,
            previous_data={"status": "PROCESSED", "generation": 100},
            claimed_keys=("gcs_uri", "status", "generation"),
            claim_update_time=_CLAIM_UPDATE_TIME,
        ),
    )
    self.mock_doc_ref.update.assert_called_once_with(
        {
            "gcs_uri": _GCS_URI,
            "status": job_store.JobStatus.QUEUED,
            "generation": 200,
            "attempts": firestore.DELETE_FIELD,
            "error_message": firestore.DELETE_FIELD,
            "last_processed_at": firestore.DELETE_FIELD,
        },
        option=firestore.Client.write_option(
            last_update_time=_SNAPSHOT_UPDATE_TIME
        ),
    )

  def test_claim_queued_job_clears_generation_and_omits_missing_precondition(
      self,
  ):
    snapshot = _make_snapshot_mock(
        self.mock_doc_ref,
        exists=True,
        data={"status": "FAILED_RETRY", "generation": 100},
        update_time=None,
    )

    claimed = job_store.claim_queued_job(
        firestore_client=self.mock_firestore,
        doc_ref=self.mock_doc_ref,
        doc_snapshot=snapshot,
        job_document={
            "gcs_uri": _GCS_URI,
            "status": job_store.JobStatus.QUEUED,
        },
    )

    self.assertEqual(claimed.claim_update_time, _CLAIM_UPDATE_TIME)
    self.mock_doc_ref.update.assert_called_once_with({
        "gcs_uri": _GCS_URI,
        "status": job_store.JobStatus.QUEUED,
        "generation": firestore.DELETE_FIELD,
        "attempts": firestore.DELETE_FIELD,
        "error_message": firestore.DELETE_FIELD,
        "last_processed_at": firestore.DELETE_FIELD,
    })

  def test_rollback_claimed_job_deletes_newly_created_document(self):
    claimed_job = job_store.ClaimedQueuedJob(
        reference=self.mock_doc_ref,
        existed_before_claim=False,
        previous_data={},
        claimed_keys=("gcs_uri", "status"),
        claim_update_time=_CLAIM_UPDATE_TIME,
    )

    job_store.rollback_claimed_job(
        firestore_client=self.mock_firestore,
        claimed_job=claimed_job,
        bq_rows_deleted=False,
    )

    self.mock_doc_ref.delete.assert_called_once_with(
        option=firestore.Client.write_option(
            last_update_time=_CLAIM_UPDATE_TIME
        )
    )
    self.mock_doc_ref.update.assert_not_called()

  def test_rollback_claimed_job_deletes_without_option_when_update_time_missing(
      self,
  ):
    claimed_job = job_store.ClaimedQueuedJob(
        reference=self.mock_doc_ref,
        existed_before_claim=False,
        previous_data={},
        claimed_keys=("gcs_uri", "status"),
        claim_update_time=None,
    )

    job_store.rollback_claimed_job(
        firestore_client=self.mock_firestore,
        claimed_job=claimed_job,
        bq_rows_deleted=False,
    )

    self.mock_doc_ref.delete.assert_called_once_with()

  def test_rollback_claimed_job_marks_failed_retry_after_bq_rows_deleted(self):
    claimed_job = job_store.ClaimedQueuedJob(
        reference=self.mock_doc_ref,
        existed_before_claim=True,
        previous_data={"status": "PROCESSED", "generation": 100},
        claimed_keys=("status", "generation"),
        claim_update_time=_CLAIM_UPDATE_TIME,
    )

    job_store.rollback_claimed_job(
        firestore_client=self.mock_firestore,
        claimed_job=claimed_job,
        bq_rows_deleted=True,
    )

    self.mock_doc_ref.update.assert_called_once_with(
        {"status": job_store.JobStatus.FAILED_RETRY, "attempts": 5},
        option=firestore.Client.write_option(
            last_update_time=_CLAIM_UPDATE_TIME
        ),
    )

  def test_rollback_claimed_job_restores_previous_fields_before_bq_delete(self):
    claimed_job = job_store.ClaimedQueuedJob(
        reference=self.mock_doc_ref,
        existed_before_claim=True,
        previous_data={"status": "PROCESSED"},
        claimed_keys=("status", "generation", "queued_at"),
        claim_update_time=None,
    )

    job_store.rollback_claimed_job(
        firestore_client=self.mock_firestore,
        claimed_job=claimed_job,
        bq_rows_deleted=False,
    )

    self.mock_doc_ref.update.assert_called_once_with({
        "status": "PROCESSED",
        "generation": firestore.DELETE_FIELD,
        "queued_at": firestore.DELETE_FIELD,
    })

  @parameterized.named_parameters(
      dict(
          testcase_name="api_error",
          error=google_exceptions.ServiceUnavailable("Firestore unavailable"),
      ),
      dict(
          testcase_name="retry_error",
          error=google_exceptions.RetryError(
              "Deadline exceeded", cause=Exception("503")
          ),
      ),
      dict(
          testcase_name="auth_error",
          error=auth_exceptions.RefreshError("Token refresh failed"),
      ),
  )
  def test_rollback_claimed_job_logs_and_suppresses_firestore_errors(
      self, error: Exception
  ):
    self.mock_doc_ref.delete.side_effect = error
    claimed_job = job_store.ClaimedQueuedJob(
        reference=self.mock_doc_ref,
        existed_before_claim=False,
        previous_data={},
        claimed_keys=("status",),
        claim_update_time=_CLAIM_UPDATE_TIME,
    )

    with self.assertLogs(job_store.__name__, level="ERROR"):
      job_store.rollback_claimed_job(
          firestore_client=self.mock_firestore,
          claimed_job=claimed_job,
          bq_rows_deleted=False,
      )


if __name__ == "__main__":
  absltest.main()
