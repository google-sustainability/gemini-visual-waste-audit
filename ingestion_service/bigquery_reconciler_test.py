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
import time
from unittest import mock

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

from ingestion_service import bigquery_reconciler
from absl.testing import absltest
from absl.testing import parameterized

_PROJECT_ID = "test-project"
_DATASET_ID = "waste_dataset"
_TABLE_NAME = "waste_table"
_QUALIFIED_TABLE_ID = "test-project.waste_dataset.waste_table"
_DOMAIN_QUALIFIED_TABLE_ID = "google.com:test-project.waste_dataset.waste_table"
_GCS_URI = "gs://audit-bucket/sample.jpg"
_SERIALIZATION_ERROR = google_exceptions.BadRequest(
    "Could not serialize access to table waste_table"
)


class BigQueryReconcilerTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_bq = mock.create_autospec(
        bigquery.Client, instance=True, spec_set=True
    )
    self.mock_query_job = mock.create_autospec(
        bigquery.QueryJob, instance=True, spec_set=True
    )
    self.mock_bq.query.return_value = self.mock_query_job
    self.mock_query_job.num_dml_affected_rows = 0
    self.mock_sleep = self.enter_context(
        mock.patch.object(time, "sleep", autospec=True)
    )

  def _assert_query_parameters(
      self, expected_params: Mapping[str, str]
  ) -> None:
    job_config = self.mock_bq.query.call_args.kwargs["job_config"]
    actual_params = {
        param.name: (param.type_, param.value)
        for param in job_config.query_parameters
    }
    expected_typed_params = {
        name: ("STRING", value) for name, value in expected_params.items()
    }
    self.assertEqual(actual_params, expected_typed_params)

  @parameterized.named_parameters(
      dict(
          testcase_name="standard_project",
          project_id="test-project",
          expected="test-project.waste_dataset.waste_table",
      ),
      dict(
          testcase_name="domain_scoped_project",
          project_id="google.com:test-project",
          expected="google.com:test-project.waste_dataset.waste_table",
      ),
  )
  def test_resolve_table_id_returns_qualified_id(
      self, project_id: str, expected: str
  ):
    actual = bigquery_reconciler.resolve_table_id(
        project_id=project_id,
        dataset_id=_DATASET_ID,
        table_id=_TABLE_NAME,
    )

    self.assertEqual(actual, expected)

  @parameterized.named_parameters(
      dict(
          testcase_name="missing_project",
          project_id=None,
          dataset_id=_DATASET_ID,
          table_id=_TABLE_NAME,
      ),
      dict(
          testcase_name="blank_project",
          project_id="   ",
          dataset_id=_DATASET_ID,
          table_id=_TABLE_NAME,
      ),
      dict(
          testcase_name="missing_dataset",
          project_id=_PROJECT_ID,
          dataset_id=None,
          table_id=_TABLE_NAME,
      ),
      dict(
          testcase_name="missing_table",
          project_id=_PROJECT_ID,
          dataset_id=_DATASET_ID,
          table_id="",
      ),
  )
  def test_resolve_table_id_returns_none_and_logs_warning_when_incomplete(
      self,
      project_id: str | None,
      dataset_id: str | None,
      table_id: str | None,
  ):
    with self.assertLogs(bigquery_reconciler.__name__, level="WARNING"):
      actual = bigquery_reconciler.resolve_table_id(
          project_id=project_id,
          dataset_id=dataset_id,
          table_id=table_id,
      )

    self.assertIsNone(actual)

  @parameterized.named_parameters(
      dict(
          testcase_name="backtick_in_project",
          project_id="bad`proj",
          dataset_id=_DATASET_ID,
          table_id=_TABLE_NAME,
          expected_error_regex="Invalid GCP project_id:",
      ),
      dict(
          testcase_name="dot_in_project_without_domain",
          project_id="bad.project",
          dataset_id=_DATASET_ID,
          table_id=_TABLE_NAME,
          expected_error_regex="Invalid GCP project_id:",
      ),
      dict(
          testcase_name="multiple_colons_in_project",
          project_id="google.com:sub:proj",
          dataset_id=_DATASET_ID,
          table_id=_TABLE_NAME,
          expected_error_regex="Invalid GCP project_id:",
      ),
      dict(
          testcase_name="dot_in_dataset",
          project_id=_PROJECT_ID,
          dataset_id="bad.dataset",
          table_id=_TABLE_NAME,
          expected_error_regex="Invalid BigQuery dataset_id:",
      ),
      dict(
          testcase_name="hyphen_in_dataset",
          project_id=_PROJECT_ID,
          dataset_id="bad-dataset",
          table_id=_TABLE_NAME,
          expected_error_regex="Invalid BigQuery dataset_id:",
      ),
      dict(
          testcase_name="semicolon_in_table",
          project_id=_PROJECT_ID,
          dataset_id=_DATASET_ID,
          table_id="waste_table; DROP TABLE x",
          expected_error_regex="Invalid BigQuery table_id:",
      ),
  )
  def test_resolve_table_id_rejects_invalid_identifiers(
      self,
      project_id: str,
      dataset_id: str,
      table_id: str,
      expected_error_regex: str,
  ):
    with self.assertRaisesRegex(ValueError, expected_error_regex):
      bigquery_reconciler.resolve_table_id(
          project_id=project_id,
          dataset_id=dataset_id,
          table_id=table_id,
      )

  def test_delete_rows_scopes_to_prompt_version_when_provided(self):
    self.mock_query_job.num_dml_affected_rows = 3

    deleted = bigquery_reconciler.delete_rows(
        self.mock_bq,
        table_id=_QUALIFIED_TABLE_ID,
        gcs_uri=_GCS_URI,
        prompt_version_id="v1",
    )

    self.assertEqual(deleted, 3)
    self.mock_bq.query.assert_called_once()
    self.mock_query_job.result.assert_called_once_with()
    delete_sql = self.mock_bq.query.call_args.args[0]
    self.assertEqual(
        delete_sql,
        f"DELETE FROM `{_QUALIFIED_TABLE_ID}`\n"
        "WHERE\n"
        "  gcs_uri = @gcs_uri\n"
        "  AND prompt_version_id = @prompt_version_id;",
    )
    self._assert_query_parameters({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
    })

  @parameterized.named_parameters(
      dict(testcase_name="standard_project", table_id=_QUALIFIED_TABLE_ID),
      dict(
          testcase_name="domain_scoped_project",
          table_id=_DOMAIN_QUALIFIED_TABLE_ID,
      ),
  )
  def test_delete_rows_deletes_all_versions_when_version_omitted(
      self, table_id: str
  ):
    self.mock_query_job.num_dml_affected_rows = 5

    deleted = bigquery_reconciler.delete_rows(
        self.mock_bq,
        table_id=table_id,
        gcs_uri=_GCS_URI,
    )

    self.assertEqual(deleted, 5)
    self.mock_query_job.result.assert_called_once_with()
    delete_sql = self.mock_bq.query.call_args.args[0]
    self.assertEqual(
        delete_sql,
        f"DELETE FROM `{table_id}`\nWHERE\n  gcs_uri = @gcs_uri;",
    )
    self._assert_query_parameters({"gcs_uri": _GCS_URI})

  def test_delete_rows_returns_zero_when_affected_rows_is_none(self):
    self.mock_query_job.num_dml_affected_rows = None

    deleted = bigquery_reconciler.delete_rows(
        self.mock_bq,
        table_id=_QUALIFIED_TABLE_ID,
        gcs_uri=_GCS_URI,
    )

    self.assertEqual(deleted, 0)

  def test_delete_rows_retries_transient_dml_serialization_conflict(self):
    failed_job_1 = mock.create_autospec(
        bigquery.QueryJob, instance=True, spec_set=True
    )
    failed_job_1.result.side_effect = google_exceptions.BadRequest(
        "Could not serialize access to table waste_table"
    )
    failed_job_2 = mock.create_autospec(
        bigquery.QueryJob, instance=True, spec_set=True
    )
    failed_job_2.result.side_effect = google_exceptions.BadRequest(
        "Concurrent update on table waste_table"
    )
    succeeded_job = mock.create_autospec(
        bigquery.QueryJob, instance=True, spec_set=True
    )
    succeeded_job.num_dml_affected_rows = 4
    self.mock_bq.query.side_effect = [failed_job_1, failed_job_2, succeeded_job]

    with self.assertLogs(bigquery_reconciler.__name__, level="WARNING") as logs:
      deleted = bigquery_reconciler.delete_rows(
          self.mock_bq,
          table_id=_QUALIFIED_TABLE_ID,
          gcs_uri=_GCS_URI,
      )

    self.assertEqual(deleted, 4)
    self.assertEqual(self.mock_bq.query.call_count, 3)
    self.assertLen(logs.output, 2)
    sleep_delays = [call.args[0] for call in self.mock_sleep.call_args_list]
    self.assertLen(sleep_delays, 2)
    self.assertGreaterEqual(sleep_delays[0], 0.0)
    self.assertLessEqual(sleep_delays[0], 0.5)
    self.assertGreaterEqual(sleep_delays[1], 0.0)
    self.assertLessEqual(sleep_delays[1], 1.0)

  def test_delete_rows_raises_non_retryable_bad_request_immediately(self):
    self.mock_query_job.result.side_effect = google_exceptions.BadRequest(
        "Unrecognized name: invalid_column"
    )

    with self.assertRaisesRegex(
        google_exceptions.BadRequest, "Unrecognized name"
    ):
      bigquery_reconciler.delete_rows(
          self.mock_bq,
          table_id=_QUALIFIED_TABLE_ID,
          gcs_uri=_GCS_URI,
      )

    self.mock_bq.query.assert_called_once()
    self.mock_sleep.assert_not_called()

  def test_delete_rows_raises_after_exhausting_dml_serialization_retries(self):
    self.mock_query_job.result.side_effect = _SERIALIZATION_ERROR

    with self.assertRaisesRegex(
        google_exceptions.BadRequest, "Could not serialize access"
    ):
      bigquery_reconciler.delete_rows(
          self.mock_bq,
          table_id=_QUALIFIED_TABLE_ID,
          gcs_uri=_GCS_URI,
      )

    self.assertEqual(self.mock_bq.query.call_count, 4)
    sleep_delays = [call.args[0] for call in self.mock_sleep.call_args_list]
    self.assertLen(sleep_delays, 3)
    for delay, upper_bound in zip(sleep_delays, (0.5, 1.0, 2.0), strict=True):
      self.assertGreaterEqual(delay, 0.0)
      self.assertLessEqual(delay, upper_bound)

  @parameterized.named_parameters(
      dict(testcase_name="missing_table_part", bad_table_id="proj.dataset"),
      dict(
          testcase_name="extra_table_part",
          bad_table_id="proj.dataset.tbl.extra",
      ),
      dict(
          testcase_name="hyphen_in_dataset",
          bad_table_id="proj.bad-dataset.tbl",
      ),
      dict(testcase_name="leading_dot_project", bad_table_id="..dataset.tbl"),
      dict(
          testcase_name="backtick_injection",
          bad_table_id="proj.dataset.tbl` WHERE TRUE --",
      ),
  )
  def test_delete_rows_rejects_malformed_table_id(self, bad_table_id: str):
    with self.assertRaisesRegex(ValueError, "Invalid BigQuery table_id:"):
      bigquery_reconciler.delete_rows(
          self.mock_bq,
          table_id=bad_table_id,
          gcs_uri=_GCS_URI,
      )
    self.mock_bq.query.assert_not_called()


if __name__ == "__main__":
  absltest.main()
