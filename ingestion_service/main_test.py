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

"""Behavioral tests for `main.py` loaded via `importlib` with Flask stubbed."""

from collections.abc import Mapping
import datetime
import http
import importlib.util
import json
import os
import sys
import types
from typing import Any
from unittest import mock

from google.api_core import exceptions as google_exceptions
from google.auth import exceptions as auth_exceptions
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import tasks_v2

from ingestion_service import bigquery_reconciler
from ingestion_service import job_store
from utils import prompts
from absl.testing import absltest
from absl.testing import parameterized

_PROJECT_ID = "test-project"
_REGION = "us-central1"
_QUEUE_ID = "audit-queue"
_DATASET_ID = "waste_dataset"
_TABLE_ID = "waste_table"
_QUALIFIED_TABLE_ID = "test-project.waste_dataset.waste_table"
_QUEUE_PATH = "projects/test-project/locations/us-central1/queues/audit-queue"
_PROCESSING_URL = "https://processing.example.com/process"
_HTTP_PROCESSING_URL = "http://localhost:8080/process"
_SERVICE_ACCOUNT = "ingest@test-project.iam.gserviceaccount.com"
_GCS_URI = "gs://audit-bucket/sample.jpg"
_SNAPSHOT_UPDATE_TIME = datetime.datetime(
    2026, 11, 5, 12, 0, tzinfo=datetime.timezone.utc
)
_CLAIM_UPDATE_TIME = datetime.datetime(
    2026, 11, 5, 12, 1, tzinfo=datetime.timezone.utc
)


class _StubFlaskApp:
  """Minimal Flask application stub for route registration."""

  def route(self, *unused_args: Any, **unused_kwargs: Any) -> Any:
    return lambda func: func

  def run(self, *unused_args: Any, **unused_kwargs: Any) -> None:
    pass


class _StubFlaskRequest:
  """Minimal Flask request stub for autospeccing `flask.request`."""

  def get_json(self, silent: bool = False) -> Any:
    del silent
    return None


def _stub_jsonify(payload: Any) -> Any:
  return payload


def _load_ingestion_main() -> Any:
  stub_flask = types.SimpleNamespace(
      Flask=lambda _name: _StubFlaskApp(),
      Response=object,
      request=_StubFlaskRequest(),
      jsonify=_stub_jsonify,
  )
  stub_modules = {
      "bigquery_reconciler": bigquery_reconciler,
      "flask": stub_flask,
      "job_store": job_store,
      "utils": types.SimpleNamespace(prompts=prompts),
      "utils.prompts": prompts,
      "utils.config_manager": types.SimpleNamespace(
          ConfigManager=lambda _sheet_id: None
      ),
      "utils.logging_config": types.SimpleNamespace(setup_logging=lambda: None),
  }
  main_path = os.path.join(os.path.dirname(__file__), "main.py")
  spec = importlib.util.spec_from_file_location("ingestion_main", main_path)
  if spec is None or spec.loader is None:
    raise RuntimeError(f"Unable to load module spec from {main_path}")
  module = importlib.util.module_from_spec(spec)
  with (
      mock.patch.dict(os.environ, {}, clear=True),
      mock.patch.dict(sys.modules, stub_modules),
      mock.patch.object(sys, "path", list(sys.path)),
  ):
    spec.loader.exec_module(module)
  return module


main = _load_ingestion_main()


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


class IngestionMainTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_firestore = mock.create_autospec(
        firestore.Client, instance=True, spec_set=True
    )
    self.mock_tasks = mock.create_autospec(
        tasks_v2.CloudTasksClient, instance=True, spec_set=True
    )
    self.mock_bq = mock.create_autospec(
        bigquery.Client, instance=True, spec_set=True
    )
    self.mock_collection = mock.create_autospec(
        firestore.CollectionReference, instance=True, spec_set=True
    )
    self.mock_doc_ref = mock.create_autospec(
        firestore.DocumentReference, instance=True, spec_set=True
    )
    self.mock_doc_ref.id = "doc-123"
    self.mock_snapshot = _make_snapshot_mock(self.mock_doc_ref)
    self.mock_query_job = mock.create_autospec(
        bigquery.QueryJob, instance=True, spec_set=True
    )
    write_result = firestore.types.WriteResult(update_time=_CLAIM_UPDATE_TIME)

    self.mock_firestore.collection.return_value = self.mock_collection
    self.mock_firestore.write_option.side_effect = firestore.Client.write_option
    self.mock_collection.document.return_value = self.mock_doc_ref
    self.mock_doc_ref.get.return_value = self.mock_snapshot
    self.mock_doc_ref.create.return_value = write_result
    self.mock_doc_ref.update.return_value = write_result
    self.mock_tasks.queue_path.return_value = _QUEUE_PATH
    self.mock_bq.query.return_value = self.mock_query_job
    self.mock_query_job.num_dml_affected_rows = 0

    for attribute_name, patched_value in (
        ("GCP_PROJECT_ID", _PROJECT_ID),
        ("GCP_REGION", _REGION),
        ("TASK_QUEUE_ID", _QUEUE_ID),
        ("BIGQUERY_DATASET", _DATASET_ID),
        ("BIGQUERY_TABLE", _TABLE_ID),
        ("PROCESSING_SERVICE_URL", _PROCESSING_URL),
        ("INGESTION_SA_EMAIL", _SERVICE_ACCOUNT),
        ("_CONFIG_MANAGER", None),
        ("_TASK_QUEUE_PATH", _QUEUE_PATH),
        ("_FIRESTORE_CLIENT", self.mock_firestore),
        ("_TASKS_CLIENT", self.mock_tasks),
        ("_BQ_CLIENT", self.mock_bq),
    ):
      self.enter_context(mock.patch.object(main, attribute_name, patched_value))
    self.enter_context(
        mock.patch.dict(main.prompts.PROMPTS_CONFIG, {"v1": {}}, clear=True)
    )
    self.mock_request = mock.create_autospec(
        _StubFlaskRequest, instance=True, spec_set=True
    )
    self.enter_context(
        mock.patch.object(main.flask, "request", self.mock_request)
    )
    self.enter_context(
        mock.patch.object(
            main.flask,
            "jsonify",
            autospec=True,
            side_effect=lambda payload: payload,
        )
    )

  def _assert_queued_task_payload(
      self,
      expected_payload: Mapping[str, Any],
      *,
      expected_url: str = _PROCESSING_URL,
      expect_oidc_token: bool = True,
  ) -> None:
    self.mock_tasks.create_task.assert_called_once()
    call_kwargs = self.mock_tasks.create_task.call_args.kwargs
    self.assertEqual(call_kwargs["parent"], _QUEUE_PATH)
    queued_task = call_kwargs["task"]
    self.assertEqual(queued_task.http_request.url, expected_url)
    if expect_oidc_token:
      self.assertEqual(
          queued_task.http_request.oidc_token.service_account_email,
          _SERVICE_ACCOUNT,
      )
    else:
      self.assertNotIn("oidc_token", queued_task.http_request)
    actual_payload = json.loads(queued_task.http_request.body.decode())
    self.assertEqual(actual_payload, expected_payload)

  def _assert_bq_query_parameters(
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

  def test_health_check_returns_ok(self):
    response_body, status = main.health_check()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response_body, "OK")

  @parameterized.named_parameters(
      dict(
          testcase_name="numeric_string_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": "100",
          },
          expected_doc={
              "gcs_uri": _GCS_URI,
              "prompt_version_id": "v1",
              "status": "QUEUED",
              "queued_at": mock.ANY,
              "generation": 100,
          },
      ),
      dict(
          testcase_name="positive_int_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": 100,
          },
          expected_doc={
              "gcs_uri": _GCS_URI,
              "prompt_version_id": "v1",
              "status": "QUEUED",
              "queued_at": mock.ANY,
              "generation": 100,
          },
      ),
      dict(
          testcase_name="blank_whitespace_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": "   ",
          },
          expected_doc={
              "gcs_uri": _GCS_URI,
              "prompt_version_id": "v1",
              "status": "QUEUED",
              "queued_at": mock.ANY,
          },
      ),
  )
  def test_ingest_event_queues_new_image(
      self,
      payload: Mapping[str, Any],
      expected_doc: Mapping[str, Any],
  ):
    call_order: list[str] = []
    write_result = firestore.types.WriteResult(update_time=_CLAIM_UPDATE_TIME)
    self.mock_doc_ref.create.side_effect = lambda *_a, **_kw: (
        call_order.append("claim") or write_result
    )
    self.mock_tasks.create_task.side_effect = lambda *_a, **_kw: (
        call_order.append("create_task") or tasks_v2.Task()
    )
    self.mock_request.get_json.return_value = dict(payload)
    self.mock_snapshot.exists = False

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 1})
    self.assertEqual(call_order, ["claim", "create_task"])
    self._assert_queued_task_payload({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
    })
    self.mock_bq.query.assert_not_called()
    self.mock_doc_ref.create.assert_called_once_with(dict(expected_doc))

  def test_ingest_event_resolves_queue_path_and_omits_oidc_for_http_url(self):
    self.enter_context(mock.patch.object(main, "_TASK_QUEUE_PATH", None))
    self.enter_context(
        mock.patch.object(main, "PROCESSING_SERVICE_URL", _HTTP_PROCESSING_URL)
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }
    self.mock_snapshot.exists = False

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 1})
    self.mock_tasks.queue_path.assert_called_once_with(
        _PROJECT_ID, _REGION, _QUEUE_ID
    )
    self._assert_queued_task_payload(
        {"gcs_uri": _GCS_URI, "prompt_version_id": "v1"},
        expected_url=_HTTP_PROCESSING_URL,
        expect_oidc_token=False,
    )

  def test_ingest_event_uses_sheets_config_versions_when_available(self):
    fake_config_manager = types.SimpleNamespace(
        get_config=lambda: {"sheet_v2": {}}
    )
    self.enter_context(
        mock.patch.object(main, "_CONFIG_MANAGER", fake_config_manager)
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }
    self.mock_snapshot.exists = False

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 1})
    self._assert_queued_task_payload({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "sheet_v2",
    })

  @parameterized.named_parameters(
      dict(testcase_name="stale_generation", generation="99"),
      dict(testcase_name="duplicate_generation", generation="100"),
      dict(testcase_name="omitted_generation", generation=None),
  )
  def test_ingest_event_skips_duplicate_or_stale_generation(
      self, generation: str | None
  ):
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": generation,
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 0})
    self.mock_tasks.create_task.assert_not_called()
    self.mock_bq.query.assert_not_called()
    self.mock_doc_ref.create.assert_not_called()
    self.mock_doc_ref.update.assert_not_called()

  def test_ingest_overwrites_settled_job_and_deletes_version_rows_first(self):
    call_order: list[str] = []
    write_result = firestore.types.WriteResult(update_time=_CLAIM_UPDATE_TIME)
    self.mock_doc_ref.update.side_effect = lambda *_a, **_kw: (
        call_order.append("claim") or write_result
    )
    self.mock_bq.query.side_effect = lambda *_a, **_kw: (
        call_order.append("bq_delete") or self.mock_query_job
    )
    self.mock_tasks.create_task.side_effect = lambda *_a, **_kw: (
        call_order.append("create_task") or tasks_v2.Task()
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }
    self.mock_query_job.num_dml_affected_rows = 3

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 1})
    self.assertEqual(call_order, ["claim", "bq_delete", "create_task"])
    delete_sql = self.mock_bq.query.call_args.args[0]
    self.assertEqual(
        delete_sql,
        f"DELETE FROM `{_QUALIFIED_TABLE_ID}`\n"
        "WHERE\n"
        "  gcs_uri = @gcs_uri\n"
        "  AND prompt_version_id = @prompt_version_id;",
    )
    self._assert_bq_query_parameters({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
    })
    self.mock_doc_ref.update.assert_called_once_with(
        {
            "gcs_uri": _GCS_URI,
            "prompt_version_id": "v1",
            "status": "QUEUED",
            "queued_at": mock.ANY,
            "generation": 200,
            "attempts": firestore.DELETE_FIELD,
            "error_message": firestore.DELETE_FIELD,
            "last_processed_at": firestore.DELETE_FIELD,
        },
        option=firestore.Client.write_option(
            last_update_time=_SNAPSHOT_UPDATE_TIME
        ),
    )

  def test_ingest_event_overwrites_without_bq_dml_when_bq_unconfigured(self):
    self.enter_context(mock.patch.object(main, "BIGQUERY_DATASET", None))
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 1})
    self.mock_bq.query.assert_not_called()
    self._assert_queued_task_payload({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
    })

  def test_ingest_event_defers_when_prior_generation_is_in_flight(self):
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSING",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.SERVICE_UNAVAILABLE)
    self.assertEqual(
        response,
        {
            "status": "deferred",
            "tasks_created": 0,
            "deferred_versions": ["v1"],
        },
    )
    self.mock_bq.query.assert_not_called()
    self.mock_tasks.create_task.assert_not_called()
    self.mock_doc_ref.create.assert_not_called()
    self.mock_doc_ref.update.assert_not_called()

  def test_ingest_event_requeues_exhausted_failed_job_without_bq_delete(self):
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "100",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "FAILED_RETRY",
        "generation": 100,
        "attempts": 5,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 1})
    self.mock_bq.query.assert_not_called()
    self._assert_queued_task_payload({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
    })

  def test_ingest_event_skips_non_image_file(self):
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "notes.txt",
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, "Skipped")
    self.mock_firestore.collection.assert_not_called()
    self.mock_tasks.create_task.assert_not_called()

  @parameterized.named_parameters(
      dict(testcase_name="missing_json", payload=None),
      dict(testcase_name="non_dict_json", payload=["unexpected"]),
      dict(testcase_name="missing_bucket", payload={"name": "sample.jpg"}),
      dict(
          testcase_name="empty_bucket",
          payload={"bucket": "", "name": "sample.jpg"},
      ),
      dict(
          testcase_name="whitespace_bucket",
          payload={"bucket": "   ", "name": "sample.jpg"},
      ),
      dict(
          testcase_name="padded_whitespace_bucket",
          payload={"bucket": " audit-bucket ", "name": "sample.jpg"},
      ),
      dict(
          testcase_name="internal_whitespace_bucket",
          payload={"bucket": "audit bucket", "name": "sample.jpg"},
      ),
      dict(
          testcase_name="non_alnum_boundary_bucket",
          payload={"bucket": "-audit-bucket", "name": "sample.jpg"},
      ),
      dict(
          testcase_name="slash_in_bucket",
          payload={"bucket": "audit/bucket", "name": "sample.jpg"},
      ),
      dict(
          testcase_name="empty_name",
          payload={"bucket": "audit-bucket", "name": ""},
      ),
      dict(
          testcase_name="whitespace_name",
          payload={"bucket": "audit-bucket", "name": " \t "},
      ),
      dict(
          testcase_name="non_string_name",
          payload={"bucket": "audit-bucket", "name": 123},
      ),
      dict(
          testcase_name="bool_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": True,
          },
      ),
      dict(
          testcase_name="zero_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": 0,
          },
      ),
      dict(
          testcase_name="negative_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": "-5",
          },
      ),
      dict(
          testcase_name="overflow_int64_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": str(1 << 63),
          },
      ),
      dict(
          testcase_name="invalid_generation",
          payload={
              "bucket": "audit-bucket",
              "name": "sample.jpg",
              "generation": "not-an-int",
          },
      ),
  )
  def test_ingest_event_rejects_invalid_payload(self, payload: Any):
    self.mock_request.get_json.return_value = payload

    _, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.BAD_REQUEST)
    self.mock_tasks.create_task.assert_not_called()

  @parameterized.named_parameters(
      dict(
          testcase_name="firestore_credentials_error",
          client_attr="_FIRESTORE_CLIENT",
          client_cls=(firestore, "Client"),
          error=auth_exceptions.DefaultCredentialsError("Missing ADC"),
      ),
      dict(
          testcase_name="tasks_credentials_error",
          client_attr="_TASKS_CLIENT",
          client_cls=(tasks_v2, "CloudTasksClient"),
          error=auth_exceptions.RefreshError("Token refresh failed"),
      ),
      dict(
          testcase_name="firestore_api_error",
          client_attr="_FIRESTORE_CLIENT",
          client_cls=(firestore, "Client"),
          error=google_exceptions.ServiceUnavailable("Firestore unavailable"),
      ),
  )
  def test_ingest_event_returns_internal_error_when_client_init_fails(
      self,
      client_attr: str,
      client_cls: tuple[Any, str],
      error: Exception,
  ):
    self.enter_context(mock.patch.object(main, client_attr, None))
    target_module, class_name = client_cls
    self.enter_context(
        mock.patch.object(
            target_module,
            class_name,
            autospec=True,
            spec_set=True,
            side_effect=error,
        )
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(response, "Internal Server Error")
    self.mock_tasks.create_task.assert_not_called()

  def test_ingest_event_returns_internal_error_when_queue_config_missing(self):
    self.enter_context(mock.patch.object(main, "_TASK_QUEUE_PATH", None))
    self.enter_context(mock.patch.object(main, "TASK_QUEUE_ID", None))
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(response, "Internal Server Error")
    self.mock_tasks.create_task.assert_not_called()

  def test_ingest_event_skips_bq_delete_when_service_config_missing(self):
    self.enter_context(mock.patch.object(main, "PROCESSING_SERVICE_URL", None))
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v1"]},
    )
    self.mock_bq.query.assert_not_called()
    self.mock_tasks.create_task.assert_not_called()
    self.mock_doc_ref.update.assert_not_called()

  @parameterized.named_parameters(
      dict(
          testcase_name="api_error",
          error=google_exceptions.ServiceUnavailable("Cloud Tasks unavailable"),
      ),
      dict(
          testcase_name="retry_error",
          error=google_exceptions.RetryError(
              "Deadline exceeded", cause=Exception("503")
          ),
      ),
      dict(
          testcase_name="auth_refresh_error",
          error=auth_exceptions.RefreshError("Token refresh failed"),
      ),
  )
  def test_ingest_event_reports_partial_failure_and_rolls_back_failed_version(
      self, error: Exception
  ):
    self.enter_context(
        mock.patch.dict(
            main.prompts.PROMPTS_CONFIG, {"v1": {}, "v2": {}}, clear=True
        )
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }
    self.mock_snapshot.exists = False
    self.mock_tasks.create_task.side_effect = [tasks_v2.Task(), error]

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 1, "failed_versions": ["v2"]},
    )
    self.assertEqual(self.mock_tasks.create_task.call_count, 2)
    self.assertEqual(self.mock_doc_ref.create.call_count, 2)
    self.mock_doc_ref.delete.assert_called_once_with(
        option=firestore.Client.write_option(
            last_update_time=_CLAIM_UPDATE_TIME
        )
    )

  def test_ingest_event_enqueues_settled_version_and_defers_in_flight_sibling(
      self,
  ):
    self.enter_context(
        mock.patch.dict(
            main.prompts.PROMPTS_CONFIG, {"v1": {}, "v2": {}}, clear=True
        )
    )
    doc_ref_v1 = mock.create_autospec(
        firestore.DocumentReference, instance=True, spec_set=True
    )
    doc_ref_v2 = mock.create_autospec(
        firestore.DocumentReference, instance=True, spec_set=True
    )
    write_result = firestore.types.WriteResult(update_time=_CLAIM_UPDATE_TIME)
    doc_ref_v1.update.return_value = write_result
    doc_ref_v1.get.return_value = _make_snapshot_mock(
        doc_ref_v1,
        exists=True,
        data={"status": "PROCESSED", "generation": 100},
    )
    doc_ref_v2.get.return_value = _make_snapshot_mock(
        doc_ref_v2,
        exists=True,
        data={"status": "PROCESSING", "generation": 100},
    )
    self.mock_collection.document.side_effect = [doc_ref_v1, doc_ref_v2]
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.SERVICE_UNAVAILABLE)
    self.assertEqual(
        response,
        {
            "status": "deferred",
            "tasks_created": 1,
            "deferred_versions": ["v2"],
        },
    )
    self._assert_queued_task_payload({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
    })
    doc_ref_v2.update.assert_not_called()

  def test_ingest_event_prioritizes_error_over_deferred_across_versions(self):
    self.enter_context(
        mock.patch.dict(
            main.prompts.PROMPTS_CONFIG, {"v1": {}, "v2": {}}, clear=True
        )
    )
    doc_ref_v1 = mock.create_autospec(
        firestore.DocumentReference, instance=True, spec_set=True
    )
    doc_ref_v2 = mock.create_autospec(
        firestore.DocumentReference, instance=True, spec_set=True
    )
    doc_ref_v1.get.return_value = _make_snapshot_mock(
        doc_ref_v1,
        exists=True,
        data={"status": "PROCESSING", "generation": 100},
    )
    doc_ref_v2.get.side_effect = google_exceptions.ServiceUnavailable(
        "Firestore read failed"
    )
    self.mock_collection.document.side_effect = [doc_ref_v1, doc_ref_v2]
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v2"]},
    )

  def test_ingest_event_restores_previous_doc_when_overwrite_bq_delete_fails(
      self,
  ):
    self.mock_bq.query.side_effect = google_exceptions.InternalServerError(
        "BigQuery DML failure"
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v1"]},
    )
    self.mock_tasks.create_task.assert_not_called()
    self.assertEqual(self.mock_doc_ref.update.call_count, 2)
    self.assertEqual(
        self.mock_doc_ref.update.call_args_list[1],
        mock.call(
            {
                "status": "PROCESSED",
                "generation": 100,
                "gcs_uri": firestore.DELETE_FIELD,
                "prompt_version_id": firestore.DELETE_FIELD,
                "queued_at": firestore.DELETE_FIELD,
            },
            option=firestore.Client.write_option(
                last_update_time=_CLAIM_UPDATE_TIME
            ),
        ),
    )

  def test_ingest_marks_failed_retry_when_create_task_fails_after_bq_delete(
      self,
  ):
    self.mock_tasks.create_task.side_effect = (
        google_exceptions.ServiceUnavailable("Cloud Tasks unavailable")
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v1"]},
    )
    self.assertEqual(self.mock_doc_ref.update.call_count, 2)
    self.assertEqual(
        self.mock_doc_ref.update.call_args_list[1],
        mock.call(
            {"status": "FAILED_RETRY", "attempts": 5},
            option=firestore.Client.write_option(
                last_update_time=_CLAIM_UPDATE_TIME
            ),
        ),
    )

  @parameterized.named_parameters(
      dict(
          testcase_name="create_already_exists",
          exists=False,
          claim_error=google_exceptions.AlreadyExists("Already claimed"),
      ),
      dict(
          testcase_name="overwrite_failed_precondition",
          exists=True,
          claim_error=google_exceptions.FailedPrecondition("Stale update_time"),
      ),
  )
  def test_ingest_event_aborts_when_claim_precondition_loses_race(
      self, exists: bool, claim_error: Exception
  ):
    self.mock_doc_ref.create.side_effect = claim_error
    self.mock_doc_ref.update.side_effect = claim_error
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = exists
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v1"]},
    )
    self.mock_bq.query.assert_not_called()
    self.mock_tasks.create_task.assert_not_called()
    self.mock_doc_ref.delete.assert_not_called()

  def test_ingest_event_returns_internal_error_when_bq_client_init_fails(self):
    self.enter_context(mock.patch.object(main, "_BQ_CLIENT", None))
    self.enter_context(
        mock.patch.object(
            bigquery,
            "Client",
            autospec=True,
            spec_set=True,
            side_effect=auth_exceptions.DefaultCredentialsError("Missing ADC"),
        )
    )
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v1"]},
    )
    self.mock_tasks.create_task.assert_not_called()
    self.assertEqual(self.mock_doc_ref.update.call_count, 2)

  def test_ingest_event_returns_internal_error_when_bq_table_config_invalid(
      self,
  ):
    self.enter_context(mock.patch.object(main, "BIGQUERY_TABLE", "bad`table"))
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
        "generation": "200",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {
        "status": "PROCESSED",
        "generation": 100,
    }

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v1"]},
    )
    self.mock_bq.query.assert_not_called()
    self.mock_tasks.create_task.assert_not_called()
    self.mock_doc_ref.update.assert_not_called()


if __name__ == "__main__":
  absltest.main()
