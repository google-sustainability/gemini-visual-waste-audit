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

"""Behavioral unit tests for Eventarc image ingestion routes."""

from collections.abc import Mapping
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
from google.cloud import firestore
from google.cloud import tasks_v2

from utils import prompts
from absl.testing import absltest
from absl.testing import parameterized

_PROJECT_ID = "test-project"
_REGION = "us-central1"
_QUEUE_ID = "audit-queue"
_QUEUE_PATH = "projects/test-project/locations/us-central1/queues/audit-queue"
_PROCESSING_URL = "https://processing.example.com/process"
_HTTP_PROCESSING_URL = "http://localhost:8080/process"
_SERVICE_ACCOUNT = "ingest@test-project.iam.gserviceaccount.com"
_GCS_URI = "gs://audit-bucket/sample.jpg"


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
      "flask": stub_flask,
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


class IngestionMainTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_firestore = mock.create_autospec(firestore.Client, instance=True)
    self.mock_tasks = mock.create_autospec(
        tasks_v2.CloudTasksClient, instance=True
    )
    self.mock_collection = mock.create_autospec(
        firestore.CollectionReference, instance=True
    )
    self.mock_doc_ref = mock.create_autospec(
        firestore.DocumentReference, instance=True
    )
    self.mock_snapshot = mock.create_autospec(
        firestore.DocumentSnapshot, instance=True
    )

    self.mock_firestore.collection.return_value = self.mock_collection
    self.mock_collection.document.return_value = self.mock_doc_ref
    self.mock_doc_ref.get.return_value = self.mock_snapshot
    self.mock_tasks.queue_path.return_value = _QUEUE_PATH

    for attribute_name, patched_value in (
        ("GCP_PROJECT_ID", _PROJECT_ID),
        ("GCP_REGION", _REGION),
        ("TASK_QUEUE_ID", _QUEUE_ID),
        ("PROCESSING_SERVICE_URL", _PROCESSING_URL),
        ("INGESTION_SA_EMAIL", _SERVICE_ACCOUNT),
        ("_CONFIG_MANAGER", None),
        ("_TASK_QUEUE_PATH", _QUEUE_PATH),
        ("_FIRESTORE_CLIENT", self.mock_firestore),
        ("_TASKS_CLIENT", self.mock_tasks),
    ):
      self.enter_context(mock.patch.object(main, attribute_name, patched_value))
    self.enter_context(
        mock.patch.dict(main.prompts.PROMPTS_CONFIG, {"v1": {}}, clear=True)
    )
    self.mock_request = mock.create_autospec(_StubFlaskRequest, instance=True)
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

  def test_health_check_returns_ok(self):
    response_body, status = main.health_check()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response_body, "OK")

  def test_ingest_event_queues_new_image(self):
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
        "prompt_version_id": "v1",
    })
    self.mock_doc_ref.set.assert_called_once_with({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
        "status": "QUEUED",
        "queued_at": mock.ANY,
    })

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
      dict(testcase_name="queued_job", job_status="QUEUED"),
      dict(testcase_name="processing_job", job_status="PROCESSING"),
      dict(testcase_name="processed_job", job_status="PROCESSED"),
  )
  def test_ingest_event_skips_active_job(self, job_status: str):
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {"status": job_status}

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 0})
    self.mock_tasks.create_task.assert_not_called()
    self.mock_doc_ref.set.assert_not_called()

  def test_ingest_event_requeues_non_active_job_status(self):
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }
    self.mock_snapshot.exists = True
    self.mock_snapshot.to_dict.return_value = {"status": "FAILED_RETRY"}

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.OK)
    self.assertEqual(response, {"status": "success", "tasks_created": 1})
    self._assert_queued_task_payload({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
    })
    self.mock_doc_ref.set.assert_called_once_with({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
        "status": "QUEUED",
        "queued_at": mock.ANY,
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

  def test_ingest_event_returns_internal_error_when_service_config_missing(
      self,
  ):
    self.enter_context(mock.patch.object(main, "PROCESSING_SERVICE_URL", None))
    self.mock_request.get_json.return_value = {
        "bucket": "audit-bucket",
        "name": "sample.jpg",
    }
    self.mock_snapshot.exists = False

    response, status = main.ingest_event()

    self.assertEqual(status, http.HTTPStatus.INTERNAL_SERVER_ERROR)
    self.assertEqual(
        response,
        {"status": "error", "tasks_created": 0, "failed_versions": ["v1"]},
    )
    self.mock_tasks.create_task.assert_not_called()
    self.mock_doc_ref.set.assert_not_called()

  @parameterized.named_parameters(
      dict(
          testcase_name="api_error",
          error=google_exceptions.ServiceUnavailable("Cloud Tasks unavailable"),
      ),
      dict(
          testcase_name="auth_refresh_error",
          error=auth_exceptions.RefreshError("Token refresh failed"),
      ),
  )
  def test_ingest_event_reports_partial_failure_across_prompt_versions(
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
    self.mock_doc_ref.set.assert_called_once_with({
        "gcs_uri": _GCS_URI,
        "prompt_version_id": "v1",
        "status": "QUEUED",
        "queued_at": mock.ANY,
    })


if __name__ == "__main__":
  absltest.main()
