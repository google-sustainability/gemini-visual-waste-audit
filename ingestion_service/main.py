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

"""Cloud Run service to ingest and reconcile image processing tasks.

Handles Eventarc GCS Object Finalized (`/ingest`) CloudEvents.
"""

import dataclasses
import datetime
import http
import json
import logging
import os
import sys
from typing import Any

import bigquery_reconciler
import flask
from google.api_core import exceptions as google_exceptions
from google.auth import exceptions as auth_exceptions
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import tasks_v2
import job_store



# Add the parent directory to the Python path to allow imports from 'utils'
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from utils import prompts
from utils.config_manager import ConfigManager
from utils.logging_config import setup_logging




# --- Configuration ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_REGION = os.environ.get("GCP_REGION")
TASK_QUEUE_ID = os.environ.get("TASK_QUEUE_ID")
PROCESSING_SERVICE_URL = os.environ.get("PROCESSING_SERVICE_URL")
FIRESTORE_COLLECTION = os.environ.get(
    "FIRESTORE_COLLECTION", "image_audit_jobs"
)
INGESTION_SA_EMAIL = os.environ.get("INGESTION_SA_EMAIL")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.environ.get("BIGQUERY_TABLE")

_SUPPORTED_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png")

app = flask.Flask(__name__)

# --- Global Clients ---
setup_logging()
_LOGGER = logging.getLogger(__name__)
_FIRESTORE_CLIENT: firestore.Client | None = None
_TASKS_CLIENT: tasks_v2.CloudTasksClient | None = None
_BQ_CLIENT: bigquery.Client | None = None
_TASK_QUEUE_PATH: str | None = None
_CONFIG_MANAGER = ConfigManager(SPREADSHEET_ID) if SPREADSHEET_ID else None


@dataclasses.dataclass(frozen=True, kw_only=True)
class _GcsEvent:
  """Validated GCS object metadata extracted from an Eventarc CloudEvent.

  Attributes:
    bucket: GCS bucket name containing the object.
    name: GCS object path within the bucket.
    generation: Optional positive integer GCS object generation number.
  """

  bucket: str
  name: str
  generation: int | None = None

  @property
  def gcs_uri(self) -> str:
    """The full `gs://` URI of the object."""
    return f"gs://{self.bucket}/{self.name}"


def _get_firestore_client() -> firestore.Client:
  """Lazy initializes and returns the Firestore client."""
  global _FIRESTORE_CLIENT
  if _FIRESTORE_CLIENT is None:
    _FIRESTORE_CLIENT = firestore.Client()
  return _FIRESTORE_CLIENT


def _get_tasks_client() -> tasks_v2.CloudTasksClient:
  """Lazy initializes and returns the Cloud Tasks client."""
  global _TASKS_CLIENT
  if _TASKS_CLIENT is None:
    _TASKS_CLIENT = tasks_v2.CloudTasksClient()
  return _TASKS_CLIENT


def _get_bq_client() -> bigquery.Client:
  """Lazy initializes and returns the BigQuery client."""
  global _BQ_CLIENT
  if _BQ_CLIENT is None:
    _BQ_CLIENT = bigquery.Client(project=GCP_PROJECT_ID)
  return _BQ_CLIENT


def _get_task_queue_path() -> str:
  """Lazy initializes and returns the Cloud Tasks queue resource path.

  Returns:
    The fully qualified Cloud Tasks queue resource path.

  Raises:
    RuntimeError: If `GCP_PROJECT_ID`, `GCP_REGION`, or `TASK_QUEUE_ID` is
      unset.
  """
  global _TASK_QUEUE_PATH
  if _TASK_QUEUE_PATH is None:
    if not (GCP_PROJECT_ID and GCP_REGION and TASK_QUEUE_ID):
      raise RuntimeError(
          "GCP_PROJECT_ID, GCP_REGION, and TASK_QUEUE_ID must be set."
      )
    _TASK_QUEUE_PATH = _get_tasks_client().queue_path(
        GCP_PROJECT_ID, GCP_REGION, TASK_QUEUE_ID
    )
  return _TASK_QUEUE_PATH


def _parse_gcs_event(event_data: Any) -> _GcsEvent | None:
  """Parses an Eventarc payload into a `_GcsEvent`, or returns `None`.

  Args:
    event_data: Decoded JSON body from the HTTP request.

  Returns:
    A validated `_GcsEvent`, or `None` if `event_data` is not a dict with a
    valid GCS `bucket` (alphanumeric boundaries, no `/` or whitespace),
    non-blank `name`, and optional positive integer `generation`.
  """
  if not isinstance(event_data, dict):
    return None
  bucket = event_data.get("bucket")
  name = event_data.get("name")
  is_valid_bucket = (
      isinstance(bucket, str)
      and bool(bucket)
      and not any(ch.isspace() for ch in bucket)
      and "/" not in bucket
      and bucket[0].isalnum()
      and bucket[-1].isalnum()
  )
  is_valid_object_target = (
      is_valid_bucket and isinstance(name, str) and bool(name.strip())
  )
  if not is_valid_object_target:
    return None

  raw_generation = event_data.get("generation")
  is_unset_generation = raw_generation is None or (
      isinstance(raw_generation, str) and not raw_generation.strip()
  )
  if is_unset_generation:
    generation = None
  else:
    generation = job_store.parse_generation(raw_generation)
    if generation is None:
      return None

  return _GcsEvent(bucket=bucket, name=name, generation=generation)


def _is_supported_image(object_name: str) -> bool:
  """Returns whether `object_name` has a supported image file extension."""
  return object_name.lower().endswith(_SUPPORTED_IMAGE_EXTENSIONS)


def _create_task(*, gcs_uri: str, prompt_version_id: str) -> tasks_v2.Task:
  """Creates a Cloud Task with an OIDC token for the processing service.

  Args:
    gcs_uri: Full `gs://` URI of the uploaded image.
    prompt_version_id: Configured prompt version key to execute.

  Returns:
    A configured `tasks_v2.Task` targeting `PROCESSING_SERVICE_URL`.

  Raises:
    RuntimeError: If `PROCESSING_SERVICE_URL` or `INGESTION_SA_EMAIL` is unset.
  """
  if not PROCESSING_SERVICE_URL or not INGESTION_SA_EMAIL:
    raise RuntimeError(
        "PROCESSING_SERVICE_URL and INGESTION_SA_EMAIL must be set to "
        "create an authenticated task."
    )

  payload = {"gcs_uri": gcs_uri, "prompt_version_id": prompt_version_id}
  oidc_token = (
      tasks_v2.OidcToken(service_account_email=INGESTION_SA_EMAIL)
      if PROCESSING_SERVICE_URL.startswith("https://")
      else None
  )
  http_request = tasks_v2.HttpRequest(
      http_method=tasks_v2.HttpMethod.POST,
      url=PROCESSING_SERVICE_URL,
      headers={"Content-type": "application/json"},
      body=json.dumps(payload).encode(),
      oidc_token=oidc_token,
  )
  return tasks_v2.Task(http_request=http_request)


def _get_prompt_versions() -> list[str]:
  """Returns configured prompt version IDs from Sheets or local fallback."""
  if _CONFIG_MANAGER is not None:
    sheets_config = _CONFIG_MANAGER.get_config()
    if sheets_config:
      return list(sheets_config.keys())
  _LOGGER.warning("Using local fallback version keys.")
  return list(prompts.PROMPTS_CONFIG.keys())


def _enqueue_prompt_version_job(
    *,
    firestore_client: firestore.Client,
    jobs_collection: firestore.CollectionReference,
    tasks_client: tasks_v2.CloudTasksClient,
    task_queue_path: str,
    event: _GcsEvent,
    prompt_version_id: str,
) -> job_store.EnqueueOutcome:
  """Reconciles state and enqueues a prompt version job if needed.

  Args:
    firestore_client: Firestore client used to build write preconditions.
    jobs_collection: Firestore collection tracking ingestion jobs.
    tasks_client: Cloud Tasks client used to dispatch processing tasks.
    task_queue_path: Fully qualified Cloud Tasks queue resource path.
    event: Validated GCS object event metadata.
    prompt_version_id: Prompt version key to reconcile and enqueue.

  Returns:
    `EnqueueOutcome.QUEUED` when a Cloud Task and Firestore record were created,
    `EnqueueOutcome.SKIPPED` when an active or newer job already covers the
    event, or `EnqueueOutcome.DEFERRED_IN_FLIGHT` when a prior generation is
    still executing.

  Raises:
    RuntimeError: If task creation configuration is missing.
    ValueError: If BigQuery table configuration identifiers are invalid.
    auth_exceptions.GoogleAuthError: If Firestore, BigQuery, or Cloud Tasks
      authentication fails.
    google_exceptions.GoogleAPIError: If Firestore, BigQuery, or Cloud Tasks
      fails.
  """
  doc_id = job_store.make_job_doc_id(event.gcs_uri, prompt_version_id)
  doc_ref = jobs_collection.document(doc_id)

  doc_snapshot = doc_ref.get()
  decision = job_store.ReconcileDecision(
      outcome=job_store.EnqueueOutcome.QUEUED
  )
  if doc_snapshot.exists:
    existing_job_data = doc_snapshot.to_dict() or {}
    decision = job_store.reconcile_existing_job(
        existing_job_data, event_generation=event.generation
    )
    if decision.outcome == job_store.EnqueueOutcome.SKIPPED:
      _LOGGER.info(
          "Skipping %s (%s) at generation=%s (stored_status=%s,"
          " stored_generation=%s).",
          event.gcs_uri,
          prompt_version_id,
          event.generation,
          existing_job_data.get("status"),
          existing_job_data.get("generation"),
      )
      return job_store.EnqueueOutcome.SKIPPED
    if decision.outcome == job_store.EnqueueOutcome.DEFERRED_IN_FLIGHT:
      _LOGGER.info(
          "Deferring %s (%s) at generation=%s while prior generation=%s is %s.",
          event.gcs_uri,
          prompt_version_id,
          event.generation,
          existing_job_data.get("generation"),
          existing_job_data.get("status"),
      )
      return job_store.EnqueueOutcome.DEFERRED_IN_FLIGHT

  task = _create_task(
      gcs_uri=event.gcs_uri, prompt_version_id=prompt_version_id
  )
  table_id = None
  if decision.delete_superseded_rows:
    table_id = bigquery_reconciler.resolve_table_id(
        project_id=GCP_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        table_id=BIGQUERY_TABLE,
    )

  job_document: dict[str, Any] = {
      "gcs_uri": event.gcs_uri,
      "prompt_version_id": prompt_version_id,
      "status": job_store.JobStatus.QUEUED,
      "queued_at": datetime.datetime.now(datetime.timezone.utc),
  }
  if event.generation is not None:
    job_document["generation"] = event.generation

  claimed_job = job_store.claim_queued_job(
      firestore_client=firestore_client,
      doc_ref=doc_ref,
      doc_snapshot=doc_snapshot,
      job_document=job_document,
  )
  bq_rows_deleted = False
  try:
    if table_id is not None:
      deleted_rows = bigquery_reconciler.delete_rows(
          _get_bq_client(),
          table_id=table_id,
          gcs_uri=event.gcs_uri,
          prompt_version_id=prompt_version_id,
      )
      bq_rows_deleted = True
      _LOGGER.info(
          "Deleted %d superseded BigQuery rows for %s (%s).",
          deleted_rows,
          event.gcs_uri,
          prompt_version_id,
      )
    tasks_client.create_task(parent=task_queue_path, task=task)
  except Exception:
    job_store.rollback_claimed_job(
        firestore_client=firestore_client,
        claimed_job=claimed_job,
        bq_rows_deleted=bq_rows_deleted,
    )
    raise

  _LOGGER.info("Created task for %s (%s).", event.gcs_uri, prompt_version_id)
  return job_store.EnqueueOutcome.QUEUED


@app.route("/", methods=["GET"])
def health_check() -> tuple[str, http.HTTPStatus]:
  """Returns 200 OK for service health checks."""
  return "OK", http.HTTPStatus.OK


@app.route("/ingest", methods=["POST"])
def ingest_event() -> tuple[str | flask.Response, http.HTTPStatus]:
  """Handles CloudEvents from Eventarc (GCS Object Finalized)."""
  event_data = flask.request.get_json(silent=True)
  if event_data is None:
    _LOGGER.warning("No JSON payload received.")
    return "Bad Request", http.HTTPStatus.BAD_REQUEST

  event = _parse_gcs_event(event_data)
  if event is None:
    _LOGGER.warning("Invalid event data: %r", event_data)
    return "Invalid event data", http.HTTPStatus.BAD_REQUEST

  if not _is_supported_image(event.name):
    _LOGGER.info("Skipping non-image file: %s", event.name)
    return "Skipped", http.HTTPStatus.OK

  _LOGGER.info("Ingesting file: %s", event.gcs_uri)
  try:
    firestore_client = _get_firestore_client()
    jobs_collection = firestore_client.collection(FIRESTORE_COLLECTION)
    tasks_client = _get_tasks_client()
    task_queue_path = _get_task_queue_path()
    prompt_versions = _get_prompt_versions()
  except (
      auth_exceptions.GoogleAuthError,
      google_exceptions.GoogleAPIError,
      RuntimeError,
  ):
    _LOGGER.exception(
        "Failed to initialize ingestion dependencies for %s", event.gcs_uri
    )
    return "Internal Server Error", http.HTTPStatus.INTERNAL_SERVER_ERROR

  tasks_created_count = 0
  deferred_versions: list[str] = []
  failed_versions: list[str] = []
  for prompt_version_id in prompt_versions:
    try:
      outcome = _enqueue_prompt_version_job(
          firestore_client=firestore_client,
          jobs_collection=jobs_collection,
          tasks_client=tasks_client,
          task_queue_path=task_queue_path,
          event=event,
          prompt_version_id=prompt_version_id,
      )
    except (
        auth_exceptions.GoogleAuthError,
        google_exceptions.GoogleAPIError,
        RuntimeError,
        ValueError,
    ):
      _LOGGER.exception(
          "Error reconciling or creating task for %s (%s)",
          event.gcs_uri,
          prompt_version_id,
      )
      failed_versions.append(prompt_version_id)
    else:
      if outcome == job_store.EnqueueOutcome.QUEUED:
        tasks_created_count += 1
      elif outcome == job_store.EnqueueOutcome.DEFERRED_IN_FLIGHT:
        deferred_versions.append(prompt_version_id)

  if failed_versions:
    return (
        flask.jsonify({
            "status": "error",
            "tasks_created": tasks_created_count,
            "failed_versions": failed_versions,
        }),
        http.HTTPStatus.INTERNAL_SERVER_ERROR,
    )

  if deferred_versions:
    return (
        flask.jsonify({
            "status": "deferred",
            "tasks_created": tasks_created_count,
            "deferred_versions": deferred_versions,
        }),
        http.HTTPStatus.SERVICE_UNAVAILABLE,
    )

  _LOGGER.info("Ingestion complete. Created %d new tasks.", tasks_created_count)
  return (
      flask.jsonify(
          {"status": "success", "tasks_created": tasks_created_count}
      ),
      http.HTTPStatus.OK,
  )


if __name__ == "__main__":
  app.run(
      host="0.0.0.0",
      port=int(os.environ.get("PORT", 8080)),
  )
