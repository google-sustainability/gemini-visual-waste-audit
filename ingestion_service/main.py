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

"""Cloud Run service to ingest and dispatch image processing tasks.

This service is triggered by Eventarc (GCS Object Finalized) events.

**Workflow:**
1.  Receives a CloudEvent via HTTP POST at `/ingest`.
2.  Extracts the GCS bucket and object name.
3.  Iterates through all configured prompt versions.
4.  Creates a unique task for each version in a Google Cloud Tasks queue,
    targeting the processing service.
5.  Creates/Updates a document in Firestore to mark the job as 'QUEUED'.
"""

import datetime
import hashlib
import http
import json
import logging
import os
import sys
from typing import Optional

import flask
from google.api_core import exceptions as google_exceptions
from google.auth import exceptions as auth_exceptions
from google.cloud import firestore
from google.cloud import tasks_v2



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

app = flask.Flask(__name__)

# --- Global Clients ---
setup_logging()
_LOGGER = logging.getLogger(__name__)
_FIRESTORE_CLIENT: Optional[firestore.Client] = None
_TASKS_CLIENT: Optional[tasks_v2.CloudTasksClient] = None
_TASK_QUEUE_PATH: Optional[str] = None
_CONFIG_MANAGER = ConfigManager(SPREADSHEET_ID) if SPREADSHEET_ID else None


def _get_firestore_client() -> firestore.Client:
  """Lazy initializes and returns the Firestore client."""
  global _FIRESTORE_CLIENT
  if _FIRESTORE_CLIENT is None:
    try:
      _FIRESTORE_CLIENT = firestore.Client()
    except (
        auth_exceptions.DefaultCredentialsError,
        google_exceptions.GoogleAPICallError,
    ) as e:
      _LOGGER.critical("Failed to initialize Firestore client: %s", e)
      raise
  return _FIRESTORE_CLIENT


def _get_tasks_client() -> tasks_v2.CloudTasksClient:
  """Lazy initializes and returns the Cloud Tasks client."""
  global _TASKS_CLIENT
  if _TASKS_CLIENT is None:
    try:
      _TASKS_CLIENT = tasks_v2.CloudTasksClient()
    except (
        auth_exceptions.DefaultCredentialsError,
        google_exceptions.GoogleAPICallError,
    ) as e:
      _LOGGER.critical("Failed to initialize Cloud Tasks client: %s", e)
      raise
  return _TASKS_CLIENT


def _get_task_queue_path() -> str:
  """Lazy initializes and returns the Task Queue path."""
  global _TASK_QUEUE_PATH
  if _TASK_QUEUE_PATH is None:
    tasks_client = _get_tasks_client()
    try:
      _TASK_QUEUE_PATH = tasks_client.queue_path(
          GCP_PROJECT_ID, GCP_REGION, TASK_QUEUE_ID
      )
    except google_exceptions.GoogleAPICallError as e:
      _LOGGER.critical("Failed to get task queue path: %s", e)
      raise
  return _TASK_QUEUE_PATH


def _create_task(
    gcs_uri: str, prompt_version_id: str
) -> Optional[tasks_v2.Task]:
  """Creates a Cloud Task with an OIDC token for the processing service."""
  if not PROCESSING_SERVICE_URL or not INGESTION_SA_EMAIL:
    _LOGGER.critical(
        "PROCESSING_SERVICE_URL or INGESTION_SA_EMAIL not set. "
        "Cannot create authenticated task."
    )
    return None

  task_request = {
      "http_method": tasks_v2.types.HttpMethod.POST,
      "url": PROCESSING_SERVICE_URL,
      "headers": {"Content-type": "application/json"},
      "body": (
          json.dumps(
              {"gcs_uri": gcs_uri, "prompt_version_id": prompt_version_id}
          ).encode()
      ),
  }

  if PROCESSING_SERVICE_URL.startswith("https://"):
    task_request["oidc_token"] = {"service_account_email": INGESTION_SA_EMAIL}

  return tasks_v2.Task(http_request=task_request)


@app.route("/", methods=["GET"])
def health_check():
  return "OK", http.HTTPStatus.OK


@app.route("/ingest", methods=["POST"])
def ingest_event():
  """HTTP endpoint to handle CloudEvents from Eventarc (GCS Object Finalized)."""
  # CloudEvents are typically JSON
  event_data = flask.request.get_json(silent=True)
  if not event_data:
    _LOGGER.warning("No JSON payload received.")
    return "Bad Request", http.HTTPStatus.BAD_REQUEST

  # Handle standard GCS notification payload or CloudEvent data
  # Eventarc sends the data payload in the body.
  # Structure: {"bucket": "...", "name": "..."}
  bucket = event_data.get("bucket")
  name = event_data.get("name")

  if not bucket or not name:
    _LOGGER.warning("Invalid event data: %s", event_data)
    return "Invalid event data", http.HTTPStatus.BAD_REQUEST

  # Only process image files to avoid unnecessary processing and potential
  # errors.
  if not name.lower().endswith((".jpg", ".jpeg", ".png")):
    _LOGGER.info("Skipping non-image file: %s", name)
    return "Skipped", http.HTTPStatus.OK

  gcs_uri = f"gs://{bucket}/{name}"
  _LOGGER.info("Ingesting file: %s", gcs_uri)

  tasks_created_count = 0
  jobs_collection = _get_firestore_client().collection(FIRESTORE_COLLECTION)

  prompt_versions = []
  if _CONFIG_MANAGER:
    config = _CONFIG_MANAGER.get_config()
    if config:
      prompt_versions = list(config.keys())

  if not prompt_versions:
    _LOGGER.warning("Using local fallback version keys.")
    prompt_versions = list(prompts.PROMPTS_CONFIG.keys())

  # Fan-out: Create a task for each prompt version
  for prompt_version_id in prompt_versions:
    # Use a safe composite key that works as a Firestore document ID
    # Replace '/' in gcs_uri to avoid sub-collection interpretation if
    # necessary, but hashing is safer.
    composite_key_raw = f"{gcs_uri}:{prompt_version_id}"
    composite_key = hashlib.sha256(composite_key_raw.encode()).hexdigest()
    _LOGGER.info("Composite key: %s -> %s", composite_key_raw, composite_key)

    doc_ref = jobs_collection.document(composite_key)

    # Idempotency check: If already queued or processed, skip
    doc_snapshot = doc_ref.get()
    if doc_snapshot.exists:
      status = doc_snapshot.get("status")
      if status in ["QUEUED", "PROCESSING", "PROCESSED"]:
        _LOGGER.info(
            "Job %s already in status %s. Skipping.", composite_key, status
        )
        continue
    tasks_client = _get_tasks_client()
    task = _create_task(gcs_uri, prompt_version_id)
    if not task:
      continue

    try:
      tasks_client.create_task(parent=_get_task_queue_path(), task=task)
      _LOGGER.info("Created task for %s", gcs_uri)
      doc_ref.set({
          "gcs_uri": gcs_uri,
          "prompt_version_id": prompt_version_id,
          "status": "QUEUED",
          "queued_at": datetime.datetime.now(datetime.timezone.utc),
      })
      tasks_created_count += 1

    except google_exceptions.GoogleAPICallError as api_error:
      _LOGGER.error(
          "Error creating task or Firestore doc for %s: %s",
          composite_key,
          api_error,
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
