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

"""Cloud Run service to process images using the Gemini API.

This service is triggered by tasks from a Google Cloud Tasks queue.

**Workflow:**
1.  Receives a task containing a `gcs_uri` and `prompt_version_id`.
2.  Updates the job status in Firestore to 'PROCESSING'.
3.  Downloads the specified image file from Google Cloud Storage.
4.  Processes the image using the Gemini API with a versioned prompt.
5.  Generates a 200x200 WebP thumbnail and uploads to GCS.
6.  Generates 7-day V4 Signed URLs for raw image and thumbnail.
7.  Flattens the nested JSON response from Gemini into a relational format.
8.  Writes the resulting data (including Signed URLs) to a BigQuery table.
9.  Updates the final job status in Firestore to 'PROCESSED' or 'FAILED'.
"""

import collections
import datetime
import hashlib
import http
import json
import logging
import os
import sys
from typing import Any, Dict, Generator, Optional, Union

import flask
from google.api_core import exceptions as google_api_exceptions
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud.firestore_v1 import document



# Add current directory and parent directory to sys.path for open-source
# import compatibility
_CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, _CURRENT_DIR)
sys.path.insert(0, os.path.abspath(os.path.join(_CURRENT_DIR, "..")))

import image_processor
import signed_url_refresher

from utils import bigquery_writer
from utils import config_manager
from utils import gcs_handler
from utils import gemini_processor
from utils import logging_config
from utils import prompts




# --- Configuration ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_LOCATION = os.getenv("LOCATION", "us-central1")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.environ.get("BIGQUERY_TABLE")
BIGQUERY_ERRORS_TABLE = os.environ.get("BIGQUERY_ERRORS_TABLE")
FIRESTORE_COLLECTION = os.environ.get(
    "FIRESTORE_COLLECTION", "image_audit_jobs"
)
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
THUMBNAIL_BUCKET_SUFFIX = os.environ.get(
    "THUMBNAIL_BUCKET_SUFFIX", "-thumbnails"
)

app = flask.Flask(__name__)

# --- Global Clients ---
logging_config.setup_logging()
_LOGGER = logging.getLogger(__name__)

_GCS_HANDLER: Optional[gcs_handler.GCSHandler] = None
_FIRESTORE_CLIENT: Optional[firestore.Client] = None
_BQ_WRITER: Optional[bigquery_writer.BigQueryWriter] = None
_BQ_ERROR_WRITER: Optional[bigquery_writer.BigQueryWriter] = None
_GEMINI_PROCESSORS: Dict[str, gemini_processor.GeminiProcessor] = {}
_CONFIG_MANAGER: Optional[config_manager.ConfigManager] = None


def _get_gcs_handler() -> gcs_handler.GCSHandler:
  """Lazy initializes and returns the GCS Handler."""
  global _GCS_HANDLER
  if _GCS_HANDLER is None:
    try:
      _GCS_HANDLER = gcs_handler.GCSHandler()
    except Exception:
      _LOGGER.exception("Failed to initialize GCS Handler")
      raise
  return _GCS_HANDLER


def _get_firestore_client() -> firestore.Client:
  """Lazy initializes and returns the Firestore client."""
  global _FIRESTORE_CLIENT
  if _FIRESTORE_CLIENT is None:
    try:
      _FIRESTORE_CLIENT = firestore.Client()
    except Exception:
      _LOGGER.exception("Failed to initialize Firestore client")
      raise
  return _FIRESTORE_CLIENT


def _get_bq_writer() -> Optional[bigquery_writer.BigQueryWriter]:
  """Lazy initializes and returns the BigQuery Writer."""
  global _BQ_WRITER
  if _BQ_WRITER is None:
    if all([GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE]):
      try:
        _BQ_WRITER = bigquery_writer.BigQueryWriter(
            GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE
        )
      except Exception:
        _LOGGER.exception("Failed to initialize BigQuery writer")
        raise
  return _BQ_WRITER


def _get_bq_error_writer() -> Optional[bigquery_writer.BigQueryWriter]:
  """Lazy initializes and returns the BigQuery Error Writer."""
  global _BQ_ERROR_WRITER
  if _BQ_ERROR_WRITER is None:
    if all([GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_ERRORS_TABLE]):
      try:
        _BQ_ERROR_WRITER = bigquery_writer.BigQueryWriter(
            GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_ERRORS_TABLE
        )
      except Exception:
        _LOGGER.exception("Failed to initialize BigQuery Error writer")
        raise
  return _BQ_ERROR_WRITER


def _get_config_manager() -> Optional[config_manager.ConfigManager]:
  """Lazy initializes and returns the Config Manager."""
  global _CONFIG_MANAGER
  if _CONFIG_MANAGER is None and SPREADSHEET_ID:
    _CONFIG_MANAGER = config_manager.ConfigManager(SPREADSHEET_ID)
  return _CONFIG_MANAGER


def _get_version_config(prompt_version_id: str) -> Optional[Dict[str, Any]]:
  """Retrieves the configuration for a specific version."""
  mgr = _get_config_manager()
  if mgr:
    sheets_config = mgr.get_config()
    if sheets_config and prompt_version_id in sheets_config:
      return sheets_config[prompt_version_id]
  return prompts.PROMPTS_CONFIG.get(prompt_version_id)


def _get_gemini_processor(
    prompt_version_id: str,
) -> Optional[gemini_processor.GeminiProcessor]:
  """Lazy initializes and returns the Gemini Processor for a specific version."""
  if prompt_version_id not in _GEMINI_PROCESSORS:
    config = _get_version_config(prompt_version_id)
    if config and GCP_PROJECT_ID:
      try:
        _GEMINI_PROCESSORS[prompt_version_id] = (
            gemini_processor.GeminiProcessor(
                project_id=GCP_PROJECT_ID,
                model_name=config["model_name"],
                generation_config=config["generation_config"],
            )
        )
      except (ValueError, google_api_exceptions.GoogleAPICallError) as e:
        _LOGGER.critical(
            "Failed to initialize Gemini Processor for %s: %s",
            prompt_version_id,
            e,
        )
        return None
  return _GEMINI_PROCESSORS.get(prompt_version_id)


def _update_job_status(
    doc_ref: document.DocumentReference,
    status: str,
    attempt: int,
    error_message: Optional[str] = None,
) -> None:
  """Updates the job status in Firestore."""
  update_data = {
      "status": status,
      "last_processed_at": datetime.datetime.now(datetime.timezone.utc),
      "attempts": attempt,
  }
  if error_message:
    update_data["error_message"] = error_message
  doc_ref.set(update_data, merge=True)


def _flatten_gemini_response(
    waste_audit: Dict[str, Any],
) -> Generator[Dict[str, Union[str, float]], None, None]:
  """Flattens a nested waste audit result from Gemini.

  For each material found, this generator yields a dictionary containing the
  class, material, estimated percentage, and scaled weight.

  Args:
    waste_audit: A dictionary representing the waste audit result from Gemini.

  Yields:
    A dictionary for a single waste item.
  """
  total_weight = waste_audit.get("TotalWeightLbs")
  if not isinstance(total_weight, (int, float)):
    _LOGGER.warning("TotalWeightLbs not found or invalid in Gemini response.")
    return

  class_weights = collections.defaultdict(float)

  for class_name, materials in waste_audit.items():
    if class_name == "TotalWeightLbs" or not isinstance(materials, dict):
      continue

    for material, percentage in materials.items():
      if isinstance(percentage, (int, float)):
        scaled_weight = total_weight * percentage
        class_weights[class_name] += scaled_weight
        yield {
            "class": class_name,
            "material": material,
            "estimated_percentage": percentage,
            "scaled_weight": scaled_weight,
        }

  for class_name, scaled_weight in class_weights.items():
    yield {
        "class": "Class",
        "material": class_name,
        "estimated_percentage": round(scaled_weight / total_weight, 2),
        "scaled_weight": scaled_weight,
    }


def _process_and_upload_thumbnail(
    gcs_handler_instance: gcs_handler.GCSHandler,
    raw_bucket: str,
    raw_object: str,
    image_bytes: bytes,
) -> tuple[str, str]:
  """Generates a WebP thumbnail, uploads it to GCS, and returns (thumb_gcs_uri, thumb_signed_url)."""
  try:
    thumb_bytes = image_processor.create_thumbnail(image_bytes)
    thumb_bucket = f"{raw_bucket}{THUMBNAIL_BUCKET_SUFFIX}"
    thumb_object = f"thumbnails/{os.path.splitext(raw_object)[0]}_thumb.webp"

    gcs_handler_instance.upload_bytes_to_gcs(
        thumb_bytes, thumb_bucket, thumb_object, content_type="image/webp"
    )

    thumb_gcs_uri = f"gs://{thumb_bucket}/{thumb_object}"
    thumb_signed_url = gcs_handler_instance.generate_v4_signed_url(
        bucket_name=thumb_bucket,
        object_name=thumb_object,
        expiration=datetime.timedelta(days=gcs_handler.DEFAULT_EXPIRATION_DAYS),
    )
    return thumb_gcs_uri, thumb_signed_url
  except Exception:
    _LOGGER.exception("Failed to generate/upload thumbnail")
    return "", ""


def _store_results_in_bigquery(
    prompt_version_id: str,
    gemini_result: Dict[str, Any],
    gcs_uri: str,
    image_bytes: Optional[bytes] = None,
) -> None:
  """Stores the processed results, thumbnails, and Signed URLs in BigQuery."""
  bq_writer = _get_bq_writer()
  if not bq_writer:
    _LOGGER.warning("BigQuery writer not available. Skipping storage.")
    return

  processing_timestamp = datetime.datetime.now(
      datetime.timezone.utc
  ).isoformat()

  filename = os.path.basename(gcs_uri)
  image_id = os.path.splitext(filename)[0]

  clean_uri = gcs_uri.removeprefix("gs://")
  raw_bucket, raw_object = clean_uri.split("/", 1)

  gcs_handler_inst = _get_gcs_handler()

  raw_signed_url = gcs_handler_inst.generate_v4_signed_url(
      bucket_name=raw_bucket,
      object_name=raw_object,
      expiration=datetime.timedelta(days=gcs_handler.DEFAULT_EXPIRATION_DAYS),
  )

  config = _get_version_config(prompt_version_id)
  model_name = config["model_name"] if config else "unknown"

  row_metadata = {
      "image_id": image_id,
      "model_name": model_name,
      "prompt_version_id": prompt_version_id,
      "processing_timestamp": processing_timestamp,
      "gemini_result": json.dumps(gemini_result),
      "gcs_uri": gcs_uri,
      "raw_signed_url": raw_signed_url,
  }

  if image_bytes:
    thumb_gcs_uri, thumb_signed_url = _process_and_upload_thumbnail(
        gcs_handler_inst, raw_bucket, raw_object, image_bytes
    )
    if thumb_gcs_uri:
      row_metadata["gcs_thumb_uri"] = thumb_gcs_uri
    if thumb_signed_url:
      row_metadata["thumb_signed_url"] = thumb_signed_url

  flattened_rows = list(_flatten_gemini_response(gemini_result))
  if not flattened_rows:
    _LOGGER.warning(
        "No materials to insert for %s:%s.", image_id, prompt_version_id
    )
    return

  rows_to_insert = [{**row_metadata, **material} for material in flattened_rows]
  bq_writer.insert_rows(rows_to_insert)


def _log_error_to_bigquery(
    gcs_uri: str, prompt_version_id: str, error_message: str
) -> None:
  """Logs errors to the BigQuery error table."""
  bq_error_writer = _get_bq_error_writer()
  if not bq_error_writer:
    _LOGGER.warning("BigQuery error writer is not initialized.")
    return

  try:
    filename = os.path.basename(gcs_uri)
    image_id = os.path.splitext(filename)[0]

    config = _get_version_config(prompt_version_id)
    model_name = config["model_name"] if config else "unknown"

    error_row = {
        "image_id": image_id,
        "gcs_uri": gcs_uri,
        "prompt_version_id": prompt_version_id,
        "model_name": model_name,
        "error_timestamp": (
            datetime.datetime.now(datetime.timezone.utc).isoformat()
        ),
        "error_message": error_message,
    }
    bq_error_writer.insert_row(error_row)
  except bigquery.exceptions.BigQueryError as e:
    _LOGGER.error("Failed to log error to BigQuery: %s", e)
  except KeyError as e:
    _LOGGER.error("Failed to log error to BigQuery, missing key: %s", e)


@app.route("/", methods=["POST"])
def process_image_task() -> tuple[str, int]:
  """Main endpoint to handle tasks from Cloud Tasks."""
  firestore_client = _get_firestore_client()

  data = flask.request.get_json(silent=True)
  if not data or "gcs_uri" not in data or "prompt_version_id" not in data:
    return (
        "Invalid request payload. 'gcs_uri' and 'prompt_version_id' required.",
        http.HTTPStatus.BAD_REQUEST,
    )

  gcs_uri = data["gcs_uri"]
  prompt_version_id = data["prompt_version_id"]
  retry_count = int(flask.request.headers.get("X-Cloudtasks-Taskretrycount", 0))
  attempt = retry_count + 1

  composite_key_raw = f"{gcs_uri}:{prompt_version_id}"
  composite_key = hashlib.sha256(composite_key_raw.encode()).hexdigest()

  job_doc_ref = firestore_client.collection(FIRESTORE_COLLECTION).document(
      composite_key
  )

  _LOGGER.info(
      "Processing task for %s (%s), attempt #%d",
      gcs_uri,
      prompt_version_id,
      attempt,
  )
  _update_job_status(job_doc_ref, "PROCESSING", attempt)

  prompt_config = _get_version_config(prompt_version_id)
  processor = _get_gemini_processor(prompt_version_id)

  if not prompt_config or not processor:
    msg = f"Prompt version or processor for '{prompt_version_id}' not found."
    _update_job_status(job_doc_ref, "FAILED_PERMANENT", attempt, msg)
    _log_error_to_bigquery(gcs_uri, prompt_version_id, msg)
    return msg, http.HTTPStatus.INTERNAL_SERVER_ERROR

  try:
    gcs_h = _get_gcs_handler()
    image_bytes = gcs_h.download_image_to_memory(gcs_uri)
    if not image_bytes:
      raise ValueError(f"Download failed from GCS: {gcs_uri}")

    gemini_result = processor.process_image(
        image_bytes, prompt_config["prompt_text"]
    )
    if not gemini_result:
      raise ValueError("Processing failed with Gemini API.")

    _store_results_in_bigquery(
        prompt_version_id, gemini_result, gcs_uri, image_bytes
    )
    _update_job_status(job_doc_ref, "PROCESSED", attempt)

    return "Success", http.HTTPStatus.OK

  except Exception as unknown_error:
    error_msg = str(unknown_error)
    _LOGGER.error(
        "Error processing %s on attempt %d: %s", gcs_uri, attempt, error_msg
    )
    _update_job_status(job_doc_ref, "FAILED_RETRY", attempt, error_msg)
    _log_error_to_bigquery(gcs_uri, prompt_version_id, error_msg)
    return error_msg, http.HTTPStatus.INTERNAL_SERVER_ERROR


@app.route("/refresh-urls", methods=["POST"])
def refresh_signed_urls_task() -> tuple[str, int]:
  """Endpoint triggered by Cloud Scheduler to batch refresh Signed URLs."""
  dataset_id = BIGQUERY_DATASET
  table_id = BIGQUERY_TABLE
  if not dataset_id or not table_id:
    _LOGGER.error("BIGQUERY_DATASET or BIGQUERY_TABLE not configured.")
    return (
        "Missing BIGQUERY_DATASET or BIGQUERY_TABLE configuration.",
        http.HTTPStatus.INTERNAL_SERVER_ERROR,
    )

  try:
    count = signed_url_refresher.refresh_all_active_signed_urls(
        bigquery.Client(),
        _get_gcs_handler().storage_client,
        dataset_id=dataset_id,
        table_id=table_id,
    )
    _LOGGER.info("Successfully refreshed %d Signed URLs in BigQuery.", count)
    return f"Successfully refreshed {count} Signed URLs.", http.HTTPStatus.OK
  except Exception as unknown_error:
    error_msg = f"Failed to refresh Signed URLs: {unknown_error}"
    _LOGGER.error(error_msg)
    return error_msg, http.HTTPStatus.INTERNAL_SERVER_ERROR


if __name__ == "__main__":
  app.run(
      host="0.0.0.0",
      port=int(os.environ.get("PORT", 8080)),
  )
