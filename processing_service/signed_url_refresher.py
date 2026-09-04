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

"""Batch refresher module for BigQuery Signed URLs (Every 5 Days)."""

import dataclasses
import datetime
import logging
import os
import re
import sys
from typing import Any, List, Optional, Tuple

from google.cloud import bigquery
from google.cloud import storage



# Add current directory and parent directory to sys.path for open-source
# import compatibility
_CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, _CURRENT_DIR)
sys.path.insert(0, os.path.abspath(os.path.join(_CURRENT_DIR, "..")))

import image_processor

from utils import gcs_handler



_LOGGER = logging.getLogger(__name__)

_DEFAULT_EXPIRATION = datetime.timedelta(days=7)
_THUMBNAIL_BUCKET_SUFFIX = "-thumbnails"
_THUMBNAIL_PREFIX = "thumbnails"

_SELECT_ACTIVE_RECORDS_QUERY = """
SELECT image_id, gcs_uri, MAX(gcs_thumb_uri) AS gcs_thumb_uri
FROM `{dataset_id}.{table_id}`
WHERE gcs_uri IS NOT NULL AND image_id IS NOT NULL
GROUP BY image_id, gcs_uri
"""

_MERGE_SIGNED_URLS_QUERY = """
MERGE `{dataset_id}.{table_id}` T
USING UNNEST(@updates) S
ON T.image_id = S.image_id
WHEN MATCHED THEN
  UPDATE SET
    raw_signed_url = S.raw_signed_url,
    thumb_signed_url = S.thumb_signed_url,
    gcs_thumb_uri = S.gcs_thumb_uri
"""


@dataclasses.dataclass(frozen=True)
class ThumbnailStatus:
  """Status and location of a GCS thumbnail."""

  bucket: str
  object_name: str
  exists: bool

  @property
  def gcs_uri(self) -> str:
    return f"gs://{self.bucket}/{self.object_name}"


@dataclasses.dataclass(frozen=True)
class SignedUrlUpdate:
  """Refreshed Signed URLs payload for a BigQuery record."""

  image_id: str
  raw_signed_url: str
  thumb_signed_url: str
  gcs_thumb_uri: str


def parse_gcs_uri(gcs_uri: str) -> Tuple[str, str]:
  """Parses 'gs://bucket/object/path' into (bucket, object_path)."""
  match = re.fullmatch(r"gs://([^/]+)/(.*)", gcs_uri)
  if match:
    return match.group(1), match.group(2)
  clean_uri = gcs_uri.removeprefix("gs://")
  parts = clean_uri.split("/", 1)
  return parts[0], (parts[1] if len(parts) > 1 else "")


def _determine_thumbnail_location(
    raw_bucket: str,
    raw_obj: str,
    image_id: str,
    gcs_thumb_uri: Optional[str],
) -> Tuple[str, str]:
  """Determines target GCS bucket and object path for a thumbnail."""
  del raw_obj  # Unused, preserved for interface consistency.
  if gcs_thumb_uri:
    thumb_bucket, thumb_obj = parse_gcs_uri(gcs_thumb_uri)
    if thumb_bucket and thumb_obj:
      return thumb_bucket, thumb_obj
  return (
      f"{raw_bucket}{_THUMBNAIL_BUCKET_SUFFIX}",
      f"{_THUMBNAIL_PREFIX}/{image_id}_thumb.webp",
  )


def _backfill_thumbnail(
    gcs_uri: str,
    thumb_bucket: str,
    thumb_obj: str,
    image_id: str,
    gcs_handler_instance: gcs_handler.GCSHandler,
) -> bool:
  """Backfills thumbnail from raw image, returning True on success."""
  try:
    raw_bytes = gcs_handler_instance.download_image_to_memory(gcs_uri)
    if not raw_bytes:
      return False
    thumb_bytes = image_processor.create_thumbnail(raw_bytes)
    gcs_handler_instance.upload_bytes_to_gcs(
        thumb_bytes, thumb_bucket, thumb_obj, content_type="image/webp"
    )
    _LOGGER.info(
        "Successfully backfilled thumbnail for %s at gs://%s/%s",
        image_id,
        thumb_bucket,
        thumb_obj,
    )
    return True
  except Exception as e:
    _LOGGER.warning(
        "Failed to backfill thumbnail for %s (%s): %s",
        image_id,
        gcs_uri,
        e,
    )
    return False


def _ensure_thumbnail_exists(
    gcs_uri: str,
    raw_bucket: str,
    raw_obj: str,
    image_id: str,
    gcs_thumb_uri: Optional[str],
    storage_client: storage.Client,
    gcs_handler_instance: gcs_handler.GCSHandler,
) -> ThumbnailStatus:
  """Verifies thumbnail exists or backfills it from the raw image."""
  thumb_bucket, thumb_obj = _determine_thumbnail_location(
      raw_bucket, raw_obj, image_id, gcs_thumb_uri
  )
  try:
    if storage_client.bucket(thumb_bucket).blob(thumb_obj).exists():
      return ThumbnailStatus(
          bucket=thumb_bucket,
          object_name=thumb_obj,
          exists=True,
      )
  except Exception as e:
    _LOGGER.warning(
        "Failed to check thumbnail existence for gs://%s/%s: %s",
        thumb_bucket,
        thumb_obj,
        e,
    )
  success_to_backfill = _backfill_thumbnail(
      gcs_uri, thumb_bucket, thumb_obj, image_id, gcs_handler_instance
  )
  return ThumbnailStatus(
      bucket=thumb_bucket,
      object_name=thumb_obj,
      exists=success_to_backfill,
  )


def _build_signed_url_payload(
    gcs_handler_instance: gcs_handler.GCSHandler,
    *,
    image_id: str,
    raw_bucket: str,
    raw_obj: str,
    thumb_status: ThumbnailStatus,
    fallback_thumb_uri: Optional[str],
) -> SignedUrlUpdate:
  """Builds SignedUrlUpdate dataclass with generated Signed URLs."""
  new_raw_url = gcs_handler_instance.generate_v4_signed_url(
      bucket_name=raw_bucket,
      object_name=raw_obj,
      expiration=_DEFAULT_EXPIRATION,
  )
  new_thumb_url = new_raw_url
  if thumb_status.exists:
    new_thumb_url = gcs_handler_instance.generate_v4_signed_url(
        bucket_name=thumb_status.bucket,
        object_name=thumb_status.object_name,
        expiration=_DEFAULT_EXPIRATION,
    )
  final_thumb_uri = (
      thumb_status.gcs_uri
      if thumb_status.exists
      else (fallback_thumb_uri or "")
  )
  return SignedUrlUpdate(
      image_id=str(image_id),
      raw_signed_url=str(new_raw_url),
      thumb_signed_url=str(new_thumb_url),
      gcs_thumb_uri=str(final_thumb_uri),
  )


def _process_single_row_signed_urls(
    row: Any,
    storage_client: storage.Client,
    gcs_handler_instance: gcs_handler.GCSHandler,
) -> Optional[SignedUrlUpdate]:
  """Processes GCS checks, backfills thumbnail, and generates Signed URLs."""
  image_id = getattr(row, "image_id", None)
  gcs_uri = getattr(row, "gcs_uri", None)
  if not gcs_uri or not image_id:
    return None

  raw_bucket, raw_obj = parse_gcs_uri(gcs_uri)
  if not raw_bucket or not raw_obj:
    return None

  gcs_thumb_uri = getattr(row, "gcs_thumb_uri", None)
  thumb_status = _ensure_thumbnail_exists(
      gcs_uri,
      raw_bucket,
      raw_obj,
      image_id,
      gcs_thumb_uri,
      storage_client,
      gcs_handler_instance,
  )

  return _build_signed_url_payload(
      gcs_handler_instance=gcs_handler_instance,
      image_id=str(image_id),
      raw_bucket=raw_bucket,
      raw_obj=raw_obj,
      thumb_status=thumb_status,
      fallback_thumb_uri=gcs_thumb_uri,
  )


def _batch_merge_updates(
    bq_client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    updates: List[SignedUrlUpdate],
) -> None:
  """Executes BigQuery MERGE statement for batch updating Signed URLs."""
  merge_query = _MERGE_SIGNED_URLS_QUERY.format(
      dataset_id=dataset_id, table_id=table_id
  )
  bq_client.query(
      merge_query,
      job_config=bigquery.QueryJobConfig(
          query_parameters=[
              bigquery.ArrayQueryParameter(
                  "updates",
                  bigquery.enums.SqlTypeNames.STRUCT,
                  [
                      bigquery.StructQueryParameter(
                          "update",
                          bigquery.ScalarQueryParameter(
                              "image_id", "STRING", update.image_id
                          ),
                          bigquery.ScalarQueryParameter(
                              "raw_signed_url", "STRING", update.raw_signed_url
                          ),
                          bigquery.ScalarQueryParameter(
                              "thumb_signed_url",
                              "STRING",
                              update.thumb_signed_url,
                          ),
                          bigquery.ScalarQueryParameter(
                              "gcs_thumb_uri", "STRING", update.gcs_thumb_uri
                          ),
                      )
                      for update in updates
                  ],
              )
          ]
      ),
  ).result()


def refresh_all_active_signed_urls(
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    *,
    dataset_id: str,
    table_id: str,
    gcs_handler_instance: Optional[gcs_handler.GCSHandler] = None,
) -> int:
  """Batch updates and backfills Signed URLs in BigQuery with fresh 7-day expiration."""
  query = _SELECT_ACTIVE_RECORDS_QUERY.format(
      dataset_id=dataset_id, table_id=table_id
  )
  rows = bq_client.query(query).result()
  updates: List[SignedUrlUpdate] = []
  seen_image_ids = set()
  gcs_handler_inst = gcs_handler_instance or gcs_handler.GCSHandler(
      storage_client=storage_client
  )

  for row in rows:
    image_id = getattr(row, "image_id", None)
    if not image_id or image_id in seen_image_ids:
      continue
    seen_image_ids.add(image_id)

    update_payload = _process_single_row_signed_urls(
        row,
        storage_client,
        gcs_handler_inst,
    )
    if update_payload:
      updates.append(update_payload)

  if not updates:
    return 0

  _batch_merge_updates(bq_client, dataset_id, table_id, updates)
  return len(updates)
