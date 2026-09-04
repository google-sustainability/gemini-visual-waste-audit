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

"""Google Cloud Storage handler for downloading images and generating Signed URLs."""

import datetime
import logging
import os
import re
from typing import Optional
import urllib.request

import google.auth
import google.auth.transport.requests
from google.cloud import exceptions as cloud_exceptions
from google.cloud import storage

_LOGGER = logging.getLogger(__name__)
DEFAULT_EXPIRATION_DAYS = 7


def _get_default_service_account_email(
    storage_client: storage.Client,
) -> Optional[str]:
  """Retrieves the default service account email for IAM signing."""
  credentials = getattr(storage_client, "_credentials", None)
  if (
      credentials
      and getattr(credentials, "service_account_email", None)
      and credentials.service_account_email != "default"
  ):
    return credentials.service_account_email

  env_sa = os.environ.get("PROCESSING_SA_EMAIL") or os.environ.get(
      "SERVICE_ACCOUNT_EMAIL"
  )
  if env_sa:
    return env_sa

  try:
    req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(req, timeout=2) as resp:
      if resp.status == 200:
        return resp.read().decode("utf-8").strip()
  except Exception:
    pass

  return None


def _get_iam_access_token(storage_client: storage.Client) -> Optional[str]:
  """Retrieves an OAuth2 access token for IAM signing in Cloud Run environments."""
  credentials = getattr(storage_client, "_credentials", None)
  if not credentials:
    return None

  try:
    if hasattr(credentials, "with_scopes"):
      credentials = credentials.with_scopes(
          ["https://www.googleapis.com/auth/cloud-platform"]
      )
    if (
        not getattr(credentials, "valid", False)
        or getattr(credentials, "token", None) is None
    ):
      credentials.refresh(google.auth.transport.requests.Request())
    return getattr(credentials, "token", None)
  except Exception as e:
    _LOGGER.warning("Could not refresh access token for IAM signing: %s", e)
    return None


class GCSHandler:
  """Handles interactions with Google Cloud Storage."""

  def __init__(self, storage_client: Optional[storage.Client] = None):
    """Initializes the GCSHandler instance."""
    if storage_client is None:
      credentials, project = google.auth.default(
          scopes=["https://www.googleapis.com/auth/cloud-platform"]
      )
      storage_client = storage.Client(credentials=credentials, project=project)
    self.storage_client = storage_client

  def download_image_to_memory(self, gcs_uri: str) -> Optional[bytes]:
    """Downloads an image from a GCS URI into memory."""
    match = re.match(r"gs://([^/]+)/(.+)", gcs_uri)
    if not match:
      _LOGGER.error("Invalid GCS URI format: %s", gcs_uri)
      return None

    bucket_name = match.group(1)
    blob_name = match.group(2)

    try:
      bucket = self.storage_client.bucket(bucket_name)
      blob = bucket.blob(blob_name)
      return blob.download_as_bytes()
    except cloud_exceptions.NotFound as e:
      _LOGGER.error("GCS object not found %s: %s", gcs_uri, e)
      return None
    except cloud_exceptions.GoogleCloudError as e:
      _LOGGER.error("Failed to download %s: %s", gcs_uri, e)
      return None

  def upload_bytes_to_gcs(
      self,
      data: bytes,
      bucket_name: str,
      object_name: str,
      content_type: str = "image/webp",
  ) -> None:
    """Uploads in-memory bytes to a GCS bucket path."""
    bucket = self.storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)

  def generate_v4_signed_url(
      self,
      *,
      bucket_name: str,
      object_name: str,
      expiration: Optional[datetime.timedelta] = None,
      service_account_email: Optional[str] = None,
  ) -> str:
    """Generates a V4 Signed URL for a private GCS object.

    Args:
      bucket_name: Name of the GCS bucket.
      object_name: Path to the object in GCS.
      expiration: Expiration duration (defaults to 7 days).
      service_account_email: Optional service account email.

    Returns:
      Signed URL string.

    Raises:
      ValueError: If bucket_name or object_name is empty.
    """
    if not bucket_name:
      raise ValueError("bucket_name cannot be empty.")
    if not object_name:
      raise ValueError("object_name cannot be empty.")

    bucket = self.storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    sa_email = service_account_email or _get_default_service_account_email(
        self.storage_client
    )

    kwargs = {
        "version": "v4",
        "expiration": (
            expiration or datetime.timedelta(days=DEFAULT_EXPIRATION_DAYS)
        ),
        "method": "GET",
    }
    if sa_email:
      kwargs["service_account_email"] = sa_email

    access_token = _get_iam_access_token(self.storage_client)
    if access_token:
      kwargs["access_token"] = access_token

    return blob.generate_signed_url(**kwargs)
