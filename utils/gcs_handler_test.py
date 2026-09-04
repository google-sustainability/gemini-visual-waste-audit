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

"""Tests for GCSHandler in utils."""

import datetime
import unittest
from unittest import mock

from google.cloud import storage

from utils import gcs_handler


class GCSHandlerTest(unittest.TestCase):

  @mock.patch.dict(
      "os.environ",
      {"PROCESSING_SA_EMAIL": "sa@project.iam.gserviceaccount.com"},
  )
  def test_generate_v4_signed_url_success(self):
    """Test generate_v4_signed_url method on GCSHandler using autospec."""
    mock_client = mock.create_autospec(storage.Client, instance=True)
    mock_bucket = mock.create_autospec(storage.Bucket, instance=True)
    mock_blob = mock.create_autospec(storage.Blob, instance=True)

    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.generate_signed_url.return_value = (
        "https://storage.googleapis.com/waste-raw/photo.jpg?token=abc"
    )

    handler = gcs_handler.GCSHandler(storage_client=mock_client)
    url = handler.generate_v4_signed_url(
        bucket_name="waste-raw",
        object_name="photo.jpg",
        expiration=datetime.timedelta(days=7),
    )

    self.assertEqual(
        url, "https://storage.googleapis.com/waste-raw/photo.jpg?token=abc"
    )
    mock_client.bucket.assert_called_once_with("waste-raw")
    mock_bucket.blob.assert_called_once_with("photo.jpg")
    mock_blob.generate_signed_url.assert_called_once_with(
        version="v4",
        expiration=datetime.timedelta(days=7),
        method="GET",
        service_account_email="sa@project.iam.gserviceaccount.com",
    )

  def test_generate_v4_signed_url_with_credentials(self):
    """Test generate_v4_signed_url with storage_client credentials requiring scoping."""
    mock_client = mock.create_autospec(storage.Client, instance=True)
    mock_bucket = mock.create_autospec(storage.Bucket, instance=True)
    mock_blob = mock.create_autospec(storage.Blob, instance=True)

    mock_credentials = mock.MagicMock()
    mock_credentials.service_account_email = (
        "sa@project.iam.gserviceaccount.com"
    )
    mock_scoped_credentials = mock.MagicMock()
    mock_credentials.with_scopes.return_value = mock_scoped_credentials
    mock_scoped_credentials.token = "mock-access-token"
    mock_scoped_credentials.valid = True
    mock_client._credentials = mock_credentials

    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.generate_signed_url.return_value = (
        "https://storage.googleapis.com/waste-raw/photo.jpg?token=abc"
    )

    handler = gcs_handler.GCSHandler(storage_client=mock_client)
    url = handler.generate_v4_signed_url(
        bucket_name="waste-raw",
        object_name="photo.jpg",
        expiration=datetime.timedelta(days=7),
    )

    self.assertEqual(
        url, "https://storage.googleapis.com/waste-raw/photo.jpg?token=abc"
    )
    mock_credentials.with_scopes.assert_called_once_with(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    mock_blob.generate_signed_url.assert_called_once_with(
        version="v4",
        expiration=datetime.timedelta(days=7),
        method="GET",
        service_account_email="sa@project.iam.gserviceaccount.com",
        access_token="mock-access-token",
    )

  def test_generate_v4_signed_url_empty_bucket_raises_value_error(self):
    """Test error handling when bucket_name is empty."""
    mock_client = mock.create_autospec(storage.Client, instance=True)
    handler = gcs_handler.GCSHandler(storage_client=mock_client)
    with self.assertRaises(ValueError):
      handler.generate_v4_signed_url(
          bucket_name="",
          object_name="photo.jpg",
      )

  def test_download_image_to_memory_success(self):
    """Test download_image_to_memory with valid GCS URI."""
    mock_client = mock.create_autospec(storage.Client, instance=True)
    mock_bucket = mock.create_autospec(storage.Bucket, instance=True)
    mock_blob = mock.create_autospec(storage.Blob, instance=True)

    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.download_as_bytes.return_value = b"fake-image-bytes"

    handler = gcs_handler.GCSHandler(storage_client=mock_client)
    data = handler.download_image_to_memory("gs://my-bucket/image.jpg")

    self.assertEqual(data, b"fake-image-bytes")
    mock_client.bucket.assert_called_once_with("my-bucket")
    mock_bucket.blob.assert_called_once_with("image.jpg")


if __name__ == "__main__":
  unittest.main()
