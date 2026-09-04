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

"""Unit tests for signed_url_refresher."""

from unittest import mock

from google.cloud import bigquery
from google.cloud import storage

from processing_service import signed_url_refresher
from utils import gcs_handler
from absl.testing import absltest
from absl.testing import parameterized

_DATASET_ID = "waste_dataset"
_TABLE_ID = "waste_records"
_DEFAULT_THUMB_BUCKET = "waste-raw-thumbnails"
_DEFAULT_THUMB_OBJ = "thumbnails/AUDIT_001_thumb.webp"


class SignedUrlRefresherTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_bq_client = mock.create_autospec(bigquery.Client, instance=True)
    self.mock_storage_client = mock.create_autospec(
        storage.Client, instance=True
    )
    self.mock_gcs_handler = mock.create_autospec(
        gcs_handler.GCSHandler, instance=True
    )

  def _assert_merge_updates_equal(self, expected_updates: list[dict[str, str]]):
    """Asserts that BigQuery MERGE was called with the expected updates."""
    self.assertEqual(self.mock_bq_client.query.call_count, 2)
    merge_call_args = self.mock_bq_client.query.call_args_list[1]
    job_config = merge_call_args.kwargs.get("job_config")
    self.assertIsNotNone(job_config)
    array_param = job_config.query_parameters[0]
    self.assertEqual(array_param.name, "updates")

    actual_updates = [
        dict(struct.struct_values) for struct in array_param.values
    ]
    self.assertEqual(actual_updates, expected_updates)

  @parameterized.named_parameters(
      (
          "full_path",
          "gs://waste-raw/photos/photo001.jpg",
          ("waste-raw", "photos/photo001.jpg"),
      ),
      ("bucket_only", "gs://waste-raw", ("waste-raw", "")),
      (
          "without_gs_prefix",
          "waste-raw/photos/photo001.jpg",
          ("waste-raw", "photos/photo001.jpg"),
      ),
      (
          "nested_path",
          "gs://my-bucket/nested/dir/image.png",
          ("my-bucket", "nested/dir/image.png"),
      ),
  )
  def test_parse_gcs_uri(self, uri: str, expected: tuple[str, str]):
    self.assertEqual(signed_url_refresher.parse_gcs_uri(uri), expected)

  def test_refresh_urls_with_existing_thumbnail_generates_signed_urls(self):
    mock_row = mock.MagicMock(
        image_id="AUDIT_001",
        gcs_uri="gs://waste-raw/photos/p1.jpg",
        gcs_thumb_uri="gs://waste-thumbs/photos/p1_thumb.webp",
    )
    self.mock_bq_client.query.return_value.result.return_value = [mock_row]
    self.mock_storage_client.bucket.return_value.blob.return_value.exists.return_value = (
        True
    )
    self.mock_gcs_handler.generate_v4_signed_url.side_effect = [
        "https://signed-raw-url",
        "https://signed-thumb-url",
    ]

    updated_count = signed_url_refresher.refresh_all_active_signed_urls(
        self.mock_bq_client,
        self.mock_storage_client,
        dataset_id=_DATASET_ID,
        table_id=_TABLE_ID,
        gcs_handler_instance=self.mock_gcs_handler,
    )

    self.assertEqual(updated_count, 1)
    self.mock_gcs_handler.download_image_to_memory.assert_not_called()
    self._assert_merge_updates_equal([{
        "image_id": "AUDIT_001",
        "raw_signed_url": "https://signed-raw-url",
        "thumb_signed_url": "https://signed-thumb-url",
        "gcs_thumb_uri": "gs://waste-thumbs/photos/p1_thumb.webp",
    }])

  @mock.patch.object(signed_url_refresher.image_processor, "create_thumbnail")
  def test_refresh_urls_backfills_missing_thumbnail(
      self, mock_create_thumbnail
  ):
    mock_row = mock.MagicMock(
        image_id="AUDIT_001",
        gcs_uri="gs://waste-raw/photos/p1.jpg",
        gcs_thumb_uri=None,
    )
    self.mock_bq_client.query.return_value.result.return_value = [mock_row]
    self.mock_storage_client.bucket.return_value.blob.return_value.exists.return_value = (
        False
    )
    self.mock_gcs_handler.download_image_to_memory.return_value = (
        b"raw_image_data"
    )
    mock_create_thumbnail.return_value = b"thumb_image_data"
    self.mock_gcs_handler.generate_v4_signed_url.side_effect = [
        "https://signed-raw",
        "https://signed-thumb",
    ]

    updated_count = signed_url_refresher.refresh_all_active_signed_urls(
        self.mock_bq_client,
        self.mock_storage_client,
        dataset_id=_DATASET_ID,
        table_id=_TABLE_ID,
        gcs_handler_instance=self.mock_gcs_handler,
    )

    self.assertEqual(updated_count, 1)
    self.mock_gcs_handler.download_image_to_memory.assert_called_once_with(
        "gs://waste-raw/photos/p1.jpg"
    )
    mock_create_thumbnail.assert_called_once_with(b"raw_image_data")
    self.mock_gcs_handler.upload_bytes_to_gcs.assert_called_once_with(
        b"thumb_image_data",
        _DEFAULT_THUMB_BUCKET,
        _DEFAULT_THUMB_OBJ,
        content_type="image/webp",
    )
    self._assert_merge_updates_equal([{
        "image_id": "AUDIT_001",
        "raw_signed_url": "https://signed-raw",
        "thumb_signed_url": "https://signed-thumb",
        "gcs_thumb_uri": f"gs://{_DEFAULT_THUMB_BUCKET}/{_DEFAULT_THUMB_OBJ}",
    }])

  def test_refresh_urls_handles_backfill_failure_gracefully(self):
    mock_row = mock.MagicMock(
        image_id="AUDIT_001",
        gcs_uri="gs://waste-raw/photos/p1.jpg",
        gcs_thumb_uri="gs://legacy-bucket/thumb.webp",
    )
    self.mock_bq_client.query.return_value.result.return_value = [mock_row]
    self.mock_storage_client.bucket.return_value.blob.return_value.exists.return_value = (
        False
    )
    self.mock_gcs_handler.download_image_to_memory.side_effect = RuntimeError(
        "GCS download failed"
    )
    self.mock_gcs_handler.generate_v4_signed_url.return_value = (
        "https://signed-raw"
    )

    updated_count = signed_url_refresher.refresh_all_active_signed_urls(
        self.mock_bq_client,
        self.mock_storage_client,
        dataset_id=_DATASET_ID,
        table_id=_TABLE_ID,
        gcs_handler_instance=self.mock_gcs_handler,
    )

    self.assertEqual(updated_count, 1)
    self._assert_merge_updates_equal([{
        "image_id": "AUDIT_001",
        "raw_signed_url": "https://signed-raw",
        "thumb_signed_url": "https://signed-raw",
        "gcs_thumb_uri": "gs://legacy-bucket/thumb.webp",
    }])

  def test_refresh_urls_uses_explicit_gcs_thumb_uri(self):
    mock_row = mock.MagicMock(
        image_id="AUDIT_001",
        gcs_uri="gs://waste-raw/photos/p1.jpg",
        gcs_thumb_uri="gs://custom-bucket/custom/thumb.webp",
    )
    self.mock_bq_client.query.return_value.result.return_value = [mock_row]
    self.mock_storage_client.bucket.return_value.blob.return_value.exists.return_value = (
        True
    )
    self.mock_gcs_handler.generate_v4_signed_url.side_effect = [
        "https://signed-raw",
        "https://signed-thumb",
    ]

    updated_count = signed_url_refresher.refresh_all_active_signed_urls(
        self.mock_bq_client,
        self.mock_storage_client,
        dataset_id=_DATASET_ID,
        table_id=_TABLE_ID,
        gcs_handler_instance=self.mock_gcs_handler,
    )

    self.assertEqual(updated_count, 1)
    self.mock_storage_client.bucket.assert_called_with("custom-bucket")
    self._assert_merge_updates_equal([{
        "image_id": "AUDIT_001",
        "raw_signed_url": "https://signed-raw",
        "thumb_signed_url": "https://signed-thumb",
        "gcs_thumb_uri": "gs://custom-bucket/custom/thumb.webp",
    }])

  def test_refresh_urls_filters_invalid_and_duplicate_records(self):
    rows = [
        mock.MagicMock(
            image_id="AUDIT_VALID",
            gcs_uri="gs://b/p1.jpg",
            gcs_thumb_uri=None,
        ),
        mock.MagicMock(
            image_id="AUDIT_VALID",
            gcs_uri="gs://b/p1.jpg",
            gcs_thumb_uri=None,
        ),
        mock.MagicMock(
            image_id=None,
            gcs_uri="gs://b/p2.jpg",
            gcs_thumb_uri=None,
        ),
        mock.MagicMock(
            image_id="AUDIT_NO_GCS",
            gcs_uri=None,
            gcs_thumb_uri=None,
        ),
    ]
    self.mock_bq_client.query.return_value.result.return_value = rows
    self.mock_storage_client.bucket.return_value.blob.return_value.exists.return_value = (
        True
    )
    self.mock_gcs_handler.generate_v4_signed_url.return_value = (
        "https://signed-url"
    )

    updated_count = signed_url_refresher.refresh_all_active_signed_urls(
        self.mock_bq_client,
        self.mock_storage_client,
        dataset_id=_DATASET_ID,
        table_id=_TABLE_ID,
        gcs_handler_instance=self.mock_gcs_handler,
    )

    self.assertEqual(updated_count, 1)
    self._assert_merge_updates_equal([{
        "image_id": "AUDIT_VALID",
        "raw_signed_url": "https://signed-url",
        "thumb_signed_url": "https://signed-url",
        "gcs_thumb_uri": "gs://b-thumbnails/thumbnails/AUDIT_VALID_thumb.webp",
    }])

  def test_refresh_urls_skips_merge_when_no_active_records(self):
    self.mock_bq_client.query.return_value.result.return_value = []

    updated_count = signed_url_refresher.refresh_all_active_signed_urls(
        self.mock_bq_client,
        self.mock_storage_client,
        dataset_id=_DATASET_ID,
        table_id=_TABLE_ID,
        gcs_handler_instance=self.mock_gcs_handler,
    )

    self.assertEqual(updated_count, 0)
    self.assertEqual(self.mock_bq_client.query.call_count, 1)


if __name__ == "__main__":
  absltest.main()
