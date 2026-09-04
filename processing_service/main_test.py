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

import datetime
import importlib.util
import io
import os
import sys
from unittest import mock

from PIL import Image

from processing_service import image_processor
from processing_service import signed_url_refresher
from utils import gcs_handler
from absl.testing import absltest

# Mock modules that are not direct dependencies of this test target
sys.modules["flask"] = mock.MagicMock()
sys.modules["google.cloud"] = mock.MagicMock()
sys.modules["google.cloud.bigquery"] = mock.MagicMock()
sys.modules["google.cloud.firestore"] = mock.MagicMock()
sys.modules["google.cloud.firestore_v1"] = mock.MagicMock()
sys.modules["google.api_core"] = mock.MagicMock()
sys.modules["google.api_core.exceptions"] = mock.MagicMock()
sys.modules["image_processor"] = image_processor
sys.modules["signed_url_refresher"] = signed_url_refresher
sys.modules["utils.config_manager"] = mock.MagicMock()
sys.modules["utils.bigquery_writer"] = mock.MagicMock()
sys.modules["utils.logging_config"] = mock.MagicMock()
sys.modules["utils.gemini_processor"] = mock.MagicMock()
sys.modules["utils.prompts"] = mock.MagicMock()

_MAIN_PATH = os.path.join(os.path.dirname(__file__), "main.py")
_SPEC = importlib.util.spec_from_file_location("main_module", _MAIN_PATH)
main = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(main)
_SIGNED_URL = "https://storage.googleapis.com/test-bucket-thumbnails/thumbnails/test_thumb.webp?signed=true"


class MainTest(absltest.TestCase):

  def test_process_and_upload_thumbnail_success(self):
    """Tests _process_and_upload_thumbnail generates thumbnail and V4 signed URL."""
    raw_img = Image.new("RGB", (200, 200), color="blue")
    img_byte_arr = io.BytesIO()
    raw_img.save(img_byte_arr, format="JPEG")
    raw_bytes = img_byte_arr.getvalue()

    mock_gcs_handler = mock.MagicMock()
    mock_gcs_handler.generate_v4_signed_url.return_value = _SIGNED_URL

    thumb_uri, signed_url = main._process_and_upload_thumbnail(
        gcs_handler_instance=mock_gcs_handler,
        raw_bucket="test-bucket",
        raw_object="test.jpg",
        image_bytes=raw_bytes,
    )

    self.assertEqual(
        thumb_uri,
        "gs://test-bucket-thumbnails/thumbnails/test_thumb.webp",
    )
    self.assertEqual(
        signed_url,
        _SIGNED_URL,
    )
    mock_gcs_handler.upload_bytes_to_gcs.assert_called_once_with(
        mock.ANY,
        "test-bucket-thumbnails",
        "thumbnails/test_thumb.webp",
        content_type="image/webp",
    )
    mock_gcs_handler.generate_v4_signed_url.assert_called_once_with(
        bucket_name="test-bucket-thumbnails",
        object_name="thumbnails/test_thumb.webp",
        expiration=datetime.timedelta(days=gcs_handler.DEFAULT_EXPIRATION_DAYS),
    )

  def test_process_and_upload_thumbnail_graceful_recovery_on_error(self):
    """Tests _process_and_upload_thumbnail recovers gracefully on exception."""
    mock_gcs_handler = mock.MagicMock()
    mock_gcs_handler.upload_bytes_to_gcs.side_effect = RuntimeError(
        "GCS Upload Exception"
    )

    thumb_uri, signed_url = main._process_and_upload_thumbnail(
        gcs_handler_instance=mock_gcs_handler,
        raw_bucket="test-bucket",
        raw_object="test.jpg",
        image_bytes=b"fake_image_bytes",
    )

    self.assertEqual(thumb_uri, "")
    self.assertEqual(signed_url, "")

  @mock.patch.object(main, "_get_gcs_handler")
  @mock.patch.object(main, "_get_bq_writer")
  def test_store_results_in_bigquery_signed_url_generation(
      self, mock_get_bq_writer, mock_get_gcs_handler
  ):
    """Tests raw image and thumbnail V4 signed URLs use DEFAULT_EXPIRATION_DAYS."""
    mock_gcs_handler = mock.MagicMock()
    mock_get_gcs_handler.return_value = mock_gcs_handler
    mock_gcs_handler.generate_v4_signed_url.return_value = _SIGNED_URL

    mock_bq_writer = mock.MagicMock()
    mock_get_bq_writer.return_value = mock_bq_writer

    main._store_results_in_bigquery(
        prompt_version_id="v1",
        gemini_result={"TotalWeightLbs": 10.0, "Cardboard": {"Boxes": 1.0}},
        gcs_uri="gs://my-bucket/photo.jpg",
        image_bytes=None,
    )

    mock_gcs_handler.generate_v4_signed_url.assert_called_once_with(
        bucket_name="my-bucket",
        object_name="photo.jpg",
        expiration=datetime.timedelta(days=gcs_handler.DEFAULT_EXPIRATION_DAYS),
    )
    mock_bq_writer.insert_rows.assert_called_once()
    inserted_rows = mock_bq_writer.insert_rows.call_args[0][0]
    self.assertNotIn("gcs_thumb_uri", inserted_rows[0])
    self.assertNotIn("thumb_signed_url", inserted_rows[0])

  @mock.patch.object(main, "_process_and_upload_thumbnail")
  @mock.patch.object(main, "_get_gcs_handler")
  @mock.patch.object(main, "_get_bq_writer")
  def test_store_results_in_bigquery_with_image_bytes(
      self, mock_get_bq_writer, mock_get_gcs_handler, mock_process_thumb
  ):
    """Tests thumbnail URL is stored when image_bytes is provided."""
    mock_gcs_handler = mock.MagicMock()
    mock_get_gcs_handler.return_value = mock_gcs_handler
    mock_gcs_handler.generate_v4_signed_url.return_value = _SIGNED_URL

    mock_process_thumb.return_value = (
        "gs://my-bucket-thumbnails/thumbnails/photo_thumb.webp",
        _SIGNED_URL,
    )

    mock_bq_writer = mock.MagicMock()
    mock_get_bq_writer.return_value = mock_bq_writer

    main._store_results_in_bigquery(
        prompt_version_id="v1",
        gemini_result={"TotalWeightLbs": 10.0, "Cardboard": {"Boxes": 1.0}},
        gcs_uri="gs://my-bucket/photo.jpg",
        image_bytes=b"image_content",
    )

    mock_process_thumb.assert_called_once_with(
        mock_gcs_handler, "my-bucket", "photo.jpg", b"image_content"
    )
    mock_bq_writer.insert_rows.assert_called_once()
    inserted_rows = mock_bq_writer.insert_rows.call_args[0][0]
    self.assertEqual(
        inserted_rows[0]["gcs_thumb_uri"],
        "gs://my-bucket-thumbnails/thumbnails/photo_thumb.webp",
    )
    self.assertEqual(inserted_rows[0]["thumb_signed_url"], _SIGNED_URL)


if __name__ == "__main__":
  absltest.main()
