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

"""Tests for image_processor in processing_service."""

import io
from unittest import mock

from PIL import Image

from processing_service import image_processor
from absl.testing import absltest


class ImageProcessorTest(absltest.TestCase):

  def test_create_thumbnail_success_default_params(self):
    """Tests resizing a raw image to a 200x200 WebP thumbnail (<50KB)."""
    raw_img = Image.new("RGB", (2000, 2000), color="red")
    img_byte_arr = io.BytesIO()
    raw_img.save(img_byte_arr, format="JPEG")
    raw_bytes = img_byte_arr.getvalue()

    thumb_bytes = image_processor.create_thumbnail(raw_bytes)

    thumb_img = Image.open(io.BytesIO(thumb_bytes))
    self.assertEqual(thumb_img.size, (200, 200))
    self.assertEqual(thumb_img.format, "WEBP")
    self.assertLess(len(thumb_bytes), 50 * 1024)

  def test_create_thumbnail_preserves_aspect_ratio(self):
    """Tests resizing a non-square image preserves aspect ratio."""
    raw_img = Image.new("RGB", (2000, 1000), color="blue")
    img_byte_arr = io.BytesIO()
    raw_img.save(img_byte_arr, format="JPEG")
    raw_bytes = img_byte_arr.getvalue()

    thumb_bytes = image_processor.create_thumbnail(
        raw_bytes, target_size=(200, 200)
    )

    thumb_img = Image.open(io.BytesIO(thumb_bytes))
    self.assertEqual(thumb_img.size, (200, 100))

  def test_create_thumbnail_custom_format_and_quality(self):
    """Tests resizing image with custom format (JPEG) and quality."""
    raw_img = Image.new("RGB", (500, 500), color="green")
    img_byte_arr = io.BytesIO()
    raw_img.save(img_byte_arr, format="PNG")
    raw_bytes = img_byte_arr.getvalue()

    thumb_bytes = image_processor.create_thumbnail(
        raw_bytes, target_size=(100, 100), output_format="JPEG", quality=50
    )

    thumb_img = Image.open(io.BytesIO(thumb_bytes))
    self.assertEqual(thumb_img.size, (100, 100))
    self.assertEqual(thumb_img.format, "JPEG")

  def test_create_thumbnail_empty_bytes_raises_value_error(self):
    """Tests empty image bytes raises ValueError with explicit message."""
    with self.assertRaisesRegex(ValueError, r"^image_bytes cannot be empty\.$"):
      image_processor.create_thumbnail(b"")

  def test_create_thumbnail_invalid_bytes_raises_value_error(self):
    """Tests invalid image bytes raises ValueError with expected prefix."""
    with self.assertRaisesRegex(
        ValueError, r"^Failed to process image: cannot identify image file"
    ):
      image_processor.create_thumbnail(b"invalid_image_bytes")

  def test_create_thumbnail_pil_value_error_raises_value_error(self):
    """Tests ValueError from PIL is wrapped with expected prefix."""
    raw_img = Image.new("RGB", (10, 10), color="red")
    img_byte_arr = io.BytesIO()
    raw_img.save(img_byte_arr, format="JPEG")
    raw_bytes = img_byte_arr.getvalue()

    self.enter_context(
        mock.patch.object(
            Image, "open", side_effect=ValueError("Invalid PIL mode")
        )
    )
    with self.assertRaisesRegex(
        ValueError, r"^Invalid image bytes: Invalid PIL mode$"
    ):
      image_processor.create_thumbnail(raw_bytes)

  def test_create_thumbnail_generic_exception_raises_value_error(self):
    """Tests generic Exception from PIL is wrapped with expected prefix."""
    raw_img = Image.new("RGB", (10, 10), color="red")
    img_byte_arr = io.BytesIO()
    raw_img.save(img_byte_arr, format="JPEG")
    raw_bytes = img_byte_arr.getvalue()

    self.enter_context(
        mock.patch.object(
            Image,
            "open",
            side_effect=RuntimeError("Unexpected processing failure"),
        )
    )
    with self.assertRaisesRegex(
        ValueError,
        r"^Failed to process image: Unexpected processing failure$",
    ):
      image_processor.create_thumbnail(raw_bytes)


if __name__ == "__main__":
  absltest.main()
