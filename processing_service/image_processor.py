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

"""Thumbnail generation and image compression module for Waste Audit."""

import io
from PIL import Image


def create_thumbnail(
    image_bytes: bytes,
    *,
    target_size: tuple[int, int] = (200, 200),
    output_format: str = "WEBP",
    quality: int = 80,
) -> bytes:
  """Resizes a raw image into a high-compression WebP thumbnail (~20KB-30KB).

  Args:
    image_bytes: Raw binary bytes of the uploaded photo.
    target_size: Target (width, height) dimensions.
    output_format: Destination format (WEBP recommended).
    quality: Quality compression setting (1-100).

  Returns:
    Compressed thumbnail image bytes.

  Raises:
    ValueError: If image_bytes is empty or invalid.
  """
  if not image_bytes:
    raise ValueError("image_bytes cannot be empty.")

  try:
    with Image.open(io.BytesIO(image_bytes)) as img:
      img.thumbnail(target_size, Image.Resampling.LANCZOS)

      output = io.BytesIO()
      img.save(output, format=output_format, quality=quality, optimize=True)
      return output.getvalue()
  except ValueError as value_error:
    raise ValueError(f"Invalid image bytes: {value_error}") from value_error
  except Exception as unknown_error:
    raise ValueError(
        f"Failed to process image: {unknown_error}"
    ) from unknown_error
