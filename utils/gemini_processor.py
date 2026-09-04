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

"""A module for processing images with the Gemini API."""

import io
import json
import logging
from typing import Any, Dict, Optional

from google import genai
from google.genai import types as genai_types
import PIL
import tenacity


_LOGGER = logging.getLogger(__name__)


class GeminiAPIError(Exception):
  """Indicates an error occurred while interacting with the Gemini API."""
  pass


def _retry_error_callback(retry_state: tenacity.RetryCallState):
  """Logs an error after all retry attempts have failed."""
  exception = retry_state.outcome.exception() if retry_state.outcome else None
  _LOGGER.error(
      "Gemini API call failed after %d attempts: %s",
      retry_state.attempt_number,
      exception,
  )
  raise GeminiAPIError(
      "Gemini API call failed after all retries."
  ) from exception


def _extract_json(text: str) -> Optional[str]:
  """Extracts a JSON string from a larger text block."""
  # Tries to find ```json ... ```, which is the format Gemini uses when code
  # execution feature is enabled.
  json_block_start = text.find("```json")
  if json_block_start != -1:
    start = json_block_start + 7
    end = text.find("```", start)
    if end != -1:
      return text[start:end].strip()

  # Fallbacks to finding the first { and last } in the text.
  start_index = text.find("{")
  end_index = text.rfind("}") + 1
  if start_index != -1 and end_index != 0:
    return text[start_index:end_index]
  return None


class GeminiProcessor:
  """Handles interaction with the Gemini API for image analysis."""

  def __init__(
      self,
      project_id: str,
      model_name: str,
      generation_config: genai_types.GenerateContentConfig,
  ):
    """Initializes the GeminiProcessor.

    Args:
      project_id: The ID of the Google Cloud project.
      model_name: The name of the Gemini model to use.
      generation_config: The generation config to use.
    """
    self._gemini_client = genai.Client(
        vertexai=True,
        project=project_id,
        location="global",
        http_options=genai_types.HttpOptions(api_version="v1"),
    )
    self._model_name = model_name
    self._generation_config = generation_config

  @property
  def model_name(self) -> str:
    """Returns the model name used by the processor."""
    return self._model_name

  @tenacity.retry(
      stop=tenacity.stop_after_attempt(4),
      wait=tenacity.wait_random_exponential(multiplier=2, min=10, max=120),
      retry=tenacity.retry_if_exception_type(GeminiAPIError),
      retry_error_callback=_retry_error_callback,
  )
  def process_image(
      self, image_bytes: bytes, prompt: str
  ) -> Optional[Dict[str, Any]]:
    """Analyzes an image from bytes using the Gemini API.

    Args:
      image_bytes: The image bytes.
      prompt: The text prompt to send to the Gemini API.

    Returns:
      A dictionary containing the structured JSON response.

    Raises:
      GeminiAPIError: If the Gemini API call fails or returns invalid data
        after all retries.
    """
    try:
      # Identify mime type using PIL
      try:
        pil_img = PIL.Image.open(io.BytesIO(image_bytes))
        mime_type = pil_img.get_format_mimetype() or "image/jpeg"
      except (IOError, PIL.UnidentifiedImageError) as image_processing_error:
        _LOGGER.warning(
            "Failed to identify image mime type: %s. Defaulting to image/jpeg.",
            image_processing_error,
        )
        mime_type = "image/jpeg"

      image_part = genai_types.Part.from_bytes(
          data=image_bytes, mime_type=mime_type
      )

      response = self._gemini_client.models.generate_content(
          model=self._model_name,
          contents=[prompt, image_part],
          config=self._generation_config,
      )

      if not response.text:
        raise GeminiAPIError("Gemini API returned an empty response.")

      raw_text = response.text
      json_str = _extract_json(raw_text)

      if json_str:
        try:
          return json.loads(json_str)
        except json.JSONDecodeError as e:
          _LOGGER.error("Failed to parse extracted JSON: %s", json_str)
          _LOGGER.error("Raw response was: %s", raw_text)
          raise GeminiAPIError("Failed to parse JSON response.") from e

      _LOGGER.error("No valid JSON found in Gemini response.")
      _LOGGER.error("Raw response: %s", raw_text)
      raise GeminiAPIError("No valid JSON found in Gemini response.")

    except Exception as unknown_error:
      if isinstance(unknown_error, GeminiAPIError):
        raise
      _LOGGER.error(
          "An error occurred while processing image with Gemini: %s",
          unknown_error,
      )
      raise GeminiAPIError(
          "Gemini API call failed: %s" % unknown_error
      ) from unknown_error
