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

"""A module for configuring structured logging for the application."""

import logging
import sys

from google.auth import exceptions as google_auth_exceptions
from google.cloud import logging as cloud_logging


def setup_logging() -> logging.Logger:
  """Sets up logging to both console and Google Cloud Logging.

  This function configures the root logger to send logs to standard output
  and to Google Cloud Logging if the environment is configured correctly.

  Returns:
      The configured root logger.
  """
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  # Remove any existing handlers to avoid duplicate logs.
  for handler in logger.handlers[:]:
    logger.removeHandler(handler)

  # Configure console handler.
  console_handler = logging.StreamHandler(sys.stdout)
  console_handler.setLevel(logging.INFO)
  formatter = logging.Formatter(
      "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  )
  console_handler.setFormatter(formatter)
  logger.addHandler(console_handler)

  # Configure Google Cloud Logging handler.
  try:
    client = cloud_logging.Client()
    cloud_handler = client.get_default_handler()
    cloud_handler.setLevel(logging.INFO)
    logger.addHandler(cloud_handler)
    logging.info("Successfully attached Google Cloud Logging handler.")
  except google_auth_exceptions.GoogleAuthError as auth_error:
    logging.warning(
        "Could not attach Google Cloud Logging handler due to auth error: %s",
        auth_error,
    )

  return logger
