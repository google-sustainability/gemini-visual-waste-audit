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

"""A module for writing data to Google BigQuery."""

import logging
from typing import Any, Dict, List

from google.cloud import bigquery

_LOGGER = logging.getLogger(__name__)


class BigQueryWriter:
  """Handles streaming data to a BigQuery table."""

  def __init__(self, project_id: str, dataset_name: str, table_name: str):
    """Initializes the BigQueryWriter.

    Args:
      project_id: The GCP project ID.
      dataset_name: The BigQuery dataset name.
      table_name: The BigQuery table name.
    """
    self.client = bigquery.Client(project=project_id)
    self.table_id = f"{project_id}.{dataset_name}.{table_name}"

  def insert_rows(self, rows: List[Dict[str, Any]]) -> None:
    """Streams rows into BigQuery.

    Args:
      rows: The rows to insert, as a list of dictionaries.

    Raises:
      ValueError: If the BigQuery API returns errors for the insert operation.
    """
    if not rows:
      _LOGGER.info("No rows to insert.")
      return

    errors = self.client.insert_rows_json(self.table_id, rows)
    if errors:
      _LOGGER.error("Encountered errors while inserting rows: %s", errors)
      raise ValueError(f"BigQuery insert failed: {errors}")

    _LOGGER.info("Successfully inserted %d rows.", len(rows))

  def insert_row(self, row: Dict[str, Any]) -> None:
    """Streams a single row into BigQuery.

    Args:
      row: The row to insert, as a dictionary.

    Raises:
      ValueError: If the BigQuery API returns errors for the insert operation.
    """
    self.insert_rows([row])
    _LOGGER.info(
        "Successfully inserted row for image_id: %s", row.get("image_id")
    )
