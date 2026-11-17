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

"""BigQuery DML helpers for deleting superseded or removed audit rows."""

import dataclasses
import logging
import re
from typing import NamedTuple

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery
import tenacity

_LOGGER = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = "[A-Za-z0-9_-]+"
_INVALID_TABLE_ID_MSG = "Invalid BigQuery table_id: {!r}"


@dataclasses.dataclass(frozen=True)
class _IdentifierRule:
  """Validation pattern and error message template for a table component."""

  pattern: re.Pattern[str]
  error_message: str


class _TableIdRules(NamedTuple):
  """Validation rules per component, in `project.dataset.table` order."""

  project_id: _IdentifierRule
  dataset_id: _IdentifierRule
  table_id: _IdentifierRule


_TABLE_ID_RULES = _TableIdRules(
    project_id=_IdentifierRule(
        pattern=re.compile(
            rf"(?:[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*:)?{_IDENTIFIER_PATTERN}"
        ),
        error_message="Invalid GCP project_id: {!r}",
    ),
    dataset_id=_IdentifierRule(
        pattern=re.compile("[A-Za-z0-9_]+"),
        error_message="Invalid BigQuery dataset_id: {!r}",
    ),
    table_id=_IdentifierRule(
        pattern=re.compile(_IDENTIFIER_PATTERN),
        error_message=_INVALID_TABLE_ID_MSG,
    ),
)

_DELETE_ROWS_QUERY_TEMPLATE = (
    "DELETE FROM {table_ref}\nWHERE\n  {where_clause};"
)
_MAX_DML_ATTEMPTS = 4
_INITIAL_DML_BACKOFF_SECONDS = 0.5
_MAX_DML_BACKOFF_SECONDS = 2.0


def _validate_table_id(table_id: str) -> None:
  """Raises `ValueError` unless `table_id` is a valid `project.dataset.table`."""
  # Splits from the right because domain-scoped project IDs contain dots.
  parts = table_id.rsplit(".", 2)
  if len(parts) != len(_TABLE_ID_RULES) or not all(
      rule.pattern.fullmatch(part)
      for rule, part in zip(_TABLE_ID_RULES, parts, strict=True)
  ):
    raise ValueError(_INVALID_TABLE_ID_MSG.format(table_id))


def _is_retryable_dml_error(error: BaseException) -> bool:
  """Returns whether `error` is a retryable concurrent DML update conflict."""
  if not isinstance(error, google_exceptions.BadRequest):
    return False
  message = str(error).lower()
  return (
      "serialize access to table" in message or "concurrent update" in message
  )


def _append_prompt_version(
    predicates: list[str],
    query_parameters: list[bigquery.ScalarQueryParameter],
    prompt_version_id: str | None,
) -> None:
  """Appends the optional `prompt_version_id` predicate and query parameter."""
  if prompt_version_id is None:
    return
  predicates.append("prompt_version_id = @prompt_version_id")
  query_parameters.append(
      bigquery.ScalarQueryParameter(
          "prompt_version_id", "STRING", prompt_version_id
      )
  )


@tenacity.retry(
    stop=tenacity.stop_after_attempt(_MAX_DML_ATTEMPTS),
    wait=tenacity.wait_random_exponential(
        multiplier=_INITIAL_DML_BACKOFF_SECONDS,
        max=_MAX_DML_BACKOFF_SECONDS,
    ),
    retry=tenacity.retry_if_exception(_is_retryable_dml_error),
    before_sleep=tenacity.before_sleep_log(_LOGGER, logging.WARNING),
    reraise=True,
)
def _execute_delete_query(
    client: bigquery.Client,
    query: str,
    job_config: bigquery.QueryJobConfig,
) -> int:
  """Executes a `DELETE` query job and retries transient DML conflicts."""
  query_job = client.query(query, job_config=job_config)
  query_job.result()
  return query_job.num_dml_affected_rows or 0


def resolve_table_id(
    *,
    project_id: str | None,
    dataset_id: str | None,
    table_id: str | None,
) -> str | None:
  """Resolves a validated `project.dataset.table` identifier from settings.

  Args:
    project_id: Configured GCP project ID, or `None` if unset.
    dataset_id: Configured BigQuery dataset ID, or `None` if unset.
    table_id: Configured BigQuery table ID, or `None` if unset.

  Returns:
    The validated `project.dataset.table` string, or `None` (after logging a
    warning) if any of `project_id`, `dataset_id`, or `table_id` is `None` or
    blank.

  Raises:
    ValueError: If a non-blank identifier component contains unsupported
      characters.
  """
  raw_components = (project_id, dataset_id, table_id)
  cleaned_components = [(value or "").strip() for value in raw_components]
  if not all(cleaned_components):
    _LOGGER.warning(
        "BigQuery table configuration incomplete; skipping BigQuery DML."
    )
    return None
  for rule, raw, cleaned in zip(
      _TABLE_ID_RULES, raw_components, cleaned_components, strict=True
  ):
    if not rule.pattern.fullmatch(cleaned):
      raise ValueError(rule.error_message.format(raw))
  return ".".join(cleaned_components)


def delete_rows(
    client: bigquery.Client,
    *,
    table_id: str,
    gcs_uri: str,
    prompt_version_id: str | None = None,
) -> int:
  """Deletes BigQuery rows matching `gcs_uri` (and optional prompt version).

  Retries transient BigQuery concurrent DML serialization conflicts (`HTTP 400`
  `Could not serialize access to table ... due to concurrent update`) by
  submitting a fresh query job with jittered exponential backoff.

  Args:
    client: BigQuery client used to execute the `DELETE` statement.
    table_id: Fully qualified `project.dataset.table` identifier.
    gcs_uri: Target `gs://` URI whose rows should be removed.
    prompt_version_id: Optional prompt version filter. If `None`, rows across
      all prompt versions for `gcs_uri` are deleted.

  Returns:
    Number of BigQuery rows deleted.

  Raises:
    ValueError: If `table_id` is not a valid `project.dataset.table` string.
    google_exceptions.GoogleAPICallError: If the BigQuery DML job fails.
  """
  _validate_table_id(table_id)
  predicates = ["gcs_uri = @gcs_uri"]
  query_parameters = [
      bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
  ]
  _append_prompt_version(predicates, query_parameters, prompt_version_id)

  query = _DELETE_ROWS_QUERY_TEMPLATE.format(
      table_ref=f"`{table_id}`",
      where_clause="\n  AND ".join(predicates),
  )
  job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
  return _execute_delete_query(client, query, job_config)
