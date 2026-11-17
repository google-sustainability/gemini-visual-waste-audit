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

"""A module for writing data to Google BigQuery via the Storage Write API."""

from collections.abc import Callable, Iterator, Mapping, Sequence
import dataclasses
import datetime
import json
import logging
import math
import types
from typing import Any

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types as bqs_types
from google.protobuf import descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory

_LOGGER = logging.getLogger(__name__)
_MAX_REQUEST_BYTES = 9 * 1024 * 1024
_FIELD_DESC = descriptor.FieldDescriptor
_BQ_TO_PROTO: Mapping[str, int] = types.MappingProxyType({
    "BOOL": _FIELD_DESC.TYPE_BOOL,
    "BOOLEAN": _FIELD_DESC.TYPE_BOOL,
    "BYTES": _FIELD_DESC.TYPE_BYTES,
    "FLOAT": _FIELD_DESC.TYPE_DOUBLE,
    "FLOAT64": _FIELD_DESC.TYPE_DOUBLE,
    "INT64": _FIELD_DESC.TYPE_INT64,
    "INTEGER": _FIELD_DESC.TYPE_INT64,
    "TIMESTAMP": _FIELD_DESC.TYPE_INT64,
})


class BigQueryWriteError(bigquery.exceptions.BigQueryError, ValueError):
  """An error occurred while encoding or appending rows to BigQuery."""


@dataclasses.dataclass(frozen=True, kw_only=True)
class _RowCodec:
  """Dynamic ProtoSchema and generated message class for encoding rows.

  Attributes:
    proto_schema: Dynamic `ProtoSchema` describing the destination table rows.
    row_cls: Generated protobuf message class matching `proto_schema`.
  """

  proto_schema: bqs_types.ProtoSchema
  row_cls: type[Any]


@dataclasses.dataclass(frozen=True, kw_only=True)
class _AppendBatch:
  """Single AppendRowsRequest chunk paired with its starting row offset.

  Attributes:
    row_offset: Zero-based index of the batch's first row in the caller input.
    request: `AppendRowsRequest` payload containing the serialized chunk.
  """

  row_offset: int
  request: bqs_types.AppendRowsRequest


def _to_utc_micros(dt: datetime.datetime) -> int:
  """Converts a datetime (treating naive values as UTC) to microseconds."""
  aware = dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)
  return round(aware.timestamp() * 1_000_000)


def _parse_iso_timestamp_micros(value: str) -> int:
  """Parses an ISO-8601 timestamp string into UTC microseconds."""
  iso = value.strip()
  if iso.endswith("Z"):
    iso = f"{iso[:-1]}+00:00"
  elif iso.endswith(" UTC"):
    iso = f"{iso[:-4]}+00:00"
  return _to_utc_micros(datetime.datetime.fromisoformat(iso))


def _epoch_to_utc_micros(value: int | float) -> int:
  """Converts an epoch value in seconds, millis, or micros to UTC micros."""
  epoch = float(value)
  if not math.isfinite(epoch):
    raise ValueError(f"Non-finite timestamp epoch: {value!r}")
  if abs(epoch) >= 1e14:
    scale = 1
  elif abs(epoch) >= 1e11:
    scale = 1_000
  else:
    scale = 1_000_000
  return round(epoch * scale)


def _convert_timestamp(value: Any) -> int:
  """Converts an ISO string, datetime, or epoch number to UTC microseconds."""
  if isinstance(value, str):
    return _parse_iso_timestamp_micros(value)
  if isinstance(value, datetime.datetime):
    return _to_utc_micros(value)
  if isinstance(value, (int, float)) and not isinstance(value, bool):
    return _epoch_to_utc_micros(value)
  raise TypeError(f"Unsupported timestamp value: {value!r}")


def _convert_bool(value: Any) -> bool:
  """Converts a bool, 0/1 int, or boolean string into a bool."""
  if isinstance(value, bool) or (isinstance(value, int) and value in (0, 1)):
    return bool(value)
  if isinstance(value, str):
    normalized = value.strip().lower()
    if normalized in ("true", "1", "false", "0"):
      return normalized in ("true", "1")
  raise ValueError(f"Invalid boolean value: {value!r}")


def _convert_int64(value: Any) -> int:
  """Converts a non-boolean numeric or string value into a 64-bit integer."""
  if isinstance(value, bool) or (
      isinstance(value, float) and not math.isfinite(value)
  ):
    raise ValueError(f"Invalid integer value: {value!r}")
  return int(value)


def _convert_float64(value: Any) -> float:
  """Converts a non-boolean finite numeric or string value into a float."""
  if isinstance(value, bool):
    raise ValueError(f"Invalid float value: {value!r}")
  number = float(value)
  if not math.isfinite(number):
    raise ValueError(f"Invalid float value: {value!r}")
  return number


def _convert_bytes(value: Any) -> bytes:
  """Converts bytes or bytearray into raw bytes."""
  if isinstance(value, (bytes, bytearray)):
    return bytes(value)
  raise TypeError(f"Invalid bytes value: {value!r}")


def _convert_string_or_json(value: Any) -> str:
  """Serializes dicts/lists to JSON and converts other values to strings."""
  if isinstance(value, (dict, list)):
    return json.dumps(value, allow_nan=False)
  return str(value)


_VALUE_CONVERTERS: Mapping[str, Callable[[Any], Any]] = types.MappingProxyType({
    "BOOL": _convert_bool,
    "BOOLEAN": _convert_bool,
    "BYTES": _convert_bytes,
    "FLOAT": _convert_float64,
    "FLOAT64": _convert_float64,
    "INT64": _convert_int64,
    "INTEGER": _convert_int64,
    "TIMESTAMP": _convert_timestamp,
})


def _convert_value(bq_type: str, value: Any) -> Any:
  """Converts a Python value into its protobuf wire representation."""
  converter = _VALUE_CONVERTERS.get(bq_type, _convert_string_or_json)
  try:
    return converter(value)
  except (ValueError, TypeError, OverflowError) as err:
    raise BigQueryWriteError(f"Invalid {bq_type} value: {value!r}") from err


def _chunk_by_size(
    serialized_rows: Sequence[bytes], max_batch_bytes: int
) -> Iterator[list[bytes]]:
  """Splits serialized protobuf rows into batches bounded by byte size."""
  batch: list[bytes] = []
  batch_bytes = 0
  for row_bytes in serialized_rows:
    row_size = len(row_bytes)
    if row_size > max_batch_bytes:
      raise BigQueryWriteError(
          f"Serialized row ({row_size} bytes) exceeds limit ({max_batch_bytes})"
      )
    if batch and batch_bytes + row_size > max_batch_bytes:
      yield batch
      batch = []
      batch_bytes = 0
    batch.append(row_bytes)
    batch_bytes += row_size
  if batch:
    yield batch


def _build_row_codec(field_types: Mapping[str, str]) -> _RowCodec:
  """Builds a dynamic `ProtoSchema` and message class for `field_types`."""
  row_desc = descriptor_pb2.DescriptorProto(name="Row")
  for field_num, (column, bq_type) in enumerate(field_types.items(), start=1):
    row_desc.field.add(
        name=column,
        number=field_num,
        type=_BQ_TO_PROTO.get(bq_type, _FIELD_DESC.TYPE_STRING),
        label=_FIELD_DESC.LABEL_OPTIONAL,
    )
  file_proto = descriptor_pb2.FileDescriptorProto(
      name="row.proto", package="bq"
  )
  file_proto.message_type.append(row_desc)
  pool = descriptor_pool.DescriptorPool()
  pool.Add(file_proto)
  row_cls = message_factory.GetMessageClass(
      pool.FindMessageTypeByName("bq.Row")
  )
  return _RowCodec(
      proto_schema=bqs_types.ProtoSchema(proto_descriptor=row_desc),
      row_cls=row_cls,
  )


class BigQueryWriter:
  """Writes rows to a BigQuery table via the Storage Write API default stream.

  Attributes:
    client: Storage Write API client used to append rows.
    table_id: Fully qualified `project.dataset.table` identifier.
    stream_name: Default write stream resource path for `table_id`.
  """

  def __init__(
      self,
      project_id: str,
      dataset_name: str,
      table_name: str,
      *,
      write_client: bigquery_storage_v1.BigQueryWriteClient | None = None,
      bq_client: bigquery.Client | None = None,
  ) -> None:
    """Initializes the instance.

    Args:
      project_id: Google Cloud project ID owning the target dataset.
      dataset_name: BigQuery dataset name.
      table_name: Destination BigQuery table name.
      write_client: Storage Write API client used to append rows. If None,
        instantiates a default client.
      bq_client: BigQuery client used once to inspect the destination table
        schema. If None, instantiates a temporary client and closes it after
        schema resolution; a caller-supplied client is left open.

    Raises:
      BigQueryWriteError: If fetching or mapping the table schema fails.
    """
    self.client = write_client or bigquery_storage_v1.BigQueryWriteClient()
    self.table_id = f"{project_id}.{dataset_name}.{table_name}"
    table_path = (
        f"projects/{project_id}/datasets/{dataset_name}/tables/{table_name}"
    )
    self.stream_name = f"{table_path}/streams/_default"
    self._field_types = self._fetch_field_types(project_id, bq_client)
    self._codec = _build_row_codec(self._field_types)

  def insert_rows(self, rows: Sequence[Mapping[str, Any]]) -> None:
    """Appends rows synchronously via the BigQuery Storage Write API.

    Args:
      rows: Column-to-value mappings matching the destination table schema.

    Raises:
      BigQueryWriteError: If row encoding or stream append fails.
    """
    if not rows:
      return
    serialized_rows = [self._serialize_row(row) for row in rows]
    batches = self._build_append_batches(serialized_rows)
    self._send_append_batches(batches)
    _LOGGER.info("Successfully inserted %d rows.", len(rows))

  def insert_row(self, row: Mapping[str, Any]) -> None:
    """Appends a single row into BigQuery.

    Args:
      row: Column-to-value mapping matching the destination table schema.

    Raises:
      BigQueryWriteError: If row encoding or stream append fails.
    """
    self.insert_rows([row])

  def _fetch_field_types(
      self, project_id: str, bq_client: bigquery.Client | None
  ) -> dict[str, str]:
    """Fetches and validates column types from the destination BigQuery table."""
    resolved_client = bq_client or bigquery.Client(project=project_id)
    try:
      schema = resolved_client.get_table(self.table_id).schema
    except google_exceptions.GoogleAPICallError as err:
      raise BigQueryWriteError(
          f"Failed to fetch schema for {self.table_id}: {err}"
      ) from err
    finally:
      if bq_client is None:
        resolved_client.close()

    field_types: dict[str, str] = {}
    for field in schema:
      field_type = field.field_type.upper()
      if (field.mode or "").upper() == "REPEATED" or field_type in (
          "RECORD",
          "STRUCT",
      ):
        raise BigQueryWriteError(
            f"Unsupported column {field.name}: {field.mode} {field_type}"
        )
      field_types[field.name] = field_type
    return field_types

  def _serialize_row(self, row: Mapping[str, Any]) -> bytes:
    """Validates and serializes a single row dictionary into protobuf bytes."""
    message = self._codec.row_cls()
    for column, value in row.items():
      field_type = self._field_types.get(column)
      if field_type is None:
        raise BigQueryWriteError(f"Unknown column: {column}")
      if value is None:
        continue
      converted = _convert_value(field_type, value)
      try:
        setattr(message, column, converted)
      except (TypeError, ValueError) as err:
        raise BigQueryWriteError(
            f"Invalid {field_type} value for {column}: {value!r}"
        ) from err
    return message.SerializeToString()

  def _build_append_batches(
      self, serialized_rows: Sequence[bytes]
  ) -> list[_AppendBatch]:
    """Chunks serialized rows into `_AppendBatch` requests with row offsets."""
    batches: list[_AppendBatch] = []
    row_offset = 0
    for chunk in _chunk_by_size(serialized_rows, _MAX_REQUEST_BYTES):
      proto_data = bqs_types.AppendRowsRequest.ProtoData(
          writer_schema=self._codec.proto_schema,
          rows=bqs_types.ProtoRows(serialized_rows=chunk),
      )
      request = bqs_types.AppendRowsRequest(
          write_stream=self.stream_name, proto_rows=proto_data
      )
      batches.append(_AppendBatch(row_offset=row_offset, request=request))
      row_offset += len(chunk)
    return batches

  def _send_append_batches(self, batches: Sequence[_AppendBatch]) -> None:
    """Streams `batches` to BigQuery and validates per-chunk responses."""
    metadata = (("x-goog-request-params", f"write_stream={self.stream_name}"),)
    requests = [batch.request for batch in batches]
    response_count = 0
    try:
      responses = self.client.append_rows(
          iter(requests), retry=None, metadata=metadata
      )
      for index, response in enumerate(responses):
        response_count = index + 1
        row_offset = batches[index].row_offset if index < len(batches) else 0
        if response.row_errors:
          details = ", ".join(
              f"row {row_offset + err.index}: {err.message}"
              for err in response.row_errors
          )
          raise BigQueryWriteError(f"BigQuery insert failed ({details})")
        if response.error.code:
          raise BigQueryWriteError(
              f"BigQuery insert failed: {response.error.message}"
          )
    except google_exceptions.GoogleAPICallError as err:
      _LOGGER.exception("Encountered API error while appending rows")
      raise BigQueryWriteError(f"BigQuery insert failed: {err}") from err
    if response_count != len(batches):
      raise BigQueryWriteError(
          f"BigQuery insert incomplete ({response_count}/{len(batches)} acks)"
      )
