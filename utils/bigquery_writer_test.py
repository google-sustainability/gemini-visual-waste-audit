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
from typing import Any
from unittest import mock

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types as bqs_types
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory

from utils import bigquery_writer
from absl.testing import absltest
from absl.testing import parameterized

_PROJECT_ID = "test-proj"
_DATASET_NAME = "test_ds"
_TABLE_NAME = "test_tbl"
_STREAM = (
    f"projects/{_PROJECT_ID}/datasets/{_DATASET_NAME}"
    f"/tables/{_TABLE_NAME}/streams/_default"
)
_TS_ISO = "2026-10-26T21:00:00Z"
_TS_US = 1793048400000000
_DEFAULT_SCHEMA = (
    bigquery.SchemaField("image_id", "STRING"),
    bigquery.SchemaField("ts", "TIMESTAMP"),
    bigquery.SchemaField("is_deleted", "BOOL"),
    bigquery.SchemaField("attempts", "INT64"),
    bigquery.SchemaField("percentage", "FLOAT64"),
    bigquery.SchemaField("gemini_result", "JSON"),
    bigquery.SchemaField("payload", "BYTES"),
)


class BigQueryWriterTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.captured: list[bqs_types.AppendRowsRequest] = []
    self.mock_write = mock.create_autospec(
        bigquery_storage_v1.BigQueryWriteClient, instance=True, spec_set=True
    )
    self.mock_bq = mock.create_autospec(bigquery.Client, instance=True)
    self.mock_bq.get_table.return_value.schema = list(_DEFAULT_SCHEMA)

    def fake_append_rows(requests, **kwargs):
      del kwargs
      batch = list(requests)
      self.captured.extend(batch)
      return iter(bqs_types.AppendRowsResponse() for _ in batch)

    self.mock_write.append_rows.side_effect = fake_append_rows
    self.writer = self._create_writer()

  def _create_writer(
      self,
      *,
      project_id: str = _PROJECT_ID,
      dataset_name: str = _DATASET_NAME,
      table_name: str = _TABLE_NAME,
  ) -> bigquery_writer.BigQueryWriter:
    return bigquery_writer.BigQueryWriter(
        project_id,
        dataset_name,
        table_name,
        write_client=self.mock_write,
        bq_client=self.mock_bq,
    )

  def _decode_wire_row(self, request_index: int = 0, row_index: int = 0) -> Any:
    proto_rows = self.captured[request_index].proto_rows
    file_proto = descriptor_pb2.FileDescriptorProto(
        name="wire.proto", package="wire"
    )
    file_proto.message_type.append(proto_rows.writer_schema.proto_descriptor)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    row_cls = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("wire.Row")
    )
    return row_cls.FromString(proto_rows.rows.serialized_rows[row_index])

  def test_insert_rows_sets_routing_metadata_and_disables_retry(self):
    writer = self._create_writer(
        project_id=_PROJECT_ID,
        dataset_name=_DATASET_NAME,
        table_name=_TABLE_NAME,
    )

    writer.insert_rows([{"image_id": "img-1"}])

    call_kwargs = self.mock_write.append_rows.call_args.kwargs
    self.assertIsNone(call_kwargs["retry"])
    self.assertEqual(
        call_kwargs["metadata"],
        (("x-goog-request-params", f"write_stream={_STREAM}"),),
    )

  def test_insert_rows_omits_none_fields(self):
    self.writer.insert_rows([{"image_id": None, "attempts": 1}])

    decoded = self._decode_wire_row()
    self.assertFalse(decoded.HasField("image_id"))
    self.assertEqual(decoded.attempts, 1)

  @parameterized.named_parameters(
      dict(
          testcase_name="bool_false_str",
          column="is_deleted",
          input_value="false",
          expected_value=False,
      ),
      dict(
          testcase_name="bool_zero_int",
          column="is_deleted",
          input_value=0,
          expected_value=False,
      ),
      dict(
          testcase_name="bool_false_literal",
          column="is_deleted",
          input_value=False,
          expected_value=False,
      ),
      dict(
          testcase_name="bool_true_str",
          column="is_deleted",
          input_value="1",
          expected_value=True,
      ),
      dict(
          testcase_name="bool_true_int",
          column="is_deleted",
          input_value=1,
          expected_value=True,
      ),
      dict(
          testcase_name="bool_true_literal",
          column="is_deleted",
          input_value=True,
          expected_value=True,
      ),
      dict(
          testcase_name="timestamp_iso_z",
          column="ts",
          input_value=_TS_ISO,
          expected_value=_TS_US,
      ),
      dict(
          testcase_name="timestamp_utc_suffix",
          column="ts",
          input_value="2026-10-26 21:00:00 UTC",
          expected_value=_TS_US,
      ),
      dict(
          testcase_name="timestamp_naive_datetime",
          column="ts",
          input_value=datetime.datetime(2026, 10, 26, 21, 0),
          expected_value=_TS_US,
      ),
      dict(
          testcase_name="timestamp_epoch_seconds",
          column="ts",
          input_value=1700000000.123,
          expected_value=1700000000123000,
      ),
      dict(
          testcase_name="timestamp_epoch_millis",
          column="ts",
          input_value=1700000000123,
          expected_value=1700000000123000,
      ),
      dict(
          testcase_name="timestamp_epoch_micros",
          column="ts",
          input_value=1700000000123456,
          expected_value=1700000000123456,
      ),
      dict(
          testcase_name="int64_from_str",
          column="attempts",
          input_value="3",
          expected_value=3,
      ),
      dict(
          testcase_name="float64_from_str",
          column="percentage",
          input_value="0.75",
          expected_value=0.75,
      ),
      dict(
          testcase_name="json_from_dict",
          column="gemini_result",
          input_value={"Cardboard": 0.75},
          expected_value='{"Cardboard": 0.75}',
      ),
      dict(
          testcase_name="bytes_literal",
          column="payload",
          input_value=b"raw",
          expected_value=b"raw",
      ),
  )
  def test_insert_rows_converts_column_values(
      self, column, input_value, expected_value
  ):
    self.writer.insert_rows([{column: input_value}])

    decoded = self._decode_wire_row()
    self.assertTrue(decoded.HasField(column))
    self.assertEqual(getattr(decoded, column), expected_value)

  def test_insert_rows_with_empty_sequence_does_not_call_append(self):
    self.writer.insert_rows([])

    self.mock_write.append_rows.assert_not_called()

  def test_insert_row_appends_single_row(self):
    self.writer.insert_row({"image_id": "img-1"})

    self.assertEqual(self._decode_wire_row().image_id, "img-1")

  def test_insert_rows_chunks_batches_and_offsets_row_error_index(self):
    responses = [
        bqs_types.AppendRowsResponse(),
        bqs_types.AppendRowsResponse(
            row_errors=[bqs_types.RowError(index=0, message="bad second chunk")]
        ),
    ]

    def chunked_append(requests, **kwargs):
      del kwargs
      batch = list(requests)
      self.captured.extend(batch)
      return iter(responses[: len(batch)])

    self.mock_write.append_rows.side_effect = chunked_append
    self.enter_context(
        mock.patch.object(bigquery_writer, "_MAX_REQUEST_BYTES", 10)
    )

    with self.assertRaisesRegex(
        bigquery_writer.BigQueryWriteError, "row 1: bad second chunk"
    ):
      self.writer.insert_rows([{"image_id": "row-0"}, {"image_id": "row-1"}])

    self.assertLen(self.captured, 2)

  def test_init_closes_owned_bq_client_and_wraps_get_table_error(self):
    mock_bq_cls = self.enter_context(
        mock.patch.object(bigquery, "Client", autospec=True)
    )
    owned_client = mock_bq_cls.return_value
    owned_client.get_table.side_effect = google_exceptions.NotFound("missing")

    with self.assertRaisesRegex(
        bigquery_writer.BigQueryWriteError, "Failed to fetch schema"
    ):
      bigquery_writer.BigQueryWriter(
          _PROJECT_ID,
          _DATASET_NAME,
          _TABLE_NAME,
          write_client=self.mock_write,
      )

    owned_client.close.assert_called_once()

  def test_init_rejects_unsupported_repeated_or_struct_schema_fields(self):
    self.mock_bq.get_table.return_value.schema = [
        bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
    ]

    with self.assertRaisesRegex(
        bigquery_writer.BigQueryWriteError, "Unsupported column tags"
    ):
      self._create_writer()

  @parameterized.named_parameters(
      dict(
          testcase_name="unknown_column",
          rows=[{"unknown_col": "x"}],
          err_regex="Unknown column",
      ),
      dict(
          testcase_name="invalid_bool_str",
          rows=[{"is_deleted": "maybe"}],
          err_regex="Invalid BOOL value",
      ),
      dict(
          testcase_name="invalid_bool_int",
          rows=[{"is_deleted": 2}],
          err_regex="Invalid BOOL value",
      ),
      dict(
          testcase_name="invalid_timestamp",
          rows=[{"ts": "not-a-date"}],
          err_regex="Invalid TIMESTAMP value",
      ),
      dict(
          testcase_name="nan_float",
          rows=[{"percentage": float("nan")}],
          err_regex="Invalid FLOAT64 value",
      ),
      dict(
          testcase_name="bool_for_int",
          rows=[{"attempts": True}],
          err_regex="Invalid INT64 value",
      ),
      dict(
          testcase_name="invalid_bytes",
          rows=[{"payload": "text"}],
          err_regex="Invalid BYTES value",
      ),
  )
  def test_insert_rows_rejects_invalid_schema_or_values(self, rows, err_regex):
    with self.assertRaisesRegex(bigquery_writer.BigQueryWriteError, err_regex):
      self.writer.insert_rows(rows)

  def test_insert_rows_rejects_single_row_exceeding_max_request_bytes(self):
    self.enter_context(
        mock.patch.object(bigquery_writer, "_MAX_REQUEST_BYTES", 4)
    )

    with self.assertRaisesRegex(
        bigquery_writer.BigQueryWriteError, "exceeds limit"
    ):
      self.writer.insert_rows([{"image_id": "oversized-row"}])

  @parameterized.named_parameters(
      dict(
          testcase_name="stream_error",
          responses=[
              bqs_types.AppendRowsResponse(error={"code": 3, "message": "bad"})
          ],
          err_regex="bad",
      ),
      dict(
          testcase_name="incomplete_acks",
          responses=[],
          err_regex="incomplete",
      ),
  )
  def test_insert_rows_raises_on_stream_response_failure(
      self, responses, err_regex
  ):
    self.mock_write.append_rows.side_effect = None
    self.mock_write.append_rows.return_value = iter(responses)

    with self.assertRaisesRegex(bigquery_writer.BigQueryWriteError, err_regex):
      self.writer.insert_rows([{"image_id": "bad"}])

  def test_insert_rows_wraps_api_call_error(self):
    self.mock_write.append_rows.side_effect = google_exceptions.BadRequest(
        "rpc bad"
    )

    with self.assertRaisesRegex(bigquery_writer.BigQueryWriteError, "rpc bad"):
      self.writer.insert_rows([{"image_id": "bad"}])


if __name__ == "__main__":
  absltest.main()
