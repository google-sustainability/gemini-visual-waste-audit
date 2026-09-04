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

from utils import gemini_processor
from absl.testing import absltest
from absl.testing import parameterized


class GeminiProcessorTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='json_in_markdown',
          text='```json\n{"key": "value"}\n```',
          expected='{"key": "value"}',
      ),
      dict(
          testcase_name='json_in_markdown_with_other_text',
          text='Some text before ```json\n{"key": "value"}\n``` and after.',
          expected='{"key": "value"}',
      ),
      dict(
          testcase_name='json_in_markdown_with_whitespace',
          text='```json\n  {"key": "value"}  \n```',
          expected='{"key": "value"}',
      ),
      dict(
          testcase_name='json',
          text='{"key": "value"}',
          expected='{"key": "value"}',
      ),
      dict(
          testcase_name='json_with_plain_text',
          text='Some text before {"key": "value"} and after.',
          expected='{"key": "value"}',
      ),
      dict(
          testcase_name='nested_json',
          text='{"key": {"nested_key": "nested_value"}}',
          expected='{"key": {"nested_key": "nested_value"}}',
      ),
      dict(
          testcase_name='no_json',
          text='This is just plain text.',
          expected=None,
      ),
      dict(
          testcase_name='empty_string',
          text='',
          expected=None,
      ),
      dict(
          testcase_name='json_with_only_opening_bracket',
          text='{ "key": "value"',
          expected=None,
      ),
      dict(
          testcase_name='json_with_only_closing_bracket',
          text='"key": "value" }',
          expected=None,
      ),
  )
  def test_extract_json(self, text, expected):
    self.assertEqual(gemini_processor._extract_json(text), expected)


if __name__ == '__main__':
  absltest.main()
