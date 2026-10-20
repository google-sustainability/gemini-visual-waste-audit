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

"""Tests for ConfigManager in utils."""

import dataclasses
import importlib.util
import os
import sys
from typing import Any
from unittest import mock

from utils import prompts
from absl.testing import absltest

sys.modules["gspread"] = mock.MagicMock()
sys.modules["utils"] = mock.MagicMock(prompts=prompts)
sys.modules["utils.prompts"] = prompts

_CONFIG_MANAGER_PATH = os.path.join(
    os.path.dirname(__file__), "config_manager.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "config_manager", _CONFIG_MANAGER_PATH
)
config_manager = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(config_manager)


@dataclasses.dataclass
class _FakeWorksheet:
  title: str
  records: list[dict[str, Any]] = dataclasses.field(default_factory=list)

  def get_all_records(self) -> list[dict[str, Any]]:
    return self.records


@dataclasses.dataclass
class _FakeSpreadsheet:
  sheets: list[_FakeWorksheet]

  def worksheets(self) -> list[_FakeWorksheet]:
    return self.sheets


@dataclasses.dataclass
class _FakeGspreadClient:
  spreadsheet: _FakeSpreadsheet

  def open_by_key(self, unused_key: str) -> _FakeSpreadsheet:
    return self.spreadsheet


class ConfigManagerTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.general_ws = _FakeWorksheet(title="GeneralConfig")
    self.model_ws = _FakeWorksheet(title="ModelConfigs")
    self.materials_ws = _FakeWorksheet(
        title="Materials",
        records=[
            {
                "Class": "Paper",
                "Material": "Cardboard",
                "Density": 100,
                "Description": "Corrugated cardboard boxes.",
            },
        ],
    )
    fake_client = _FakeGspreadClient(
        spreadsheet=_FakeSpreadsheet(
            sheets=[self.general_ws, self.model_ws, self.materials_ws]
        )
    )
    patcher = mock.patch.object(
        config_manager.ConfigManager,
        "_get_gspread_client",
        return_value=fake_client,
    )
    patcher.start()
    self.addCleanup(patcher.stop)

  def test_get_config_maps_distinct_system_instruction_keys_per_model_row(self):
    self.general_ws.records = [
        {
            "Key": "system_instruction_template",
            "Value": (
                "Legacy prompt: {material_details} "
                "{material_densities_class} {detected_materials}"
            ),
        },
        {
            "Key": "argon_system_instruction_template",
            "Value": (
                "Argon prompt: {material_details} "
                "{material_densities_class} {detected_materials}"
            ),
        },
        {
            "Key": "user_prompt",
            "Value": "Estimate the waste composition.",
        },
    ]
    self.model_ws.records = [
        {
            "Version ID": "v2.26-G4-argon",
            "Model Name": "gemini-4-argon",
            "Thinking Level": "HIGH",
            "System Instruction Key": "system_instruction_template",
            "User Prompt Key": "user_prompt",
            "Enable Code Execution": "TRUE",
        },
        {
            "Version ID": "v10.14-G4-argon",
            "Model Name": "gemini-4-argon",
            "Thinking Level": "HIGH",
            "System Instruction Key": "argon_system_instruction_template",
            "User Prompt Key": "user_prompt",
            "Enable Code Execution": "TRUE",
        },
    ]

    manager = config_manager.ConfigManager("sheet-123")
    result = manager.get_config()

    self.assertIn("v2.26-G4-argon", result)
    self.assertIn("v10.14-G4-argon", result)
    legacy_entry = result["v2.26-G4-argon"]
    argon_entry = result["v10.14-G4-argon"]

    self.assertEqual(legacy_entry["model_name"], "gemini-4-argon")
    self.assertEqual(
        legacy_entry["prompt_text"], "Estimate the waste composition."
    )
    self.assertIn(
        "Legacy prompt:", legacy_entry["generation_config"].system_instruction
    )
    self.assertIn(
        "Cardboard", legacy_entry["generation_config"].system_instruction
    )

    self.assertEqual(argon_entry["model_name"], "gemini-4-argon")
    self.assertEqual(
        argon_entry["prompt_text"], "Estimate the waste composition."
    )
    self.assertIn(
        "Argon prompt:", argon_entry["generation_config"].system_instruction
    )
    self.assertIn(
        "Cardboard", argon_entry["generation_config"].system_instruction
    )

  def test_get_config_skips_row_with_missing_placeholder_and_keeps_valid_row(
      self,
  ):
    self.general_ws.records = [
        {
            "Key": "system_instruction_template",
            "Value": (
                "Valid prompt: {material_details} "
                "{material_densities_class} {detected_materials}"
            ),
        },
        {
            "Key": "invalid_instruction_template",
            "Value": "Invalid prompt missing placeholders",
        },
        {
            "Key": "user_prompt",
            "Value": "Estimate the waste composition.",
        },
    ]
    self.model_ws.records = [
        {
            "Version ID": "v-valid",
            "Model Name": "gemini-4-argon",
            "Thinking Level": "HIGH",
            "System Instruction Key": "system_instruction_template",
            "User Prompt Key": "user_prompt",
            "Enable Code Execution": "TRUE",
        },
        {
            "Version ID": "v-invalid",
            "Model Name": "gemini-4-argon",
            "Thinking Level": "HIGH",
            "System Instruction Key": "invalid_instruction_template",
            "User Prompt Key": "user_prompt",
            "Enable Code Execution": "TRUE",
        },
    ]

    manager = config_manager.ConfigManager("sheet-123")
    result = manager.get_config()

    self.assertIn("v-valid", result)
    self.assertNotIn("v-invalid", result)

  def test_get_config_falls_back_to_local_prompts_when_all_rows_invalid(self):
    self.general_ws.records = [
        {
            "Key": "system_instruction_template",
            "Value": "Missing placeholders",
        },
        {
            "Key": "user_prompt",
            "Value": "Estimate the waste composition.",
        },
    ]
    self.model_ws.records = [
        {
            "Version ID": "v-invalid",
            "Model Name": "gemini-4-argon",
            "Thinking Level": "HIGH",
            "System Instruction Key": "system_instruction_template",
            "User Prompt Key": "user_prompt",
            "Enable Code Execution": "TRUE",
        },
    ]

    manager = config_manager.ConfigManager("sheet-123")
    result = manager.get_config()

    self.assertEqual(result, prompts.PROMPTS_CONFIG)


if __name__ == "__main__":
  absltest.main()
