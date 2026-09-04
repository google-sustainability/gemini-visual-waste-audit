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

"""Provides a configuration manager for the Gemini Waste Audit Service.

Retrieves and parses prompt setups, model parameters, and material metadata
from a specified Google Spreadsheet. If the spreadsheet is unreachable or
contains invalid data, falls back to local configuration presets.
"""

import json
import logging
from typing import Any, Dict, Optional
import google.auth
from google.genai import types as genai_types
import gspread
from utils import prompts

_REQUIRED_SHEETS = frozenset({"GeneralConfig", "ModelConfigs", "Materials"})
_REQUIRED_GENERAL_COLUMNS = frozenset(
    {"system_instruction_template", "user_prompt"}
)
_REQUIRED_MODEL_COLUMNS = frozenset({
    "Version ID",
    "Model Name",
    "System Instruction Key",
    "User Prompt Key",
})
_REQUIRED_MATERIAL_COLUMNS = frozenset(
    {"Class", "Material", "Density", "Description"}
)


class ConfigManager:
  """Coordinates configuration loading from Google Sheets.

  Retrieves configuration parameters and caches them to avoid redundant network
  requests, falling back to a local preset on failure.
  """

  def __init__(self, spreadsheet_id: str):
    """Initializes the manager with a specific Google Spreadsheet ID."""
    self.spreadsheet_id = spreadsheet_id
    self._config: Optional[Dict[str, Any]] = None

  def _get_gspread_client(self) -> gspread.Client:
    """Authenticates and initializes a gspread client instance."""
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    return gspread.authorize(credentials)

  def load_config(self) -> Optional[Dict[str, Any]]:
    """Retrieves and processes all configuration sheets from the spreadsheet.

    Returns:
      The parsed configuration, or None if loading or schema validation fails.
    """
    try:
      gc = self._get_gspread_client()
      sh = gc.open_by_key(self.spreadsheet_id)
      worksheets = {ws.title: ws for ws in sh.worksheets()}

      if not _REQUIRED_SHEETS.issubset(worksheets.keys()):
        logging.error(
            "Missing required sheets: %s",
            _REQUIRED_SHEETS - worksheets.keys(),
        )
        return None

      # Retrieves records from GeneralConfig, ModelConfigs, and Materials
      # worksheets.
      general_data = worksheets["GeneralConfig"].get_all_records()
      model_data = worksheets["ModelConfigs"].get_all_records()
      materials_data = worksheets["Materials"].get_all_records()

      # Parses and validates raw records into the final configuration schema.
      config = self._parse_config(general_data, model_data, materials_data)
      if config:
        self._config = config
        return config
      return None

    except Exception:
      logging.exception("Failed to load config from spreadsheet")
      return None

  def get_config(self) -> Dict[str, Any]:
    """Provides the active configuration, using a local fallback on failure.

    Returns:
      The cached configuration if available, a newly loaded spreadsheet
      configuration, or the local fallback configuration.
    """
    if self._config:
      return self._config

    config = self.load_config()
    if config:
      return config

    logging.warning("Falling back to local config")
    return prompts.PROMPTS_CONFIG

  def _parse_config(
      self,
      general_data: list[Dict[str, Any]],
      model_data: list[Dict[str, Any]],
      materials_data: list[Dict[str, Any]],
  ) -> Optional[Dict[str, Any]]:
    """Parses raw worksheet records into structured configurations.

    Organizes material densities and maps instructions to generation configs
    for each model version.

    Args:
      general_data: The list of raw general config key-value records.
      model_data: The list of raw model config records.
      materials_data: The list of raw materials records.

    Returns:
      The structured configuration mapping, or None if validation fails.
    """
    general_config = {
        row["Key"]: row["Value"] for row in general_data if row.get("Key")
    }

    if not _REQUIRED_GENERAL_COLUMNS.issubset(general_config.keys()):
      logging.error(
          "Missing required column headers: %s",
          _REQUIRED_GENERAL_COLUMNS - general_config.keys(),
      )
      return None

    material_classes: Dict[str, list[str]] = {}
    density_factors: Dict[str, int] = {}
    material_descriptions: Dict[str, str] = {}

    for row in materials_data:
      if any(row.get(k) in (None, "") for k in _REQUIRED_MATERIAL_COLUMNS):
        continue

      class_name = row["Class"]
      material_name = row["Material"]
      density = row["Density"]
      description = row["Description"]

      try:
        density = int(density)
      except ValueError:
        logging.warning(
            "Density should be a number for %s: %s", material_name, density
        )
        continue

      if class_name not in material_classes:
        material_classes[class_name] = []
      material_classes[class_name].append(material_name)
      density_factors[material_name] = density
      material_descriptions[material_name] = description

    if not material_classes:
      logging.error("No valid materials found")
      return None

    # Groups material metadata by class to construct the prompt payload JSON.
    material_details = {}
    for material_class, materials in material_classes.items():
      material_details[material_class] = {}
      for material in materials:
        material_details[material_class][material] = {
            "Density": density_factors.get(material, 0),
            "Description": material_descriptions.get(material, ""),
        }

    material_details_json = json.dumps(material_details, indent=2)

    # Constructs Python TypedDict class definitions to enforce the structured
    # schema required for model responses.
    class_fields = "\n".join(
        [f"    {c}: Dict[str, Material]" for c in material_classes.keys()]
    )
    material_densities_class_str = f"""class MaterialDensitiesByClasses(TypedDict):
    \"\"\"
    Density factors for materials, organized by classes.
    \"\"\"
{class_fields}"""

    detected_fields = "\n".join(
        [f"    {c}: Dict[str, float]" for c in material_classes.keys()]
    )
    detected_materials_str = f"""class DetectedMaterials(TypedDict):
    \"\"\"
    Detected materials, organized by classes and their respective percentage on the picture
    \"\"\"
{detected_fields}"""

    system_instruction_template = general_config["system_instruction_template"]

    required_placeholders = {
        "{material_details}",
        "{material_densities_class}",
        "{detected_materials}",
    }
    for placeholder in required_placeholders:
      if placeholder not in system_instruction_template:
        logging.error(
            "Missing placeholder %s in system_instruction_template",
            placeholder,
        )
        return None

    system_instruction = system_instruction_template.format(
        material_details=material_details_json,
        material_densities_class=material_densities_class_str,
        detected_materials=detected_materials_str,
    )

    prompts_config = {}
    for row in model_data:
      if any(row.get(k) in (None, "") for k in _REQUIRED_MODEL_COLUMNS):
        continue

      version_id = row["Version ID"]
      model_name = row["Model Name"]
      temperature = row.get("Temperature")
      system_instruction_key = row["System Instruction Key"]
      user_prompt_key = row["User Prompt Key"]
      thinking_level = row.get("Thinking Level", "HIGH")
      enable_code_execution = (
          str(row.get("Enable Code Execution", "")).upper() == "TRUE"
      )

      try:
        temperature = float(temperature) if temperature is not None else 1.0
      except ValueError:
        temperature = 1.0

      if system_instruction_key not in general_config:
        logging.warning(
            "System instruction key %s not found in GeneralConfig",
            system_instruction_key,
        )
        continue
      if user_prompt_key not in general_config:
        logging.warning(
            "User prompt key %s not found in GeneralConfig",
            user_prompt_key,
        )
        continue

      tools = []
      if enable_code_execution:
        tools.append(
            genai_types.Tool(code_execution=genai_types.ToolCodeExecution())
        )

      gen_config = genai_types.GenerateContentConfig(
          system_instruction=system_instruction,
          temperature=temperature,
          safety_settings=[
              genai_types.SafetySetting(
                  category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"
              ),
              genai_types.SafetySetting(
                  category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"
              ),
              genai_types.SafetySetting(
                  category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"
              ),
              genai_types.SafetySetting(
                  category="HARM_CATEGORY_HARASSMENT", threshold="OFF"
              ),
          ],
          tools=tools if tools else None,
          thinking_config=genai_types.ThinkingConfig(
              thinking_level=thinking_level
          )
      )

      prompts_config[version_id] = {
          "generation_config": gen_config,
          "prompt_text": general_config[user_prompt_key],
          "model_name": model_name,
      }

    if not prompts_config:
      logging.error("No valid model configs found")
      return None

    return prompts_config
