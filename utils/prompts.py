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

"""Configuration file for Gemini prompts and models.

This module defines the material classifications, density factors, and the
structured prompts used for analyzing waste audit images with the Gemini API.
"""

import json
from typing import Any, Dict, TypeAlias, Union

from google.genai import types as genai_types


PromptConfig: TypeAlias = Dict[
    str, Union[str, genai_types.GenerateContentConfig]
]

# --- Material Definitions ---

MATERIAL_CLASSES: Dict[str, list[str]] = {
    "Paper": [
        "Cardboard (unwaxed)",
        "Mixed paper",
        "Coated cartons and cups",
        "Paper food serviceware",
        "Compostable paper towels",
    ],
    "Plastic": [
        "#1 & #2 HDPE containers",
        "#3-6 containers",
        "#7 PLA or PHA rigid plastics",
        "Other rigid plastics",
        "#7 PLA or PHA film plastics",
        "Latex/nitrile gloves",
        "Snack wrappers",
        "Film plastic",
    ],
    "Metal": ["Aluminum cans", "Other recyclable metals"],
    "Glass": ["Recyclable glass", "Other glass"],
    "Organics": ["Food & liquids", "Other compostable organics"],
    "Other": [
        "E-waste and batteries",
        "HHW",
        "Operational waste (C&D, HVAC, furniture)",
        "Other residuals",
    ],
}

DENSITY_FACTORS: Dict[str, int] = {
    "Cardboard (unwaxed)": 106,
    "Mixed paper": 158,
    "Coated cartons and cups": 158,
    "Paper food serviceware": 138,
    "Compostable paper towels": 138,
    "#1 & #2 HDPE containers": 35,
    "#3-6 containers": 35,
    "#7 PLA or PHA rigid plastics": 35,
    "Other rigid plastics": 26,
    "#7 PLA or PHA film plastics": 35,
    "Latex/nitrile gloves": 35,
    "Snack wrappers": 35,
    "Film plastic": 23,
    "Aluminum cans": 46,
    "Other recyclable metals": 225,
    "Recyclable glass": 380,
    "Other glass": 999,
    "Food & liquids": 486,
    "Other compostable organics": 135,
    "E-waste and batteries": 438,
    "HHW": 1671,
    "Operational waste (C&D, HVAC, furniture)": 417,
    "Other residuals": 135,
}

MATERIAL_DESCRIPTIONS: Dict[str, str] = {
    "Cardboard (unwaxed)": (
        "Kraft linerboard, containerboard cartons and shipping boxes with"
        " corrugated paper medium (unwaxed). This category also includes Kraft"
        " (brown) paper bags. Excludes waxed and plastic-coated cardboard,"
        " solid boxboard, and bags that are not pure unbleached Kraft."
    ),
    "Mixed paper": (
        "Mixed recyclable papers, including white ledger, white office paper,"
        " newspaper, shredded paper, junk mail, magazines, colored papers,"
        " bleached Kraft, boxboard, polycoated containers, mailing tubes, clean"
        " paper cups, and paperback books. Includes paper packaging made"
        " primarily of paper but with a non-paper attachment. Examples include"
        " laundry detergent boxes with plastic handles, paper bags with plastic"
        " handles, boxboard salt containers with plastic or metal spouts, and"
        " aluminum foil packaging boxes with metal serrated edges."
    ),
    "Coated cartons and cups": (
        "Gable tops, aseptic containers, polycoated containers, and polycoated"
        " cups."
    ),
    "Paper food serviceware": (
        "Waxed cardboard: Corrugated (multi-layered) cardboard that is lined"
        " with polyethylene to prevent it from getting soggy.\nFiber-based"
        " paper containers: Cups or tubs made of paper which can be composted"
        " and are generally not recycled. May be contaminated with food,"
        " moisture, or wax. Containers are clearly labeled with 'commercially"
        " compostable'."
    ),
    "Compostable paper towels": (
        "Any low-grade or food-soiled paper towels that could potentially be"
        " composted. Used for hand drying in the bathrooms or café areas."
    ),
    "#1 & #2 HDPE containers": (
        "#1 PET single-use beverage containers: Polyethylene terephthalate"
        " bottles bearing the #1, such as carbonated drink bottles and water"
        " bottles. Lids and caps are left attached to containers when"
        " feasible.\n#1 PET other containers: Includes plastic non-bottle"
        " packaging bearing the #1, and would include oven-ready meal trays and"
        " other packaging. Also includes blow molded jars bearing the #1, such"
        " as those commonly used for peanut butter.\n#2 HDPE containers: Yogurt"
        " and margarine tubs and any packaging jar or tub bearing the #2. Lids"
        " and caps are left attached to containers when feasible."
    ),
    "#3-6 containers": (
        "All other plastic bottles, tubs, jars, cups, and other containers not"
        " included in categories above. May be labeled PVC (#3), Low-Density"
        " Polyethylene ('LDPE' or #4), Polypropylene ('PP', #5), or Polystyrene"
        " ('PS', #6)."
    ),
    "#7 PLA or PHA rigid plastics": (
        "#7 potentially compostable plastic serviceware: Any compostable"
        " plastic cutlery or utensils made from corn, potato, sugarcane or any"
        " other compostable resin. Examples include compostable straws, forks,"
        " knives and spoons.\n#7 potentially compostable plastic ups: Any"
        " compostable plastic cup made from corn, potato, sugarcane or any"
        " other compostable resin. Examples include beverage cups.\n#7"
        " potentially compostable rigid plastic other containers: Any"
        " compostable plastic packaging or food containers made from corn,"
        " potato, sugarcane or any other compostable resin. Examples include"
        " fast food clamshell containers or soup cups. Lids and caps are left"
        " attached to containers when feasible."
    ),
    "Other rigid plastics": (
        "#3-6 other plastics: All other plastics not included in categories"
        " above. May be labeled PVC (#3), Low-Density Polyethylene (''LDPE'' or"
        " #4), Polypropylene (''PP'', #5), or Polystyrene (''PS'', #6).\n#6"
        " expanded polystyrene: Expanded polystyrene (#6) foodservice"
        " containers and packaging materials. Examples include clamshells,"
        " trays, cups and packaging blocks.\nNon-recoverable/composite"
        " plastics: Includes all other types of plastic that are not one of the"
        " above materials and items that are too small to be properly"
        " identified (''2 inches''), also composites of multiple plastics and"
        " plastics mixed with other materials."
    ),
    "#7 PLA or PHA film plastics": (
        "Includes compostable plastic items, such as film 'plastic' bags made"
        " of materials such as corn starch or soy designed to compost (e.g.,"
        " BioBag, EcoSafe)."
    ),
    "Latex/nitrile gloves": "Personal protective gloves used in food handling.",
    "Snack wrappers": (
        "Lay flat packaging, intended for snacks. Include chip bags, granola"
        " bars, trail mix bags, and others."
    ),
    "Film plastic": (
        "Clean plastic film: Plastic grocery 't-shirt' and retail bags; bread"
        " and produce bags; newspaper bags; napkin, towel, tissue, diaper"
        " overwrap; bottled water case wrap; dry cleaner film bags; case and"
        " stretch wrap; plastic air pillows; food storage ('Ziploc') bags; all"
        " other film packaging that is potentially recyclable.\nNon-recoverable"
        " plastic film: Includes all other types of plastic film that are not"
        " one of the above film materials.\nDisposal bags: all bags used to"
        " contain and transport waste from generation point to disposal site."
        " Includes garbage bags, bags for recycling (clear or blue), and"
        " non-compostable bags used to contain compost. This does not include"
        " clean unused disposal bags."
    ),
    "Aluminum cans": (
        "Aluminum cans containing soda, carbonated juice, seltzer, tea,"
        " sparkling water, beer, hard cider, energy drinks, and all other"
        " aluminum cans (such as cat food containers)."
    ),
    "Other recyclable metals": (
        "Includes tinned steel food, scrap metal, clean aluminum foil, metal"
        " tools."
    ),
    "Recyclable glass": (
        "Includes any color pop, liquor, wine, juice, beer. Also includes other"
        " glass containers that can be recycled - food bottles, jars, and"
        " containers."
    ),
    "Other glass": (
        "Other types of glass products and scrap that do not fit into the above"
        " materials, including light bulbs, glassware, oven-safe baking dishes"
        " such as Pyrex, and non-C&D fiberglass."
    ),
    "Food & liquids": (
        "Potentially edible food: Plant-based and animal-derived foods that"
        " appear to be edible and pre-consumer. This includes edible breads,"
        " candies, chicken, and unopened single servings of food. This can"
        " include the food container when the container weight is not"
        " appreciable compared to the food inside.\nInedible scraps and"
        " post-consumer food: Plant-based foods and animal-derived foods that"
        " are post-consumer (plate scrapings) or trim resulting from food"
        " preparation. This includes coffee filters, tea bags, bones, and"
        " eggshells, fruit from spa water. This category can include the food"
        " container when the container weight is not appreciable compared to"
        " the food inside.\nLiquids: Beverages or other liquids including the"
        " container when the container weight is not appreciable compared to"
        " the liquid inside."
    ),
    "Other compostable organics": (
        "Organic materials such as wood products (dimensional lumber, wood"
        " pallets, pressure treated wood, particle board, and plywood), yard"
        " waste (leaves, grass clippings, garden wastes, and brush)."
    ),
    "E-waste and batteries": (
        "E-waste: Includes computer equipment, phones, radios, chargers, and"
        " televisions. Also includes any battery-powered devices.\nBatteries:"
        " Batteries."
    ),
    "HHW": (
        "Includes paint, hazardous cleaning products, industrial chemicals,"
        " motor oil, sharps, and medical waste."
    ),
    "Operational waste (C&D, HVAC, furniture)": (
        "Any waste that is generated as a result of typical site operations."
        " May include construction and demolition, HVAC and furniture from day"
        " to day operations."
    ),
    "Other residuals": (
        "Any item not described above, such as textiles, construction debris,"
        " soil, sand, dirt, grit, and non-distinct materials. Also includes"
        " aseptic containers such as soy milk boxes."
    ),
}

# --- Prompt Generation ---


def _build_material_details() -> Dict[str, Any]:
  """Builds a nested dictionary of material details for the prompt."""
  details = {}
  for material_class, materials in MATERIAL_CLASSES.items():
    details[material_class] = {}
    for material in materials:
      if material in DENSITY_FACTORS and material in MATERIAL_DESCRIPTIONS:
        details[material_class][material] = {
            "Density": DENSITY_FACTORS[material],
            "Description": MATERIAL_DESCRIPTIONS[material],
        }
  return details


WASTE_AUDIT_SYSTEM_INSTRUCTION = f"""
# Role
You are a highly skilled expert in waste characterization and material estimation using visual data analysis.
Your objective is to analyze images of waste piles, focusing on items within a designated container,
to accurately estimate material types and their respective volumes.

# Context
Utilize the provided density factors to aid in your estimations.
Disregard any bags or tarps containing the items, as well as any signage or cards indicating the source of the items.

Material classes and density factors are provided below in JSON format:
```json
{json.dumps(_build_material_details(), indent=2)}
```

These density factors assume the following data structure:
```py
class Material(TypedDict):
  Description: str
  Density: int

class MaterialDensitiesByClasses(TypedDict):
    \"\"\"
    Density factors for materials, organized by classes.
    \"\"\"
    Paper: Dict[str, Material]
    Plastic: Dict[str, Material]
    Metal: Dict[str, Material]
    Glass: Dict[str, Material]
    Organics: Dict[str, Material]
    Other: Dict[str, Material]
```

# Instructions
1. **Image Analysis**: Conduct a thorough examination of the provided image, concentrating solely on the items inside the designated container (e.g., recycling bin, trash bin, container, or bag).
2. **Material Identification**: Identify and list all distinct material types visible on the surface of the waste pile within the container. Prioritize the identification of contents within containers (e.g., liquids in bottles, food in clear containers).
3. **Percentage Estimation**: For each identified material type, estimate its approximate percentage of the total visible volume within the container. **Important**: If a container holds food or liquid (e.g., a bottle with liquid, a food-soiled container), and the container weight is negligible compared to the contents, categorize the *entire* volume of that item as "Food & liquids".
4. **Consistency Verification**: Review the estimated percentages to ensure they sum to approximately 100%. Adjust individual percentages as needed to accurately reflect the visual proportions. Exercise caution, as previous analyses indicate a tendency to overestimate "Paper" volume and underestimate "Organics" and "Plastic" types.
5. **Weight Calculation**: Based on the estimated percentages (assuming 1 cubic yard total volume) and the provided density factors, calculate the estimated weight of each material type.
6. **Final Validation**: Review the estimated material types and weights to ensure consistency with the visual information and the provided density factors. If inconsistencies are identified, revisit step 2 to refine your analysis.

# Output Schema
Your output must adhere to the following JSON schema:

```py
class DetectedMaterials(TypedDict):
    \"\"\"
    Detected materials, organized by classes and their respective percentage on the picture
    \"\"\"
    Paper: Dict[str, float]
    Plastic: Dict[str, float]
    Metal: Dict[str, float]
    Glass: Dict[str, float]
    Organics: Dict[str, float]
    Other: Dict[str, float]

    TotalWeightLbs: float # the estimated total weight of the materials in the container in lbs.
```

Ensure that the estimated percentages sum to 1.0 (100%).

# Output Format
Begin by providing a detailed rationale.
Explain your step-by-step analysis, observations of the image, and the methodology used to determine the percentages.
Following the rationale, present the final JSON result within a ```json``` code block.
"""

WASTE_AUDIT_USER_PROMPT = """
Please analyze the provided image according to the system instructions.
"""

DEFAULT_WASTE_AUDIT_GENERATION_CONFIG = genai_types.GenerateContentConfig(
    system_instruction=WASTE_AUDIT_SYSTEM_INSTRUCTION,
    temperature=1.0,
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
)

PROMPTS_CONFIG: Dict[str, PromptConfig] = {
    "v2.26-G3.7-flash": {
        "generation_config": genai_types.GenerateContentConfig(
            **DEFAULT_WASTE_AUDIT_GENERATION_CONFIG.model_dump(
                exclude_none=True
            ),
            thinking_config=genai_types.ThinkingConfig(
                thinking_level="HIGH",
            ),
            tools=[
                genai_types.Tool(code_execution=genai_types.ToolCodeExecution),
            ],
        ),
        "prompt_text": WASTE_AUDIT_USER_PROMPT,
        "model_name": "gemini-3.7-flash",
    },
    "v2.26-G3.1-pro": {
        "generation_config": DEFAULT_WASTE_AUDIT_GENERATION_CONFIG,
        "prompt_text": WASTE_AUDIT_USER_PROMPT,
        "model_name": "gemini-3.1-pro-preview",
    },
}

if __name__ == "__main__":
  print(json.dumps(_build_material_details(), indent=2))
  print(PROMPTS_CONFIG)
