#!/usr/bin/env python3
"""
Generate Pydantic models from ENA API fields.

This script fetches field information from the ENA API for a specific result type,
parses the TSV response, and generates a Pydantic model based on the fields.

Usage:
    python3 workflows/ena_utils/generate_ena_model.py <result_type>

Example:
    python3 workflows/ena_utils/generate_ena_model.py analysis

This will generate an ENAAnalysisQuery model based on the fields returned by
https://www.ebi.ac.uk/ena/portal/api/searchFields?dataPortal=metagenome&result=analysis
and an ENAAnalysisFields enum based on the fields returned by
https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=metagenome&result=analysis.
"""
import argparse
import sys
from datetime import date
from typing import Literal

import pandas as pd
from caseconverter import pascalcase
from django.utils.html import escape


def fetch_ena_fields(
    result: str,
    which_fields: Literal["search", "return"],
    data_portal: str = "metagenome",
) -> pd.DataFrame:
    url = f"https://www.ebi.ac.uk/ena/portal/api/{which_fields}Fields?dataPortal={data_portal}&result={result}"
    return pd.read_csv(url, sep="\t")


def generate_pydantic_model(result_type: str, data_portal: str = "metagenome") -> str:
    """
    Generate a Pydantic model from the parsed fields.

    Args:
        result_type: The result type used to name the model
        data_portal: The ENA data portal to fetch from

    Returns:
        A string containing the Pydantic model code
    """
    class_name = f"ENA{pascalcase(result_type)}Query"

    model_code = [
        f"class {class_name}(_ENAQueryConditions):",
        f"    # From: https://www.ebi.ac.uk/ena/portal/api/searchFields?dataPortal=metagenome&result={result_type} {date.today().strftime('%Y/%m/%d')}",
        "    # Some are controlled values not yet controlled here",
    ]

    fields = fetch_ena_fields(result_type, "search", data_portal)

    for _, row in fields.iterrows():
        field_name = row["columnId"]
        description = escape(row["description"])
        field_type = row["type"]

        python_type = "str"
        if field_type == "number":
            python_type = "int"
        elif field_type == "date":
            python_type = "date"

        model_code.append(
            f'    {field_name}: Optional[{python_type}] = Field(None, description="{description}")'
        )

    model_code.append("")

    enum_class_name = f"ENA{pascalcase(result_type)}Fields"
    model_code.extend(
        [
            "",
            f"class {enum_class_name}(FutureStrEnum):",
            f"    # from https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=metagenome&result={result_type} {date.today().strftime('%Y-%m-%d')}",
        ]
    )

    fields = fetch_ena_fields(result_type, "return", data_portal)

    for _, row in fields.iterrows():
        field_name = row["columnId"]
        description = row["description"]
        # Convert field name to uppercase for enum
        enum_name = field_name.upper()
        model_code.append(f'    {enum_name} = "{field_name}"  # {description}')

    return "\n".join(model_code)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Pydantic models from ENA API fields"
    )
    parser.add_argument("result", help="The result type (e.g., 'study', 'analysis')")
    parser.add_argument(
        "--data-portal",
        default="metagenome",
        help="The data portal to use (default: 'metagenome')",
    )

    args = parser.parse_args()

    try:
        model_code = generate_pydantic_model(args.result, args.data_portal)

        print(model_code)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
