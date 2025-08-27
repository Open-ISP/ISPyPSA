"""Main validation module for ISPyPSA templater outputs using pointblank."""

from pathlib import Path

import pandas as pd
import pointblank as pb
import yaml


def validate_ispypsa_inputs(
    ispypsa_tables: dict[str, pd.DataFrame], schema_dir: Path | None = None
) -> None:
    """
    Validates ISPyPSA templater output tables using pointblank YAML schemas.

    Only validates tables that have corresponding YAML schema files.
    Raises exception on first validation failure.

    Args:
        ispypsa_tables: Dictionary of DataFrames containing the templater output tables
        schema_dir: Path to directory containing YAML validation schemas.
                   If None, uses default location in the package.

    Raises:
        ValueError: If any validation fails or if a table with a config is missing
    """
    if schema_dir is None:
        schema_dir = Path(__file__).parent / "schemas"

    # Find all YAML schema files and determine which tables to validate
    yaml_files = list(schema_dir.glob("*.yaml"))
    tables_to_validate = [yaml_file.stem for yaml_file in yaml_files]

    for table_name in tables_to_validate:
        if table_name not in ispypsa_tables:
            raise ValueError(
                f"Table '{table_name}' has a validation config but is not found in ispypsa_tables"
            )

        yaml_path = schema_dir / f"{table_name}.yaml"

        # Run validation using pointblank YAML
        validation = pb.yaml_interrogate(
            yaml=yaml_path, set_tbl=ispypsa_tables[table_name]
        )

        # Check if validation failed
        total_failures = sum(validation.n_failed().values())
        if total_failures > 0:
            raise ValueError(f"Validation failed for table '{table_name}'")

    # Run referential integrity checks defined in YAML files
    _check_referential_integrity_from_yaml(ispypsa_tables, schema_dir)


def _check_referential_integrity_from_yaml(
    tables: dict[str, pd.DataFrame], schema_dir: Path
) -> None:
    """
    Check cross-table referential integrity based on YAML schema definitions.

    Reads referential_integrity sections from YAML files and validates the relationships.

    Raises ValueError on first integrity violation.
    """
    # Find all YAML schema files
    yaml_files = list(schema_dir.glob("*.yaml"))

    for yaml_file in yaml_files:
        table_name = yaml_file.stem

        # Skip if this table is not in the dataset
        if table_name not in tables:
            continue

        # Load the YAML file
        with open(yaml_file, "r") as f:
            schema = yaml.safe_load(f)

        # Check if this schema has referential integrity definitions
        if "referential_integrity" not in schema:
            continue

        # Process each referential integrity check
        for check in schema["referential_integrity"]:
            _validate_referential_integrity_check(
                table_name, tables[table_name], check, tables
            )


def _validate_referential_integrity_check(
    source_table_name: str,
    source_table: pd.DataFrame,
    check: dict,
    all_tables: dict[str, pd.DataFrame],
) -> None:
    """
    Validate a single referential integrity check.

    Args:
        source_table_name: Name of the source table
        source_table: The source DataFrame
        check: Dictionary containing check definition from YAML
        all_tables: All available tables
    """
    source_column = check["source_column"]
    target_table_name = check["target_table"]
    target_column = check["target_column"]
    description = check.get("description", f"{source_column} referential integrity")

    # Check if target table exists
    if target_table_name not in all_tables:
        raise ValueError(
            f"Referential integrity check failed: target table '{target_table_name}' not found"
        )

    target_table = all_tables[target_table_name]

    # Check if columns exist
    if source_column not in source_table.columns:
        raise ValueError(
            f"Referential integrity check failed: column '{source_column}' not found in table '{source_table_name}'"
        )

    if target_column not in target_table.columns:
        raise ValueError(
            f"Referential integrity check failed: column '{target_column}' not found in table '{target_table_name}'"
        )

    # Perform the referential integrity check
    source_values = set(source_table[source_column])
    target_values = set(target_table[target_column])

    invalid_values = source_values - target_values
    if invalid_values:
        raise ValueError(
            f"Referential integrity violation: {source_table_name}.{source_column} contains "
            f"invalid values: {invalid_values}. {description}"
        )
