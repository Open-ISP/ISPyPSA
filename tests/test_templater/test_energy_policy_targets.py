from pathlib import Path

import pandas as pd

from ispypsa.data_fetch import read_csvs
from ispypsa.templater.energy_policy_targets import (
    template_powering_australia_plan,
    template_renewable_generation_targets,
    template_renewable_share_targets,
    template_technology_capacity_targets,
)
from ispypsa.templater.lists import _ISP_SCENARIOS
from ispypsa.templater.mappings import _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP


def test_template_renewable_share_targets(workbook_table_cache_test_path: Path):
    """Test the renewable share targets template creation"""

    iasr_tables = read_csvs(workbook_table_cache_test_path)

    df = template_renewable_share_targets(iasr_tables)

    # Check basic DataFrame structure
    expected_columns = ["FY", "region_id", "pct", "policy_id"]
    assert all(col in df.columns for col in expected_columns)

    # Check data types
    assert df["FY"].dtype == "object"  # String type
    assert df["region_id"].dtype == "object"  # String type
    assert df["policy_id"].dtype == "object"  # String type
    assert df["pct"].dtype == "float64"
    assert all(df["pct"].between(0, 100))

    # Check that FY format is correct (YYYY_YY)
    assert all(df["FY"].str.match(r"\d{4}_\d{2}"))

    # Check expected regions are present
    expected_regions = {"VIC", "QLD"}
    assert set(df["region_id"]) == expected_regions

    # Test specific known values (sample check)
    vic_2024 = df[(df["region_id"] == "VIC") & (df["FY"] == "2024_25")]["pct"].iloc[0]
    qld_2030 = df[(df["region_id"] == "QLD") & (df["FY"] == "2030_31")]["pct"].iloc[0]

    assert vic_2024 == 40
    assert qld_2030 == 60

    # test specific known values (sample check)
    vic_policy_2024 = df[(df["region_id"] == "VIC") & (df["FY"] == "2024_25")][
        "policy_id"
    ].iloc[0]
    qld_policy_2030 = df[(df["region_id"] == "QLD") & (df["FY"] == "2030_31")][
        "policy_id"
    ].iloc[0]

    assert vic_policy_2024 == "vret"
    assert qld_policy_2030 == "qret"


def test_template_powering_australia_plan(workbook_table_cache_test_path: Path):
    """Test the Powering Australia Plan template creation"""

    iasr_tables = read_csvs(workbook_table_cache_test_path)
    df_full = iasr_tables["powering_australia_plan_trajectory"]
    for scenario in _ISP_SCENARIOS:
        df = template_powering_australia_plan(df_full, scenario)

        # Check basic DataFrame structure
        expected_columns = ["FY", "pct", "policy_id"]
        assert all(col in df.columns for col in expected_columns)

        # Check data types
        assert df["FY"].dtype == "object"  # String type
        assert df["pct"].dtype == "float64"
        assert all(df["pct"].between(0, 100))

        # Check that FY format is correct (YYYY_YY)
        assert all(df["FY"].str.match(r"\d{4}_\d{2}"))

        assert not df.isnull().any().any()


def test_template_technology_capacity_targets(workbook_table_cache_test_path: Path):
    """Test the technology capacity targets template creation"""

    iasr_tables = read_csvs(workbook_table_cache_test_path)
    df = template_technology_capacity_targets(iasr_tables)

    # Check basic DataFrame structure
    expected_columns = ["FY", "region_id", "capacity_mw", "policy_id"]
    assert all(col in df.columns for col in expected_columns)

    # Check data types
    assert df["FY"].dtype == "object"  # String type
    assert df["region_id"].dtype == "object"  # String type
    assert df["capacity_mw"].dtype == "float64"
    assert df["policy_id"].dtype == "object"  # String type

    # Check that capacity values are non-negative
    assert all(df["capacity_mw"] >= 0)

    # Check that FY format is correct (YYYY_YY)
    assert all(df["FY"].str.match(r"\d{4}_\d{2}"))

    # Check expected technologies are present
    target_files = _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP[
        "template_technology_capacity_targets"
    ]
    expected_policy_ids = {target["policy_id"] for target in target_files}
    assert set(df["policy_id"]) == expected_policy_ids

    # Test specific known values (sample check)
    vic_storage_2024 = df[
        (df["region_id"] == "VIC")
        & (df["policy_id"] == "vic_storage")
        & (df["FY"] == "2028_29")
    ]["capacity_mw"].iloc[0]
    nem_generator_2030 = df[
        (df["region_id"] == "NEM")
        & (df["policy_id"] == "cis_generator")
        & (df["FY"] == "2026_27")
    ]["capacity_mw"].iloc[0]

    assert vic_storage_2024 == 1950.0
    assert nem_generator_2030 == 4000.0

    # Check sorting
    assert df.equals(
        df.sort_values(["region_id", "policy_id", "FY"]).reset_index(drop=True)
    )


def test_template_renewable_generation_targets(workbook_table_cache_test_path: Path):
    """Test the renewable generation targets template creation"""
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    df = template_renewable_generation_targets(iasr_tables)

    # Check basic DataFrame structure
    expected_columns = ["FY", "region_id", "capacity_mwh"]
    assert all(col in df.columns for col in expected_columns)

    # Check data types
    assert df["FY"].dtype == "object"  # String type
    assert df["region_id"].dtype == "object"  # String type
    assert df["capacity_mwh"].dtype == "float64"
    assert df["policy_id"].dtype == "object"  # String type

    # Check that capacity values are non-negative
    assert all(df["capacity_mwh"] >= 0)

    # Check that FY format is correct (YYYY_YY)
    assert all(df["FY"].str.match(r"\d{4}_\d{2}"))

    # Test specific known values (sample check)
    nsw_2024 = df[(df["region_id"] == "NSW") & (df["FY"] == "2024_25")][
        "capacity_mwh"
    ].iloc[0]
    qld_2033 = df[(df["region_id"] == "TAS") & (df["FY"] == "2033_34")][
        "capacity_mwh"
    ].iloc[0]

    assert nsw_2024 == 12898000.0
    assert qld_2033 == 17850000.0

    # Verify no "Notes" rows in output
    assert not df["FY"].str.contains("Notes", case=False).any()


workbook_table_cache_test_path = Path("tests/test_workbook_table_cache")
test_template_renewable_share_targets(workbook_table_cache_test_path)
