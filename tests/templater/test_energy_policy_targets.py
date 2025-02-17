from pathlib import Path

from ispypsa.templater.energy_policy_targets import (
    template_powering_australia_plan,
    template_renewable_generation_targets,
    template_renewable_share_targets,
    template_technology_capacity_targets,
)
from ispypsa.templater.mappings import _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP


def test_template_renewable_share_targets(workbook_table_cache_test_path: Path):
    """Test the renewable share targets template creation"""

    df = template_renewable_share_targets(workbook_table_cache_test_path)

    # Check basic DataFrame structure
    expected_columns = ["FY", "region_id", "pct"]
    assert all(col in df.columns for col in expected_columns)

    # Check data types
    assert df["FY"].dtype == "object"  # String type
    assert df["region_id"].dtype == "object"  # String type
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


def test_template_powering_australia_plan(workbook_table_cache_test_path: Path):
    """Test the Powering Australia Plan template creation"""
    df = template_powering_australia_plan(workbook_table_cache_test_path)

    # Check basic DataFrame structure
    expected_columns = ["FY", "scenario", "pct"]
    assert all(col in df.columns for col in expected_columns)

    # Check data types
    assert df["FY"].dtype == "object"  # String type
    assert df["scenario"].dtype == "object"  # String type
    assert df["pct"].dtype == "float64"
    assert all(df["pct"].between(0, 100))

    # Check that FY format is correct (YYYY_YY)
    assert all(df["FY"].str.match(r"\d{4}_\d{2}"))

    assert not df.isnull().any().any()

    # Test specific known values (sample check)
    prog_2024 = df[(df["scenario"] == "Progressive Change") & (df["FY"] == "2027_28")][
        "pct"
    ].iloc[0]
    step_2030 = df[(df["scenario"] == "Step Change") & (df["FY"] == "2029_30")][
        "pct"
    ].iloc[0]

    assert prog_2024 == 63
    assert step_2030 == 82

    assert not df["scenario"].str.contains("Notes", case=False).any()


def test_template_technology_capacity_targets(workbook_table_cache_test_path: Path):
    """Test the technology capacity targets template creation"""
    df = template_technology_capacity_targets(workbook_table_cache_test_path)

    # Check basic DataFrame structure
    expected_columns = ["FY", "region_id", "capacity_mw", "technology"]
    assert all(col in df.columns for col in expected_columns)

    # Check data types
    assert df["FY"].dtype == "object"  # String type
    assert df["region_id"].dtype == "object"  # String type
    assert df["capacity_mw"].dtype == "float64"
    assert df["technology"].dtype == "object"  # String type

    # Check that capacity values are non-negative
    assert all(df["capacity_mw"] >= 0)

    # Check that FY format is correct (YYYY_YY)
    assert all(df["FY"].str.match(r"\d{4}_\d{2}"))

    # Check expected technologies are present
    target_files = _TEMPLATE_RENEWABLE_ENERGY_TARGET_MAP[
        "template_technology_capacity_targets"
    ]
    expected_technologies = {target["technology_type"] for target in target_files}
    assert set(df["technology"]) == expected_technologies

    # Test specific known values (sample check)
    vic_storage_2024 = df[
        (df["region_id"] == "VIC")
        & (df["technology"] == "storage")
        & (df["FY"] == "2028_29")
    ]["capacity_mw"].iloc[0]
    nsw_wind_2030 = df[
        (df["region_id"] == "NSW")
        & (df["technology"] == "storage")
        & (df["FY"] == "2023_24")
    ]["capacity_mw"].iloc[0]

    assert vic_storage_2024 == 1950.0
    assert nsw_wind_2030 == 0.0

    # Check sorting
    assert df.equals(
        df.sort_values(["technology", "region_id", "FY"]).reset_index(drop=True)
    )


def test_template_renewable_generation_targets(workbook_table_cache_test_path: Path):
    """Test the renewable generation targets template creation"""
    df = template_renewable_generation_targets(workbook_table_cache_test_path)

    # Check basic DataFrame structure
    expected_columns = ["FY", "region_id", "capacity_mwh"]
    assert all(col in df.columns for col in expected_columns)

    # Check data types
    assert df["FY"].dtype == "object"  # String type
    assert df["region_id"].dtype == "object"  # String type
    assert df["capacity_mwh"].dtype == "float64"

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

    empty_df = template_renewable_generation_targets(Path("non_existent_path"))
    assert list(empty_df.columns) == expected_columns
    assert len(empty_df) == 0

    # Verify no "Notes" rows in output
    assert not df["FY"].str.contains("Notes", case=False).any()
