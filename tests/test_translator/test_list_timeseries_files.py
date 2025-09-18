"""Tests for list_timeseries_files function.

Test coverage:
- Regional granularities:
  - test_list_timeseries_files_sub_regions: sub_regions granularity
  - test_list_timeseries_files_nem_regions: nem_regions granularity
  - test_list_timeseries_files_single_region: single_region granularity
- Fuel types handled as expected
  - test_list_timeseries_files_mixed_fuel_types: Filtering Solar/Wind from mixed fuel types
- Error handling:
  - test_list_timeseries_files_missing_ecaa_generators_table: KeyError for missing ecaa_generators
  - test_list_timeseries_files_missing_sub_regions_table: KeyError for missing sub_regions
  - test_list_timeseries_files_missing_generator_columns: ValueError for missing columns in ecaa_generators
  - test_list_timeseries_files_missing_sub_region_id_column: ValueError for missing isp_sub_region_id
  - test_list_timeseries_files_missing_nem_region_id_column: ValueError for missing nem_region_id
- Edge cases:
  - test_list_timeseries_files_empty_tables: Empty generator tables
"""

from pathlib import Path

import pandas as pd
import pytest

from ispypsa.translator.create_pypsa_friendly_inputs import list_timeseries_files


class MockNetworkConfig:
    def __init__(self, regional_granularity):
        self.nodes = type(
            "obj", (object,), {"regional_granularity": regional_granularity}
        )


class MockConfig:
    def __init__(self, regional_granularity):
        self.network = MockNetworkConfig(regional_granularity)


def test_list_timeseries_files_sub_regions(csv_str_to_df):
    """Test list_timeseries_files with sub_regions granularity."""

    # Create test ecaa_generators data
    ecaa_generators_csv = """
    generator,      fuel_type
    NSW_Solar_1,    Solar
    NSW_Solar_2,    Solar
    VIC_Wind_1,     Wind
    VIC_Wind_2,     Wind
    QLD_Solar_1,    Solar
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Create test sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,      nem_region_id
    NSW_North,              NSW
    NSW_South,              NSW
    VIC_East,               VIC
    QLD_Central,            QLD
    """
    sub_regions = csv_str_to_df(sub_regions_csv)

    # Create tables dictionary
    ispypsa_tables = {
        "ecaa_generators": ecaa_generators,
        "sub_regions": sub_regions,
    }

    # Create mock config with sub_regions granularity
    config = MockConfig("sub_regions")

    # Define output base path
    output_base_path = Path("/test/path")

    # Call the function
    result = list_timeseries_files(config, ispypsa_tables, output_base_path)

    # Expected files
    expected_files = [
        # Solar generator files
        Path("/test/path/solar_traces/NSW_Solar_1.parquet"),
        Path("/test/path/solar_traces/NSW_Solar_2.parquet"),
        Path("/test/path/solar_traces/QLD_Solar_1.parquet"),
        # Wind generator files
        Path("/test/path/wind_traces/VIC_Wind_1.parquet"),
        Path("/test/path/wind_traces/VIC_Wind_2.parquet"),
        # Demand trace files for sub_regions
        Path("/test/path/demand_traces/NSW_North.parquet"),
        Path("/test/path/demand_traces/NSW_South.parquet"),
        Path("/test/path/demand_traces/VIC_East.parquet"),
        Path("/test/path/demand_traces/QLD_Central.parquet"),
    ]

    # Sort both lists for comparison
    assert sorted(result) == sorted(expected_files)


def test_list_timeseries_files_nem_regions(csv_str_to_df):
    """Test list_timeseries_files with nem_regions granularity."""

    # Create test ecaa_generators data
    ecaa_generators_csv = """
    generator,      fuel_type
    NSW_Solar_1,    Solar
    VIC_Wind_1,     Wind
    SA_Wind_1,      Wind
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Create test sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,      nem_region_id
    NSW_North,              NSW
    NSW_South,              NSW
    VIC_East,               VIC
    SA_Central,             SA
    SA_North,               SA
    """
    sub_regions = csv_str_to_df(sub_regions_csv)

    # Create tables dictionary
    ispypsa_tables = {
        "ecaa_generators": ecaa_generators,
        "sub_regions": sub_regions,
    }

    # Create mock config with nem_regions granularity
    config = MockConfig("nem_regions")

    # Define output base path
    output_base_path = Path("/test/path")

    # Call the function
    result = list_timeseries_files(config, ispypsa_tables, output_base_path)

    # Expected files
    expected_files = [
        # Solar generator files
        Path("/test/path/solar_traces/NSW_Solar_1.parquet"),
        # Wind generator files
        Path("/test/path/wind_traces/VIC_Wind_1.parquet"),
        Path("/test/path/wind_traces/SA_Wind_1.parquet"),
        # Demand trace files for nem_regions (unique regions only)
        Path("/test/path/demand_traces/NSW.parquet"),
        Path("/test/path/demand_traces/VIC.parquet"),
        Path("/test/path/demand_traces/SA.parquet"),
    ]

    # Sort both lists for comparison
    assert sorted(result) == sorted(expected_files)


def test_list_timeseries_files_single_region(csv_str_to_df):
    """Test list_timeseries_files with single_region granularity."""

    # Create test ecaa_generators data
    ecaa_generators_csv = """
    generator,      fuel_type
    Solar_Gen_1,    Solar
    Solar_Gen_2,    Solar
    Wind_Gen_1,     Wind
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Create test sub_regions data (still needed even for single_region)
    sub_regions_csv = """
    isp_sub_region_id,      nem_region_id
    NSW_North,              NSW
    VIC_East,               VIC
    """
    sub_regions = csv_str_to_df(sub_regions_csv)

    # Create tables dictionary
    ispypsa_tables = {
        "ecaa_generators": ecaa_generators,
        "sub_regions": sub_regions,
    }

    # Create mock config with single_region granularity
    config = MockConfig("single_region")

    # Define output base path
    output_base_path = Path("/test/path")

    # Call the function
    result = list_timeseries_files(config, ispypsa_tables, output_base_path)

    # Expected files
    expected_files = [
        # Solar generator files
        Path("/test/path/solar_traces/Solar_Gen_1.parquet"),
        Path("/test/path/solar_traces/Solar_Gen_2.parquet"),
        # Wind generator files
        Path("/test/path/wind_traces/Wind_Gen_1.parquet"),
        # Single demand trace file for entire NEM
        Path("/test/path/demand_traces/NEM.parquet"),
    ]

    # Sort both lists for comparison
    assert sorted(result) == sorted(expected_files)


def test_list_timeseries_files_missing_ecaa_generators_table():
    """Test list_timeseries_files raises KeyError when ecaa_generators table is missing."""

    # Create tables dictionary without ecaa_generators
    ispypsa_tables = {
        "sub_regions": pd.DataFrame(
            {"isp_sub_region_id": ["NSW_North"], "nem_region_id": ["NSW"]}
        ),
    }

    # Create mock config
    config = MockConfig("sub_regions")
    output_base_path = Path("/test/path")

    # Expect KeyError
    with pytest.raises(
        KeyError, match="Missing required ISPyPSA table: 'ecaa_generators'"
    ):
        list_timeseries_files(config, ispypsa_tables, output_base_path)


def test_list_timeseries_files_missing_sub_regions_table():
    """Test list_timeseries_files raises KeyError when sub_regions table is missing."""

    # Create tables dictionary without sub_regions
    ispypsa_tables = {
        "ecaa_generators": pd.DataFrame(
            {"generator": ["Gen1"], "fuel_type": ["Solar"]}
        ),
    }

    # Create mock config
    config = MockConfig("sub_regions")
    output_base_path = Path("/test/path")

    # Expect KeyError
    with pytest.raises(KeyError, match="Missing required ISPyPSA table: 'sub_regions'"):
        list_timeseries_files(config, ispypsa_tables, output_base_path)


def test_list_timeseries_files_missing_generator_columns():
    """Test list_timeseries_files raises ValueError when required columns are missing from ecaa_generators."""

    # Create ecaa_generators without required columns
    ispypsa_tables = {
        "ecaa_generators": pd.DataFrame(
            {"some_column": ["value"]}
        ),  # Missing 'generator' and 'fuel_type'
        "sub_regions": pd.DataFrame(
            {"isp_sub_region_id": ["NSW_North"], "nem_region_id": ["NSW"]}
        ),
    }

    # Create mock config
    config = MockConfig("sub_regions")
    output_base_path = Path("/test/path")

    # Expect ValueError
    with pytest.raises(
        ValueError,
        match="Missing required columns in 'ecaa_generators' table: \\['generator', 'fuel_type'\\]",
    ):
        list_timeseries_files(config, ispypsa_tables, output_base_path)


def test_list_timeseries_files_missing_sub_region_id_column():
    """Test list_timeseries_files raises ValueError when isp_sub_region_id is missing for sub_regions granularity."""

    # Create sub_regions without isp_sub_region_id column
    ispypsa_tables = {
        "ecaa_generators": pd.DataFrame(
            {"generator": ["Gen1"], "fuel_type": ["Solar"]}
        ),
        "sub_regions": pd.DataFrame(
            {"nem_region_id": ["NSW"]}
        ),  # Missing 'isp_sub_region_id'
    }

    # Create mock config with sub_regions granularity
    config = MockConfig("sub_regions")
    output_base_path = Path("/test/path")

    # Expect ValueError
    with pytest.raises(
        ValueError,
        match="Missing required column 'isp_sub_region_id' in 'sub_regions' table for sub_regions granularity",
    ):
        list_timeseries_files(config, ispypsa_tables, output_base_path)


def test_list_timeseries_files_missing_nem_region_id_column():
    """Test list_timeseries_files raises ValueError when nem_region_id is missing for nem_regions granularity."""

    # Create sub_regions without nem_region_id column
    ispypsa_tables = {
        "ecaa_generators": pd.DataFrame(
            {"generator": ["Gen1"], "fuel_type": ["Solar"]}
        ),
        "sub_regions": pd.DataFrame(
            {"isp_sub_region_id": ["NSW_North"]}
        ),  # Missing 'nem_region_id'
    }

    # Create mock config with nem_regions granularity
    config = MockConfig("nem_regions")
    output_base_path = Path("/test/path")

    # Expect ValueError
    with pytest.raises(
        ValueError,
        match="Missing required column 'nem_region_id' in 'sub_regions' table for nem_regions granularity",
    ):
        list_timeseries_files(config, ispypsa_tables, output_base_path)


def test_list_timeseries_files_empty_tables(csv_str_to_df):
    """Test list_timeseries_files with empty tables returns empty lists for generators but still creates demand files."""

    # Create empty ecaa_generators data
    ecaa_generators_csv = """
    generator,      fuel_type
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Create sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,      nem_region_id
    NSW_North,              NSW
    """
    sub_regions = csv_str_to_df(sub_regions_csv)

    # Create tables dictionary
    ispypsa_tables = {
        "ecaa_generators": ecaa_generators,
        "sub_regions": sub_regions,
    }

    # Create mock config
    config = MockConfig("sub_regions")
    output_base_path = Path("/test/path")

    # Call the function
    result = list_timeseries_files(config, ispypsa_tables, output_base_path)

    # Expected files - only demand trace since no generators
    expected_files = [
        Path("/test/path/demand_traces/NSW_North.parquet"),
    ]

    assert result == expected_files


def test_list_timeseries_files_mixed_fuel_types(csv_str_to_df):
    """Test list_timeseries_files correctly filters generators by fuel type."""

    # Create ecaa_generators with various fuel types
    ecaa_generators_csv = """
    generator,      fuel_type
    NSW_Solar_1,    Solar
    NSW_Gas_1,      Gas
    VIC_Wind_1,     Wind
    VIC_Coal_1,     Black Coal
    SA_Battery_1,   Battery
    TAS_Wind_1,     Wind
    QLD_Solar_1,    Solar
    """
    ecaa_generators = csv_str_to_df(ecaa_generators_csv)

    # Create sub_regions data
    sub_regions_csv = """
    isp_sub_region_id,      nem_region_id
    NSW_North,              NSW
    """
    sub_regions = csv_str_to_df(sub_regions_csv)

    # Create tables dictionary
    ispypsa_tables = {
        "ecaa_generators": ecaa_generators,
        "sub_regions": sub_regions,
    }

    # Create mock config
    config = MockConfig("sub_regions")
    output_base_path = Path("/test/path")

    # Call the function
    result = list_timeseries_files(config, ispypsa_tables, output_base_path)

    # Expected files - only Solar and Wind generators should be included
    expected_files = [
        # Solar generators
        Path("/test/path/solar_traces/NSW_Solar_1.parquet"),
        Path("/test/path/solar_traces/QLD_Solar_1.parquet"),
        # Wind generators
        Path("/test/path/wind_traces/VIC_Wind_1.parquet"),
        Path("/test/path/wind_traces/TAS_Wind_1.parquet"),
        # Demand traces
        Path("/test/path/demand_traces/NSW_North.parquet"),
    ]

    # Sort both lists for comparison
    assert sorted(result) == sorted(expected_files)
