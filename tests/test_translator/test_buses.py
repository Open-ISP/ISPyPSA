from pathlib import Path

import pandas as pd

from ispypsa.translator.buses import (
    _create_single_region_bus,
    _translate_isp_sub_regions_to_buses,
    _translate_nem_regions_to_buses,
    _translate_rezs_to_buses,
    create_pypsa_friendly_bus_demand_timeseries,
)


def test_translate_isp_sub_regions_to_buses():
    isp_sub_regions = pd.DataFrame(
        columns=["isp_sub_region_id", "nem_region_id"],
        data=[["CNSW", "NSW"], ["SNSW", "NSW"]],
    )
    expected_buses = pd.DataFrame(columns=["name"], data=[["CNSW"], ["SNSW"]])
    buses = _translate_isp_sub_regions_to_buses(isp_sub_regions)
    pd.testing.assert_frame_equal(buses, expected_buses)


def test_translate_nem_regions_to_buses():
    nem_regions = pd.DataFrame(
        columns=["nem_region_id", "isp_sub_region_id"],
        data=[["NSW", "CNSW"], ["VIC", "VIC"]],
    )
    expected_buses = pd.DataFrame(columns=["name"], data=[["NSW"], ["VIC"]])
    buses = _translate_nem_regions_to_buses(nem_regions)
    pd.testing.assert_frame_equal(buses, expected_buses)


def test_create_single_region_bus():
    expected_buses = pd.DataFrame(columns=["name"], data=[["NEM"]])
    buses = _create_single_region_bus()
    pd.testing.assert_frame_equal(buses, expected_buses)


def test_translate_rezs_to_buses():
    rezs = pd.DataFrame(
        columns=["rez_id", "isp_sub_region_id"], data=[["X", "CNSW"], ["Y", "SNSW"]]
    )
    expected_buses = pd.DataFrame(columns=["name"], data=[["X"], ["Y"]])
    buses = _translate_rezs_to_buses(rezs)
    pd.testing.assert_frame_equal(buses, expected_buses)


def test_create_pypsa_friendly_bus_timeseries_data_sub_regions(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data/isp_2024")

    sub_regions_ispypsa = pd.DataFrame(
        {
            "isp_sub_region_id": ["NNSW", "SQ"],
            "nem_region_id": ["NSW", "QLD"],
        }
    )

    # Get demand traces - function no longer takes output path or snapshots
    demand_traces = create_pypsa_friendly_bus_demand_timeseries(
        sub_regions_ispypsa,
        parsed_trace_path,
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    # Build expected trace from the same source files
    files = [
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])
    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")
    # Don't rename columns or filter by snapshots - keep as raw data
    expected_trace = expected_trace.reset_index(drop=True)

    # The function returns a dictionary with node names as keys
    # For sub_regions granularity, CNSW should be its own node
    assert "CNSW" in demand_traces
    got_trace = demand_traces["CNSW"]

    # Compare the traces
    pd.testing.assert_frame_equal(expected_trace, got_trace)


def test_create_pypsa_friendly_bus_timeseries_data_nem_regions(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data/isp_2024")

    sub_regions_ispypsa = pd.DataFrame(
        {
            "isp_sub_region_id": ["NNSW", "SQ"],
            "nem_region_id": ["NSW", "QLD"],
        }
    )

    # Get demand traces - function no longer takes output path or snapshots
    demand_traces = create_pypsa_friendly_bus_demand_timeseries(
        sub_regions_ispypsa,
        parsed_trace_path,
        scenario="Step Change",
        regional_granularity="nem_regions",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    files = [
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])
    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")

    # For nem_regions, aggregate by Datetime to combine CNSW and NNSW into NSW
    expected_trace = expected_trace.groupby("Datetime", as_index=False).agg(
        {"Value": "sum"}
    )
    # Don't rename columns or filter by snapshots - keep as raw data
    expected_trace = expected_trace.reset_index(drop=True)

    # The function returns a dictionary with node names as keys
    # For nem_regions granularity, NSW should be aggregated from CNSW and NNSW
    assert "NSW" in demand_traces
    got_trace = demand_traces["NSW"]

    # Compare the traces
    pd.testing.assert_frame_equal(expected_trace, got_trace)


def test_create_pypsa_friendly_bus_timeseries_data_single_region(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data/isp_2024")

    sub_regions_ispypsa = pd.DataFrame(
        {
            "isp_sub_region_id": ["NNSW", "SQ"],
            "nem_region_id": ["NSW", "QLD"],
        }
    )

    # Get demand traces - function no longer takes output path or snapshots
    demand_traces = create_pypsa_friendly_bus_demand_timeseries(
        sub_regions_ispypsa,
        parsed_trace_path,
        scenario="Step Change",
        regional_granularity="single_region",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    files = [
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
        "demand/Step_Change/RefYear2011/SQ/POE50/OPSO_MODELLING/Step_Change_RefYear2011_SQ_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/SQ/POE50/OPSO_MODELLING/Step_Change_RefYear2011_SQ_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/SQ/POE50/OPSO_MODELLING/Step_Change_RefYear2018_SQ_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/SQ/POE50/OPSO_MODELLING/Step_Change_RefYear2018_SQ_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])
    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")

    # For single_region, aggregate all sub-regions into NEM
    expected_trace = expected_trace.groupby("Datetime", as_index=False).agg(
        {"Value": "sum"}
    )
    # Don't rename columns or filter by snapshots - keep as raw data
    expected_trace = expected_trace.reset_index(drop=True)

    # The function returns a dictionary with node names as keys
    # For single_region granularity, NEM should aggregate all sub-regions
    assert "NEM" in demand_traces
    got_trace = demand_traces["NEM"]

    # Compare the traces
    pd.testing.assert_frame_equal(expected_trace, got_trace)
