from pathlib import Path

import pandas as pd

from ispypsa.translator.buses import (
    _create_single_region_bus,
    _translate_isp_sub_regions_to_buses,
    _translate_nem_regions_to_buses,
    _translate_rezs_to_buses,
    create_pypsa_friendly_bus_demand_timeseries,
)
from ispypsa.translator.snapshots import (
    _add_investment_periods,
    _create_complete_snapshots_index,
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
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data")

    sub_regions_ispypsa = pd.DataFrame(
        {
            "isp_sub_region_id": ["CNSW", "NNSW", "CQ", "NQ"],
            "nem_region_id": ["NSW", "NSW", "QLD", "QLD"],
        }
    )

    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2026,
        temporal_resolution_min=30,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    create_pypsa_friendly_bus_demand_timeseries(
        sub_regions_ispypsa,
        parsed_trace_path,
        tmp_path,
        scenario="Step Change",
        regional_granularity="sub_regions",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
        snapshots=snapshots,
    )

    files = [
        "demand/Step_Change/RefYear2011/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])
    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")
    expected_trace = expected_trace.rename(
        columns={"Datetime": "snapshots", "Value": "p_set"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[:, ["investment_periods", "snapshots", "p_set"]]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(tmp_path / Path("demand_traces/CNSW.parquet"))

    pd.testing.assert_frame_equal(expected_trace, got_trace)


def test_create_pypsa_friendly_bus_timeseries_data_nem_regions(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data")

    sub_regions_ispypsa = pd.DataFrame(
        {
            "isp_sub_region_id": ["CNSW", "NNSW", "CQ", "NQ"],
            "nem_region_id": ["NSW", "NSW", "QLD", "QLD"],
        }
    )

    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2026,
        temporal_resolution_min=30,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    create_pypsa_friendly_bus_demand_timeseries(
        sub_regions_ispypsa,
        parsed_trace_path,
        tmp_path,
        scenario="Step Change",
        regional_granularity="nem_regions",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
        snapshots=snapshots,
    )

    files = [
        "demand/Step_Change/RefYear2011/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])

    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")

    expected_trace = expected_trace.groupby("Datetime", as_index=False).agg(
        {"Value": "sum"}
    )
    expected_trace = expected_trace.rename(
        columns={"Datetime": "snapshots", "Value": "p_set"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[:, ["investment_periods", "snapshots", "p_set"]]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(tmp_path / Path("demand_traces/NSW.parquet"))

    pd.testing.assert_frame_equal(expected_trace, got_trace)


def test_create_pypsa_friendly_bus_timeseries_data_single_region(tmp_path):
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data")

    sub_regions_ispypsa = pd.DataFrame(
        {
            "isp_sub_region_id": ["CNSW", "NNSW", "CQ", "NQ"],
            "nem_region_id": ["NSW", "NSW", "QLD", "QLD"],
        }
    )

    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2026,
        temporal_resolution_min=30,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    create_pypsa_friendly_bus_demand_timeseries(
        sub_regions_ispypsa,
        parsed_trace_path,
        tmp_path,
        scenario="Step Change",
        regional_granularity="single_region",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
        snapshots=snapshots,
    )

    files = [
        "demand/Step_Change/RefYear2011/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/CNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NNSW_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/NNSW/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NNSW_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
        "demand/Step_Change/RefYear2011/CQ/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CQ_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/CQ/POE50/OPSO_MODELLING/Step_Change_RefYear2011_CQ_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/CQ/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CQ_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/CQ/POE50/OPSO_MODELLING/Step_Change_RefYear2018_CQ_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
        "demand/Step_Change/RefYear2011/NQ/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NQ_POE50_OPSO_MODELLING_HalfYear2024-2.parquet",
        "demand/Step_Change/RefYear2011/NQ/POE50/OPSO_MODELLING/Step_Change_RefYear2011_NQ_POE50_OPSO_MODELLING_HalfYear2025-1.parquet",
        "demand/Step_Change/RefYear2018/NQ/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NQ_POE50_OPSO_MODELLING_HalfYear2025-2.parquet",
        "demand/Step_Change/RefYear2018/NQ/POE50/OPSO_MODELLING/Step_Change_RefYear2018_NQ_POE50_OPSO_MODELLING_HalfYear2026-1.parquet",
    ]

    files = [parsed_trace_path / Path(file) for file in files]

    expected_trace = pd.concat([pd.read_parquet(file) for file in files])

    expected_trace["Datetime"] = expected_trace["Datetime"].astype("datetime64[ns]")

    expected_trace = expected_trace.groupby("Datetime", as_index=False).agg(
        {"Value": "sum"}
    )
    expected_trace = expected_trace.rename(
        columns={"Datetime": "snapshots", "Value": "p_set"}
    )
    expected_trace = pd.merge(expected_trace, snapshots, on="snapshots")
    expected_trace = expected_trace.loc[:, ["investment_periods", "snapshots", "p_set"]]
    expected_trace = expected_trace.reset_index(drop=True)

    got_trace = pd.read_parquet(tmp_path / Path("demand_traces/NEM.parquet"))

    pd.testing.assert_frame_equal(expected_trace, got_trace)
