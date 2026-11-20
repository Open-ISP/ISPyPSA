from pathlib import Path

import pandas as pd

from ispypsa.translator.demand import (
    _create_demand_node_traces,
    _sum_subregion_traces_per_demand_node,
    _translate_demand_subregions_to_demand_nodes,
)

_TRACE_DATA_PATH = Path(__file__).parent.parent / "trace_data" / "isp_2024"


def _read_subregion_demand(subregion: str) -> pd.DataFrame:
    """Reads a sub-region's Step Change test trace for FY2025 and FY2026.

    Mirrors the reference_year_mapping {2025: 2011, 2026: 2018} the tests
    below pass: FY2025 comes from reference year 2011 and FY2026 from 2018.
    """
    demand_directory = _TRACE_DATA_PATH / "demand"
    trace_2011 = pd.read_parquet(
        demand_directory
        / f"Step_Change_RefYear2011_{subregion}_POE50_OPSO_MODELLING.parquet"
    )
    trace_2018 = pd.read_parquet(
        demand_directory
        / f"Step_Change_RefYear2018_{subregion}_POE50_OPSO_MODELLING.parquet"
    )
    in_fy2025 = (trace_2011["datetime"] > "2024-07-01") & (
        trace_2011["datetime"] <= "2025-07-01"
    )
    in_fy2026 = (trace_2018["datetime"] > "2025-07-01") & (
        trace_2018["datetime"] <= "2026-07-01"
    )
    trace = pd.concat([trace_2011[in_fy2025], trace_2018[in_fy2026]])
    trace["datetime"] = trace["datetime"].astype("datetime64[ns]")
    return trace[["datetime", "value"]].reset_index(drop=True)


def test_translate_demand_subregions_to_demand_nodes_sub_regions(csv_str_to_df):
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          CNSW
        NNSW,          NNSW
        SQ,            SQ
    """)

    result = _translate_demand_subregions_to_demand_nodes(demand_subregions)

    expected = csv_str_to_df("""
        name
        CNSW
        NNSW
        SQ
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_demand_subregions_to_demand_nodes_nem_regions(csv_str_to_df):
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NSW
        NNSW,          NSW
        SQ,            QLD
    """)

    result = _translate_demand_subregions_to_demand_nodes(demand_subregions)

    # CNSW and NNSW share the NSW bus, so NSW is listed once.
    expected = csv_str_to_df("""
        name
        NSW
        QLD
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_demand_subregions_to_demand_nodes_single_region(csv_str_to_df):
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NEM
        NNSW,          NEM
        SQ,            NEM
    """)

    result = _translate_demand_subregions_to_demand_nodes(demand_subregions)

    expected = csv_str_to_df("""
        name
        NEM
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_demand_subregions_to_demand_nodes_empty(csv_str_to_df):
    demand_subregions = pd.DataFrame(columns=["subregion_id", "geo_id"])

    result = _translate_demand_subregions_to_demand_nodes(demand_subregions)

    expected = csv_str_to_df("""
        name
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_sum_subregion_traces_per_demand_node(csv_str_to_df):
    subregion_traces = csv_str_to_df(
        """
        subregion,  datetime,               value
        CNSW,       2024-07-01__00:30:00,   5000.0
        NNSW,       2024-07-01__00:30:00,   500.0
        SQ,         2024-07-01__00:30:00,   -20.0
        CNSW,       2024-07-01__01:00:00,   4800.0
        NNSW,       2024-07-01__01:00:00,   -100.0
        SQ,         2024-07-01__01:00:00,   3400.0
        """,
        parse_dates=["datetime"],
    )
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NSW
        NNSW,          NSW
        SQ,            QLD
    """)

    result = _sum_subregion_traces_per_demand_node(subregion_traces, demand_subregions)

    # NSW is CNSW + NNSW at each datetime (5000 + 500, then 4800 - 100): only
    # the node total is floored, so NNSW's -100 still nets off CNSW. QLD is SQ
    # alone, whose -20 is floored at zero.
    expected = csv_str_to_df(
        """
        geo_id,  datetime,               value
        NSW,     2024-07-01__00:30:00,   5500.0
        NSW,     2024-07-01__01:00:00,   4700.0
        QLD,     2024-07-01__00:30:00,   0.0
        QLD,     2024-07-01__01:00:00,   3400.0
        """,
        parse_dates=["datetime"],
    )
    expected["datetime"] = expected["datetime"].astype("datetime64[ns]")
    pd.testing.assert_frame_equal(result, expected)


def test_sum_subregion_traces_per_demand_node_no_traces(csv_str_to_df):
    subregion_traces = pd.DataFrame(
        {
            "subregion": pd.Series(dtype="object"),
            "datetime": pd.Series(dtype="datetime64[us]"),
            "value": pd.Series(dtype="float64"),
        }
    )
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NSW
    """)

    result = _sum_subregion_traces_per_demand_node(subregion_traces, demand_subregions)

    expected = csv_str_to_df("""
        geo_id,  datetime,  value
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_create_demand_node_traces_sub_regions(csv_str_to_df):
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          CNSW
        NNSW,          NNSW
        SQ,            SQ
    """)

    result = _create_demand_node_traces(
        demand_subregions,
        _TRACE_DATA_PATH,
        scenario="Step Change",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    # Each sub-region is its own demand node and keeps its own trace.
    assert sorted(result) == ["CNSW", "NNSW", "SQ"]
    pd.testing.assert_frame_equal(result["CNSW"], _read_subregion_demand("CNSW"))
    pd.testing.assert_frame_equal(result["SQ"], _read_subregion_demand("SQ"))

    # The NNSW test trace dips below zero, and a node's demand is floored at zero.
    expected_nnsw = _read_subregion_demand("NNSW")
    expected_nnsw["value"] = expected_nnsw["value"].clip(lower=0.0)
    pd.testing.assert_frame_equal(result["NNSW"], expected_nnsw)


def test_create_demand_node_traces_nem_regions(csv_str_to_df):
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NSW
        NNSW,          NSW
        SQ,            QLD
    """)

    result = _create_demand_node_traces(
        demand_subregions,
        _TRACE_DATA_PATH,
        scenario="Step Change",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    assert sorted(result) == ["NSW", "QLD"]

    # NSW is the sum of the CNSW and NNSW traces; QLD is SQ alone.
    expected_nsw = _read_subregion_demand("CNSW")
    expected_nsw["value"] = (
        expected_nsw["value"] + _read_subregion_demand("NNSW")["value"]
    )
    pd.testing.assert_frame_equal(result["NSW"], expected_nsw)

    pd.testing.assert_frame_equal(result["QLD"], _read_subregion_demand("SQ"))


def test_create_demand_node_traces_single_region(csv_str_to_df):
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NEM
        NNSW,          NEM
        SQ,            NEM
    """)

    result = _create_demand_node_traces(
        demand_subregions,
        _TRACE_DATA_PATH,
        scenario="Step Change",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    assert sorted(result) == ["NEM"]

    # NEM is the sum of all three sub-region traces.
    expected_nem = _read_subregion_demand("CNSW")
    expected_nem["value"] = (
        expected_nem["value"]
        + _read_subregion_demand("NNSW")["value"]
        + _read_subregion_demand("SQ")["value"]
    )
    pd.testing.assert_frame_equal(result["NEM"], expected_nem)


def test_create_demand_node_traces_logs_subregions_without_traces(
    csv_str_to_df, caplog
):
    # The test trace data has no CQ or NQ traces, so QLD's demand is SQ alone.
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        SQ,            QLD
        NQ,            QLD
        CQ,            QLD
    """)

    with caplog.at_level("WARNING"):
        result = _create_demand_node_traces(
            demand_subregions,
            _TRACE_DATA_PATH,
            scenario="Step Change",
            reference_year_mapping={2025: 2011, 2026: 2018},
            year_type="fy",
        )

    assert (
        "ISP sub-regions with no demand trace data (their demand is left out of "
        "the model): ['CQ', 'NQ']"
    ) in caplog.text
    assert sorted(result) == ["QLD"]
    pd.testing.assert_frame_equal(result["QLD"], _read_subregion_demand("SQ"))


def test_create_demand_node_traces_no_log_when_every_subregion_has_a_trace(
    csv_str_to_df, caplog
):
    demand_subregions = csv_str_to_df("""
        subregion_id,  geo_id
        SQ,            QLD
    """)

    with caplog.at_level("WARNING"):
        _create_demand_node_traces(
            demand_subregions,
            _TRACE_DATA_PATH,
            scenario="Step Change",
            reference_year_mapping={2025: 2011, 2026: 2018},
            year_type="fy",
        )

    assert "no demand trace data" not in caplog.text


def test_create_demand_node_traces_no_subregions():
    demand_subregions = pd.DataFrame(columns=["subregion_id", "geo_id"])

    result = _create_demand_node_traces(
        demand_subregions,
        _TRACE_DATA_PATH,
        scenario="Step Change",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    assert result == {}
