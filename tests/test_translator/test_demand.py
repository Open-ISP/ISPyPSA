from pathlib import Path

import pandas as pd

from ispypsa.translator.demand import (
    _create_demand_node_traces,
    _place_subregions_at_model_geographies,
    _sum_subregion_traces_per_demand_node,
    _translate_demand_nodes,
)

_TRACE_DATA_PATH = Path(__file__).parent.parent / "trace_data" / "isp_2024"

_HIERARCHY_COLUMNS = ["geo_id", "geo_type", "subregion_id", "region_id", "nem_id"]
_GEOGRAPHY_COLUMNS = ["geo_id", "geo_type"]


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


def _network_geography_hierarchy(csv_str_to_df) -> pd.DataFrame:
    """Five sub-regions across two NEM regions, plus a REZ. The test trace
    data has traces for CNSW, NNSW and SQ, and none for CQ or NQ."""
    return csv_str_to_df("""
        geo_id,  geo_type,   subregion_id,  region_id,  nem_id
        NEM,     nem,        ,              ,           NEM
        NSW,     region,     ,              NSW,        NEM
        QLD,     region,     ,              QLD,        NEM
        CNSW,    subregion,  CNSW,          NSW,        NEM
        NNSW,    subregion,  NNSW,          NSW,        NEM
        SQ,      subregion,  SQ,            QLD,        NEM
        CQ,      subregion,  CQ,            QLD,        NEM
        NQ,      subregion,  NQ,            QLD,        NEM
        N3,      rez,        CNSW,          NSW,        NEM
    """)


def test_place_subregions_at_model_geographies_sub_regions(csv_str_to_df):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        CNSW,    subregion
        NNSW,    subregion
        SQ,      subregion
        N3,      rez
    """)

    result = _place_subregions_at_model_geographies(hierarchy, network_geography)

    # Each sub-region in the model is placed at itself. CQ and NQ are not in
    # the model and neither are QLD or NEM, so they have no row.
    expected = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          CNSW
        NNSW,          NNSW
        SQ,            SQ
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_place_subregions_at_model_geographies_nem_regions(csv_str_to_df):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        NSW,     region
        QLD,     region
        N3,      rez
    """)

    result = _place_subregions_at_model_geographies(hierarchy, network_geography)

    # No sub-region is in the model, so each is placed at its region_id.
    expected = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NSW
        NNSW,          NSW
        SQ,            QLD
        CQ,            QLD
        NQ,            QLD
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("subregion_id").reset_index(drop=True),
        expected.sort_values("subregion_id").reset_index(drop=True),
    )


def test_place_subregions_at_model_geographies_single_region(csv_str_to_df):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        NEM,     region
        N3,      rez
    """)

    result = _place_subregions_at_model_geographies(hierarchy, network_geography)

    # Only nem_id names a model geography, so every sub-region is placed at NEM.
    expected = csv_str_to_df("""
        subregion_id,  geo_id
        CNSW,          NEM
        NNSW,          NEM
        SQ,            NEM
        CQ,            NEM
        NQ,            NEM
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("subregion_id").reset_index(drop=True),
        expected.sort_values("subregion_id").reset_index(drop=True),
    )


def test_place_subregions_at_model_geographies_subregion_that_is_also_a_region(
    csv_str_to_df,
):
    hierarchy = csv_str_to_df("""
        geo_id,  geo_type,   subregion_id,  region_id,  nem_id
        TAS,     subregion,  TAS,           TAS,        NEM
        SQ,      subregion,  SQ,            QLD,        NEM
    """)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        TAS,     region
        QLD,     region
    """)

    result = _place_subregions_at_model_geographies(hierarchy, network_geography)

    # TAS matches the model geography TAS through both its subregion_id and
    # its region_id, but is placed there once, so its demand is not doubled.
    expected = csv_str_to_df("""
        subregion_id,  geo_id
        TAS,           TAS
        SQ,            QLD
    """)
    pd.testing.assert_frame_equal(
        result.sort_values("subregion_id").reset_index(drop=True),
        expected.sort_values("subregion_id").reset_index(drop=True),
    )


def test_place_subregions_at_model_geographies_empty_hierarchy(csv_str_to_df):
    hierarchy = pd.DataFrame(columns=_HIERARCHY_COLUMNS)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        NSW,     region
    """)

    result = _place_subregions_at_model_geographies(hierarchy, network_geography)

    expected = csv_str_to_df("""
        subregion_id,  geo_id
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_place_subregions_at_model_geographies_empty_geography(csv_str_to_df):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    result = _place_subregions_at_model_geographies(hierarchy, network_geography)

    expected = csv_str_to_df("""
        subregion_id,  geo_id
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_place_subregions_at_model_geographies_both_empty(csv_str_to_df):
    hierarchy = pd.DataFrame(columns=_HIERARCHY_COLUMNS)
    network_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    result = _place_subregions_at_model_geographies(hierarchy, network_geography)

    expected = csv_str_to_df("""
        subregion_id,  geo_id
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_translate_demand_nodes(csv_str_to_df):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        NSW,     region
        QLD,     region
        N3,      rez
    """)

    result = _translate_demand_nodes(hierarchy, network_geography)

    # CNSW and NNSW share the NSW bus, so NSW is listed once. The REZ N3 has
    # no sub-region inside it, so it carries no demand.
    expected = csv_str_to_df("""
        name
        NSW
        QLD
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_demand_nodes_empty(csv_str_to_df):
    hierarchy = pd.DataFrame(columns=_HIERARCHY_COLUMNS)
    network_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    result = _translate_demand_nodes(hierarchy, network_geography)

    expected = csv_str_to_df("""
        name
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_sum_subregion_traces_per_demand_node(csv_str_to_df):
    subregion_traces = csv_str_to_df(
        """
        subregion,  datetime,              value
        CNSW,       2024-07-01 00:30:00,   5000.0
        NNSW,       2024-07-01 00:30:00,   500.0
        SQ,         2024-07-01 00:30:00,   -20.0
        CNSW,       2024-07-01 01:00:00,   4800.0
        NNSW,       2024-07-01 01:00:00,   -100.0
        SQ,         2024-07-01 01:00:00,   3400.0
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
        geo_id,  datetime,              value
        NSW,     2024-07-01 00:30:00,   5500.0
        NSW,     2024-07-01 01:00:00,   4700.0
        QLD,     2024-07-01 00:30:00,   0.0
        QLD,     2024-07-01 01:00:00,   3400.0
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


def test_sum_subregion_traces_per_demand_node_no_demand_subregions(csv_str_to_df):
    subregion_traces = csv_str_to_df(
        """
        subregion,  datetime,              value
        CNSW,       2024-07-01 00:30:00,   5000.0
        """,
        parse_dates=["datetime"],
    )
    demand_subregions = pd.DataFrame(columns=["subregion_id", "geo_id"])

    result = _sum_subregion_traces_per_demand_node(subregion_traces, demand_subregions)

    expected = csv_str_to_df("""
        geo_id,  datetime,  value
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_sum_subregion_traces_per_demand_node_both_empty(csv_str_to_df):
    subregion_traces = pd.DataFrame(
        {
            "subregion": pd.Series(dtype="object"),
            "datetime": pd.Series(dtype="datetime64[us]"),
            "value": pd.Series(dtype="float64"),
        }
    )
    demand_subregions = pd.DataFrame(columns=["subregion_id", "geo_id"])

    result = _sum_subregion_traces_per_demand_node(subregion_traces, demand_subregions)

    expected = csv_str_to_df("""
        geo_id,  datetime,  value
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_create_demand_node_traces_sub_regions(csv_str_to_df):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        CNSW,    subregion
        NNSW,    subregion
        SQ,      subregion
    """)

    result = _create_demand_node_traces(
        hierarchy,
        network_geography,
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


def test_create_demand_node_traces_nem_regions(csv_str_to_df, caplog):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        NSW,     region
        QLD,     region
    """)

    with caplog.at_level("WARNING"):
        result = _create_demand_node_traces(
            hierarchy,
            network_geography,
            _TRACE_DATA_PATH,
            scenario="Step Change",
            reference_year_mapping={2025: 2011, 2026: 2018},
            year_type="fy",
        )

    assert sorted(result) == ["NSW", "QLD"]

    # NSW is the sum of the CNSW and NNSW traces, which cover the same datetimes.
    cnsw = _read_subregion_demand("CNSW")
    nnsw = _read_subregion_demand("NNSW")
    expected_nsw = pd.DataFrame(
        {"datetime": cnsw["datetime"], "value": cnsw["value"] + nnsw["value"]}
    )
    pd.testing.assert_frame_equal(result["NSW"], expected_nsw)

    # CQ and NQ are placed at QLD but have no test traces, so QLD is SQ alone.
    assert (
        "ISP sub-regions with no demand trace data (their demand is left out of "
        "the model): ['CQ', 'NQ']"
    ) in caplog.text
    pd.testing.assert_frame_equal(result["QLD"], _read_subregion_demand("SQ"))


def test_create_demand_node_traces_single_region(csv_str_to_df):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        NEM,     region
    """)

    result = _create_demand_node_traces(
        hierarchy,
        network_geography,
        _TRACE_DATA_PATH,
        scenario="Step Change",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    assert sorted(result) == ["NEM"]

    # NEM is the sum of the three sub-region traces that exist (CNSW, NNSW, SQ).
    cnsw = _read_subregion_demand("CNSW")
    nnsw = _read_subregion_demand("NNSW")
    sq = _read_subregion_demand("SQ")
    expected_nem = pd.DataFrame(
        {
            "datetime": cnsw["datetime"],
            "value": cnsw["value"] + nnsw["value"] + sq["value"],
        }
    )
    pd.testing.assert_frame_equal(result["NEM"], expected_nem)


def test_create_demand_node_traces_no_log_when_every_subregion_has_a_trace(
    csv_str_to_df, caplog
):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    # SQ is the only sub-region placed in this model, and it has a trace.
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        SQ,      subregion
    """)

    with caplog.at_level("WARNING"):
        _create_demand_node_traces(
            hierarchy,
            network_geography,
            _TRACE_DATA_PATH,
            scenario="Step Change",
            reference_year_mapping={2025: 2011, 2026: 2018},
            year_type="fy",
        )

    assert "no demand trace data" not in caplog.text


def test_demand_node_whose_subregions_all_lack_traces(csv_str_to_df, caplog):
    hierarchy = _network_geography_hierarchy(csv_str_to_df)
    # CQ is the only sub-region placed in this model, and it has no test trace.
    network_geography = csv_str_to_df("""
        geo_id,  geo_type
        CQ,      subregion
    """)

    demand_nodes = _translate_demand_nodes(hierarchy, network_geography)
    with caplog.at_level("WARNING"):
        demand_traces = _create_demand_node_traces(
            hierarchy,
            network_geography,
            _TRACE_DATA_PATH,
            scenario="Step Change",
            reference_year_mapping={2025: 2011, 2026: 2018},
            year_type="fy",
        )

    # CQ is still listed as a demand node, but has no entry in the traces.
    expected_demand_nodes = csv_str_to_df("""
        name
        CQ
    """)
    pd.testing.assert_frame_equal(demand_nodes, expected_demand_nodes)

    assert demand_traces == {}
    assert (
        "ISP sub-regions with no demand trace data (their demand is left out of "
        "the model): ['CQ']"
    ) in caplog.text


def test_create_demand_node_traces_no_subregions():
    hierarchy = pd.DataFrame(columns=_HIERARCHY_COLUMNS)
    network_geography = pd.DataFrame(columns=_GEOGRAPHY_COLUMNS)

    result = _create_demand_node_traces(
        hierarchy,
        network_geography,
        _TRACE_DATA_PATH,
        scenario="Step Change",
        reference_year_mapping={2025: 2011, 2026: 2018},
        year_type="fy",
    )

    assert result == {}
