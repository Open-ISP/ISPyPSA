"""Tests the transmission results under the new table format
(use_new_table_format=true): the capacities of links whose limits are set per
timeslice, and the geographic flows with the regions and zones mapping built
from network_geography.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into test_transmission.py
and drop the flag fixture.
"""

from unittest.mock import patch

import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build import build_pypsa_network
from ispypsa.results.extract import extract_regions_and_zones_mapping
from ispypsa.results.transmission import (
    extract_isp_sub_region_transmission_flows,
    extract_nem_region_transmission_flows,
    extract_rez_transmission_flows,
    extract_transmission_expansion_results,
    extract_transmission_flows,
)

_GEOGRAPHIC_FLOW_COLUMNS = (
    "investment_period,  timestep,  imports_mw,  exports_mw,  net_imports_mw"
)


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


# --- extract_transmission_expansion_results ---


def _network(
    links: pd.DataFrame, p_max_pu: pd.DataFrame, p_min_pu: pd.DataFrame
) -> pypsa.Network:
    """A network holding the links as if solved (p_nom_opt is set), with the
    per-snapshot p_max_pu and p_min_pu series given for some of them.

    p_max_pu and p_min_pu have a snapshot column and a column per link with a
    series; a link without one keeps its static p_max_pu / p_min_pu, as an
    expansion link does.
    """
    network = pypsa.Network()
    network.set_snapshots(pd.to_datetime(p_max_pu["snapshot"]))
    network.add("Bus", pd.concat([links["bus0"], links["bus1"]]).unique())
    for _, link in links.iterrows():
        network.add("Link", **link.to_dict())
    network.links_t.p_max_pu = p_max_pu.drop(columns="snapshot").set_index(
        network.snapshots
    )
    network.links_t.p_min_pu = p_min_pu.drop(columns="snapshot").set_index(
        network.snapshots
    )
    return network


def test_capacity_of_an_existing_path_with_limits_by_direction_and_timeslice(
    csv_str_to_df, new_table_format
):
    # p_nom is the largest limit of either direction (reverse, 200 MW); the
    # static p_max_pu / p_min_pu are placeholders under the series.
    links = csv_str_to_df("""
        name,          isp_name,  isp_type,   bus0,  bus1,  build_year,  p_nom_opt,  p_max_pu,  p_min_pu
        A-B_existing,  A-B,       flow_path,  A,     B,     2024,        200,        1.0,       0.0
    """)
    # Forward is 100 MW, or 50 MW at peak; reverse is 200 MW, or 150 MW at peak.
    p_max_pu = csv_str_to_df("""
        snapshot,             A-B_existing
        2025-01-01 00:00:00,  0.5
        2025-01-01 01:00:00,  0.25
    """)
    p_min_pu = csv_str_to_df("""
        snapshot,             A-B_existing
        2025-01-01 00:00:00,  -1.0
        2025-01-01 01:00:00,  -0.75
    """)
    network = _network(links, p_max_pu, p_min_pu)

    result = extract_transmission_expansion_results(network)

    expected = csv_str_to_df("""
        isp_name,  investment_period,  isp_type,   node_from,  node_to,  forward_capacity_mw,  reverse_capacity_mw
        A-B,       2024,               flow_path,  A,          B,        100.0,                -200.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_capacity_of_a_path_expanded_in_one_investment_period_and_not_another(
    csv_str_to_df, new_table_format
):
    # The expansion option is 150 MW forward and 300 MW reverse, so its links'
    # static limits are per unit of 300 MW. Nothing is built in 2025.
    links = csv_str_to_df("""
        name,          isp_name,  isp_type,   bus0,  bus1,  build_year,  p_nom_opt,  p_max_pu,  p_min_pu
        A-B_existing,  A-B,       flow_path,  A,     B,     2024,        200,        1.0,       0.0
        A-B_exp_2025,  A-B,       flow_path,  A,     B,     2025,        0,          0.5,       -1.0
        A-B_exp_2030,  A-B,       flow_path,  A,     B,     2030,        300,        0.5,       -1.0
    """)
    p_max_pu = csv_str_to_df("""
        snapshot,             A-B_existing
        2025-01-01 00:00:00,  0.5
        2025-01-01 01:00:00,  0.25
    """)
    p_min_pu = csv_str_to_df("""
        snapshot,             A-B_existing
        2025-01-01 00:00:00,  -1.0
        2025-01-01 01:00:00,  -1.0
    """)
    network = _network(links, p_max_pu, p_min_pu)

    result = extract_transmission_expansion_results(network)

    # 2030 adds 300 * 0.5 = 150 MW forward and 300 MW reverse to the existing
    # 100 MW forward and 200 MW reverse.
    expected = csv_str_to_df("""
        isp_name,  investment_period,  isp_type,   node_from,  node_to,  forward_capacity_mw,  reverse_capacity_mw
        A-B,       2024,               flow_path,  A,          B,        100.0,                -200.0
        A-B,       2025,               flow_path,  A,          B,        100.0,                -200.0
        A-B,       2030,               flow_path,  A,          B,        250.0,                -500.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_capacity_of_a_new_corridor(csv_str_to_df, new_table_format):
    # A zero-capacity existing path has zero limits; the expansion option is
    # 400 MW forward and 200 MW reverse.
    links = csv_str_to_df("""
        name,          isp_name,  isp_type,   bus0,  bus1,  build_year,  p_nom_opt,  p_max_pu,  p_min_pu
        C-D_existing,  C-D,       flow_path,  C,     D,     2024,        0,          1.0,       0.0
        C-D_exp_2030,  C-D,       flow_path,  C,     D,     2030,        400,        1.0,       -0.5
    """)
    p_max_pu = csv_str_to_df("""
        snapshot,             C-D_existing
        2025-01-01 00:00:00,  0.0
    """)
    p_min_pu = csv_str_to_df("""
        snapshot,             C-D_existing
        2025-01-01 00:00:00,  0.0
    """)
    network = _network(links, p_max_pu, p_min_pu)

    result = extract_transmission_expansion_results(network)

    expected = csv_str_to_df("""
        isp_name,  investment_period,  isp_type,   node_from,  node_to,  forward_capacity_mw,  reverse_capacity_mw
        C-D,       2024,               flow_path,  C,          D,        0.0,                  0.0
        C-D,       2030,               flow_path,  C,          D,        400.0,                -200.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_capacity_with_no_links(csv_str_to_df, new_table_format):
    # E.g. a single_region model with its REZs attached to the NEM node.
    links = pd.DataFrame(columns=["name", "bus0", "bus1"])
    p_max_pu = csv_str_to_df("""
        snapshot
        2025-01-01 00:00:00
    """)
    network = _network(links, p_max_pu, p_max_pu)

    result = extract_transmission_expansion_results(network)

    expected = csv_str_to_df("""
        isp_name,  investment_period,  isp_type,  node_from,  node_to,  forward_capacity_mw,  reverse_capacity_mw
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- capacities against a solved network ---


def _pypsa_friendly_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """A two-bus model whose demand swings between the buses, so the flow on
    the A-B path reaches its limit in both directions.

    Each bus has a 1000 MW generator and 2000 MW of demand at some
    snapshots, so the other bus exports as much as the path allows. 00:00 and
    01:00 send power from A to B and 02:00 from B to A, in each of the
    investment periods 2025 and 2030. Unserved energy covers the rest.

    A-B's existing limits are as the translator writes them: 100 MW forward
    (50 MW in the "peak" timeslice, at 01:00) and 200 MW reverse, so p_nom is
    200 MW. Its expansion option is 150 MW forward and 300 MW reverse,
    expressed per unit of 300 MW, and p_nom_max holds it to that. Building it
    in 2025 is too expensive; in 2030 it is cheap.

    The demand traces are written by each test.
    """
    tables = {}
    tables["investment_period_weights"] = csv_str_to_df("""
        period,  years,  objective
        2025,    5,      1
        2030,    5,      1
    """)
    tables["buses"] = csv_str_to_df("""
        name
        A
        B
    """)
    tables["generators"] = csv_str_to_df("""
        name,          carrier,  bus,  p_nom,  p_nom_extendable,  capital_cost,  marginal_cost
        gen_A,         Gas,      A,    1000,   False,             0,             1
        gen_B,         Gas,      B,    1000,   False,             0,             1
        unserved_A,    Gas,      A,    10000,  False,             0,             10000
        unserved_B,    Gas,      B,    10000,  False,             0,             10000
    """)
    tables["links"] = csv_str_to_df("""
        isp_name,  name,          bus0,  bus1,  carrier,  p_nom,  p_max_pu,  p_min_pu,  p_nom_extendable,  p_nom_max,  build_year,  lifetime,  capital_cost,  isp_type
        A-B,       A-B_existing,  A,     B,     AC,       200,    1.0,       0.0,       False,             inf,        2024,        inf,       0,             flow_path
        A-B,       A-B_exp_2025,  A,     B,     AC,       0,      0.5,       -1.0,      True,              300,        2025,        inf,       1e9,           flow_path
        A-B,       A-B_exp_2030,  A,     B,     AC,       0,      0.5,       -1.0,      True,              300,        2030,        inf,       1,             flow_path
    """)
    tables["link_timeslice_limits"] = csv_str_to_df("""
        name,          attribute,  timeslice,  value
        A-B_existing,  p_max_pu,   peak,       0.25
        A-B_existing,  p_max_pu,   ,           0.5
        A-B_existing,  p_min_pu,   ,           -1.0
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
    """)
    tables["snapshots"] = pd.DataFrame(
        {
            "investment_periods": [2025, 2025, 2025, 2030, 2030, 2030],
            "snapshots": pd.to_datetime(
                [
                    "2025-01-01 00:00",
                    "2025-01-01 01:00",
                    "2025-01-01 02:00",
                    "2030-01-01 00:00",
                    "2030-01-01 01:00",
                    "2030-01-01 02:00",
                ]
            ),
            "generators": 1.0,
            "objective": 1.0,
            "stores": 1.0,
        }
    )
    tables["timeslice_snapshots"] = csv_str_to_df("""
        timeslice,  investment_periods,  snapshots
        peak,       2025,                2025-01-01 01:00:00
        peak,       2030,                2030-01-01 01:00:00
    """)
    return tables


def _save_demand_traces(snapshots: pd.DataFrame, timeseries_dir) -> None:
    """Writes demand traces that need imports at B at 00:00 and 01:00 and at
    A at 02:00."""
    (timeseries_dir / "demand_traces").mkdir()
    demand = snapshots.loc[:, ["investment_periods", "snapshots"]]
    demand["p_set"] = [0.0, 0.0, 2000.0, 0.0, 0.0, 2000.0]
    demand.to_parquet(timeseries_dir / "demand_traces" / "A.parquet")
    demand["p_set"] = [2000.0, 2000.0, 0.0, 2000.0, 2000.0, 0.0]
    demand.to_parquet(timeseries_dir / "demand_traces" / "B.parquet")


def _largest_flows(network: pypsa.Network) -> pd.DataFrame:
    """The largest flow on A-B in each direction in each investment period."""
    flows = extract_transmission_flows(network)
    return flows.groupby("investment_period", as_index=False).agg(
        largest_forward_flow_mw=("flow_mw", "max"),
        largest_reverse_flow_mw=("flow_mw", "min"),
    )


def test_capacities_match_the_largest_flows_in_a_solved_network(
    csv_str_to_df, tmp_path, new_table_format
):
    tables = _pypsa_friendly_tables(csv_str_to_df)
    _save_demand_traces(tables["snapshots"], tmp_path)
    network = build_pypsa_network(tables, tmp_path)
    network.optimize.solve_model(solver_name="highs")

    result = extract_transmission_expansion_results(network)

    # The 2030 expansion is built to 300 MW, adding 150 MW forward and 300 MW
    # reverse. Before the change these were reported as 200 forward and 0
    # reverse for the existing link (p_nom and p_nom * the 0.0 placeholder),
    # and 500 / -300 in 2030.
    expected = csv_str_to_df("""
        isp_name,  investment_period,  isp_type,   node_from,  node_to,  forward_capacity_mw,  reverse_capacity_mw
        A-B,       2024,               flow_path,  A,          B,        100.0,                -200.0
        A-B,       2025,               flow_path,  A,          B,        100.0,                -200.0
        A-B,       2030,               flow_path,  A,          B,        250.0,                -500.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False)

    # The model uses the whole capacity in each direction: the flows reach
    # the capacities reported for each investment period.
    expected_flows = csv_str_to_df("""
        investment_period,  largest_forward_flow_mw,  largest_reverse_flow_mw
        2025,               100.0,                    -200.0
        2030,               250.0,                    -500.0
    """)
    pd.testing.assert_frame_equal(
        _largest_flows(network), expected_flows, check_exact=False
    )


def test_capacities_are_unchanged_for_a_network_reloaded_from_netcdf(
    csv_str_to_df, tmp_path, new_table_format
):
    tables = _pypsa_friendly_tables(csv_str_to_df)
    # A forward limit at p_nom everywhere is a p_max_pu of 1.0, PyPSA's
    # default, at every snapshot, so the series is not saved to netCDF.
    tables["link_timeslice_limits"] = csv_str_to_df("""
        name,          attribute,  timeslice,  value
        A-B_existing,  p_max_pu,   ,           1.0
        A-B_existing,  p_min_pu,   peak,       -0.25
        A-B_existing,  p_min_pu,   ,           -0.5
    """)
    _save_demand_traces(tables["snapshots"], tmp_path)
    network = build_pypsa_network(tables, tmp_path)
    network.optimize.solve_model(solver_name="highs")
    network.export_to_netcdf(tmp_path / "network.nc")
    reloaded = pypsa.Network(tmp_path / "network.nc")

    result = extract_transmission_expansion_results(reloaded)

    assert "A-B_existing" not in reloaded.links_t.p_max_pu.columns
    # Existing: 200 MW forward and 100 MW reverse. The 2030 expansion adds
    # 150 MW forward and 300 MW reverse.
    expected = csv_str_to_df("""
        isp_name,  investment_period,  isp_type,   node_from,  node_to,  forward_capacity_mw,  reverse_capacity_mw
        A-B,       2024,               flow_path,  A,          B,        200.0,                -100.0
        A-B,       2025,               flow_path,  A,          B,        200.0,                -100.0
        A-B,       2030,               flow_path,  A,          B,        350.0,                -400.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False)
    pd.testing.assert_frame_equal(
        result, extract_transmission_expansion_results(network)
    )


# --- geographic flows with the new-format mapping ---


def _geographic_flows(link_flows: pd.DataFrame, ispypsa_tables: dict) -> dict:
    mapping = extract_regions_and_zones_mapping(ispypsa_tables)
    return {
        "nem_region": extract_nem_region_transmission_flows(link_flows, mapping),
        "sub_region": extract_isp_sub_region_transmission_flows(link_flows, mapping),
        "rez": extract_rez_transmission_flows(link_flows, mapping),
    }


def test_geographic_flows_at_sub_regions_granularity(csv_str_to_df, new_table_format):
    ispypsa_tables = {
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,   region_id,  subregion_id
            CNSW,    subregion,  NSW,        CNSW
            NNSW,    subregion,  NSW,        NNSW
            SQ,      subregion,  QLD,        SQ
            N3,      rez,        NSW,        CNSW
        """),
    }
    link_flows = csv_str_to_df("""
        investment_period,  timestep,  from_node,  to_node,  flow_mw,  isp_name
        2025,               0,         CNSW,       NNSW,     100,      CNSW-NNSW
        2025,               0,         NNSW,       SQ,       40,       NNSW-SQ
        2025,               0,         N3,         CNSW,     300,      N3-CNSW
    """)

    result = _geographic_flows(link_flows, ispypsa_tables)

    # Only NNSW-SQ crosses a region boundary.
    expected = csv_str_to_df(f"""
        nem_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        NSW,            2025,  0,  0,  40,  -40
        QLD,            2025,  0,  40, 0,   40
    """)
    pd.testing.assert_frame_equal(result["nem_region"], expected)
    # N3-CNSW is inside CNSW.
    expected = csv_str_to_df(f"""
        isp_sub_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        CNSW,               2025,  0,  0,    100,  -100
        NNSW,               2025,  0,  100,  40,   60
        SQ,                 2025,  0,  40,   0,    40
    """)
    pd.testing.assert_frame_equal(result["sub_region"], expected)
    expected = csv_str_to_df(f"""
        rez_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        N3,      2025,  0,  0,  300,  -300
    """)
    pd.testing.assert_frame_equal(result["rez"], expected)


def test_geographic_flows_at_nem_regions_granularity(csv_str_to_df, new_table_format):
    ispypsa_tables = {
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,  region_id
            NSW,     region,    NSW
            QLD,     region,    QLD
            N3,      rez,       NSW
        """),
    }
    link_flows = csv_str_to_df("""
        investment_period,  timestep,  from_node,  to_node,  flow_mw,  isp_name
        2025,               0,         NSW,        QLD,      40,       NSW-QLD
        2025,               0,         N3,         NSW,      300,      N3-NSW
    """)

    result = _geographic_flows(link_flows, ispypsa_tables)

    # N3-NSW is inside NSW.
    expected = csv_str_to_df(f"""
        nem_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        NSW,            2025,  0,  0,  40,  -40
        QLD,            2025,  0,  40, 0,   40
    """)
    pd.testing.assert_frame_equal(result["nem_region"], expected)
    # The model has no sub-region nodes, so there are no sub-region flows.
    expected = csv_str_to_df(f"""
        isp_sub_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
    """)
    pd.testing.assert_frame_equal(result["sub_region"], expected, check_dtype=False)
    expected = csv_str_to_df(f"""
        rez_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        N3,      2025,  0,  0,  300,  -300
    """)
    pd.testing.assert_frame_equal(result["rez"], expected)


def test_geographic_flows_at_single_region_granularity(csv_str_to_df, new_table_format):
    ispypsa_tables = {
        "network_geography": csv_str_to_df("""
            geo_id,  geo_type,  region_id
            NEM,     region,    NEM
            N3,      rez,       NEM
        """),
    }
    link_flows = csv_str_to_df("""
        investment_period,  timestep,  from_node,  to_node,  flow_mw,  isp_name
        2025,               0,         N3,         NEM,      300,      N3-NEM
    """)

    result = _geographic_flows(link_flows, ispypsa_tables)

    # No node has a region or a sub-region at single_region granularity.
    expected = csv_str_to_df(f"""
        nem_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
    """)
    pd.testing.assert_frame_equal(result["nem_region"], expected, check_dtype=False)
    expected = csv_str_to_df(f"""
        isp_sub_region_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
    """)
    pd.testing.assert_frame_equal(result["sub_region"], expected, check_dtype=False)
    expected = csv_str_to_df(f"""
        rez_id,  {_GEOGRAPHIC_FLOW_COLUMNS}
        N3,      2025,  0,  0,  300,  -300
    """)
    pd.testing.assert_frame_equal(result["rez"], expected)
