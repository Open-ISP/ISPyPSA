"""Tests that optimize_with_rolling_horizon applies the new-format
(use_new_table_format=true) custom constraints in every window, each at the
snapshots of its investment period and timeslice.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into test_rolling_horizon.py
and drop the flag fixture.
"""

from unittest.mock import patch

import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build import optimize_with_rolling_horizon
from ispypsa.pypsa_build.custom_constraints import (
    _move_fixed_capacity_terms_to_rhs_by_investment_period,
)

_RHS_COLUMNS = [
    "constraint_name",
    "investment_period",
    "timeslice",
    "rhs",
    "constraint_type",
]
_LHS_COLUMNS = [
    "constraint_name",
    "investment_period",
    "variable_name",
    "component",
    "attribute",
    "coefficient",
]
_TIMESLICE_SNAPSHOTS_COLUMNS = ["timeslice", "investment_periods", "snapshots"]


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


def _two_bus_network(load: list[float]) -> pypsa.Network:
    """An operational network to solve in a rolling horizon: cheap generation
    at NQ and expensive generation at SQ, joined by the 1000 MW NQ-SQ link,
    serving the SQ load given per snapshot. Unconstrained, the link carries
    the whole load, so a custom constraint on the link shows up as a lower
    flow.

    There are four hourly snapshots in each of two investment periods (2025
    and 2030). Solved with horizon=3 and overlap=1, the windows are:

        window 1: 2025 00:00, 2025 01:00, 2025 02:00
        window 2: 2025 02:00, 2025 03:00, 2030 00:00   # spans both investment periods
        window 3: 2030 00:00, 2030 01:00, 2030 02:00
        window 4: 2030 02:00, 2030 03:00

    The capacities are fixed, as they are after fix_optimal_capacities. The
    two relaxation generators stand for capacity the expansion model built
    (5 MW in 2025, another 8 MW in 2030). They sit on their own bus, so they
    can't serve the load.
    """
    periods = [2025] * 4 + [2030] * 4
    timestamps = pd.to_datetime(
        [f"{period}-01-01 0{hour}:00" for period in [2025, 2030] for hour in range(4)]
    )
    index = pd.MultiIndex.from_arrays([periods, timestamps])
    network = pypsa.Network(snapshots=index, investment_periods=[2025, 2030])
    network.add("Bus", ["NQ", "SQ", "bus_for_custom_constraint_gens"])
    network.add("Link", "NQ-SQ", bus0="NQ", bus1="SQ", p_nom=1000.0)
    network.add("Generator", "cheap", bus="NQ", p_nom=1000.0, marginal_cost=1.0)
    network.add("Generator", "expensive", bus="SQ", p_nom=1000.0, marginal_cost=100.0)
    network.add(
        "Generator", "FLOWCAP_exp_2025", bus="bus_for_custom_constraint_gens", p_nom=5.0
    )
    network.add(
        "Generator", "FLOWCAP_exp_2030", bus="bus_for_custom_constraint_gens", p_nom=8.0
    )
    network.add("Load", "load_SQ", bus="SQ", p_set=pd.Series(load, index=index))
    return network


def _link_flow(network: pypsa.Network) -> pd.DataFrame:
    """The NQ-SQ link's flow out of NQ, one row per snapshot."""
    return network.links_t.p0[["NQ-SQ"]].reset_index().rename_axis(columns=None)


def test_rolling_horizon_applies_named_timeslice_only_at_its_snapshots(
    csv_str_to_df, new_table_format
):
    network = _two_bus_network(load=[100.0] * 8)
    tables = {
        "custom_constraints_rhs": csv_str_to_df("""
            constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
            FLOWCAP,          2025,               qld_peak_demand,  20.0,  <=
            FLOWCAP,          2030,               qld_peak_demand,  30.0,  <=
        """),
        "custom_constraints_lhs": csv_str_to_df("""
            constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
            FLOWCAP,          2025,               NQ-SQ,          Link,       p,          1.0
            FLOWCAP,          2030,               NQ-SQ,          Link,       p,          1.0
        """),
    }
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
        qld_peak_demand,  2030,                2030-01-01 03:00:00
    """)

    optimize_with_rolling_horizon(
        network, tables, horizon=3, overlap=1, timeslice_snapshots=timeslice_snapshots
    )

    # FLOWCAP has no fallback row, so the link carries the whole 100 MW load
    # outside qld_peak_demand. The 2025 peak snapshot is in window 1 only and
    # the 2030 one in window 4 only: windows 2 and 3 have no FLOWCAP snapshots
    # at all.
    expected = csv_str_to_df(
        """
        period,  timestep,             NQ-SQ
        2025,    2025-01-01 00:00:00,  100.0
        2025,    2025-01-01 01:00:00,  20.0
        2025,    2025-01-01 02:00:00,  100.0
        2025,    2025-01-01 03:00:00,  100.0
        2030,    2030-01-01 00:00:00,  100.0
        2030,    2030-01-01 01:00:00,  100.0
        2030,    2030-01-01 02:00:00,  100.0
        2030,    2030-01-01 03:00:00,  30.0
        """,
        parse_dates=["timestep"],
    )
    pd.testing.assert_frame_equal(
        _link_flow(network), expected, check_dtype=False, check_exact=False
    )


def test_rolling_horizon_applies_fallback_where_named_timeslices_do_not(
    csv_str_to_df, new_table_format
):
    network = _two_bus_network(load=[100.0] * 8)
    tables = {
        "custom_constraints_rhs": csv_str_to_df("""
            constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
            FLOWCAP,          2025,               qld_peak_demand,  20.0,  <=
            FLOWCAP,          2025,               ,                 40.0,  <=
            FLOWCAP,          2030,               qld_peak_demand,  30.0,  <=
            FLOWCAP,          2030,               ,                 60.0,  <=
        """),
        "custom_constraints_lhs": csv_str_to_df("""
            constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
            FLOWCAP,          2025,               NQ-SQ,          Link,       p,          1.0
            FLOWCAP,          2030,               NQ-SQ,          Link,       p,          1.0
        """),
    }
    # 2025 02:00 is the snapshot windows 1 and 2 share, and 2030 02:00 the one
    # windows 3 and 4 share.
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 02:00:00
        qld_peak_demand,  2030,                2030-01-01 02:00:00
        qld_peak_demand,  2030,                2030-01-01 03:00:00
    """)

    optimize_with_rolling_horizon(
        network, tables, horizon=3, overlap=1, timeslice_snapshots=timeslice_snapshots
    )

    # Each snapshot takes the qld_peak_demand limit of its investment period
    # where that timeslice is active and the fallback limit elsewhere. Window
    # 2 spans both investment periods and holds neither 2030 peak snapshot: it
    # applies 2025's peak and fallback rows and, at 2030 00:00, 2030's
    # fallback row. Window 4 holds only 2030 peak snapshots, so 2030's
    # fallback row has nothing left to apply to there.
    expected = csv_str_to_df(
        """
        period,  timestep,             NQ-SQ
        2025,    2025-01-01 00:00:00,  40.0
        2025,    2025-01-01 01:00:00,  40.0
        2025,    2025-01-01 02:00:00,  20.0
        2025,    2025-01-01 03:00:00,  40.0
        2030,    2030-01-01 00:00:00,  60.0
        2030,    2030-01-01 01:00:00,  60.0
        2030,    2030-01-01 02:00:00,  30.0
        2030,    2030-01-01 03:00:00,  30.0
        """,
        parse_dates=["timestep"],
    )
    pd.testing.assert_frame_equal(
        _link_flow(network), expected, check_dtype=False, check_exact=False
    )


def test_rolling_horizon_applies_fixed_capacity_and_load_terms(
    csv_str_to_df, new_table_format
):
    network = _two_bus_network(load=[60.0, 80.0, 100.0, 120.0] * 2)
    tables = {
        "custom_constraints_rhs": csv_str_to_df("""
            constraint_name,          investment_period,  timeslice,  rhs,   constraint_type
            FLOWCAP,                  2025,               ,           10.0,  <=
            FLOWCAP,                  2030,               ,           10.0,  <=
            FLOWCAP_expansion_limit,  ,                   ,           50.0,  <=
        """),
        "custom_constraints_lhs": csv_str_to_df("""
            constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
            FLOWCAP,                  2025,               NQ-SQ,             Link,       p,          1.0
            FLOWCAP,                  2025,               load_SQ,           Load,       p_set,      -0.5
            FLOWCAP,                  2025,               FLOWCAP_exp_2025,  Generator,  p_nom,      -1.0
            FLOWCAP,                  2030,               NQ-SQ,             Link,       p,          1.0
            FLOWCAP,                  2030,               load_SQ,           Load,       p_set,      -0.5
            FLOWCAP,                  2030,               FLOWCAP_exp_2025,  Generator,  p_nom,      -1.0
            FLOWCAP,                  2030,               FLOWCAP_exp_2030,  Generator,  p_nom,      -1.0
            FLOWCAP_expansion_limit,  ,                   FLOWCAP_exp_2025,  Generator,  p_nom,      1.0
            FLOWCAP_expansion_limit,  ,                   FLOWCAP_exp_2030,  Generator,  p_nom,      1.0
        """),
    }
    timeslice_snapshots = pd.DataFrame(columns=_TIMESLICE_SNAPSHOTS_COLUMNS)

    optimize_with_rolling_horizon(
        network, tables, horizon=3, overlap=1, timeslice_snapshots=timeslice_snapshots
    )

    # FLOWCAP limits the flow to 10 MW, plus the relaxation capacity built by
    # the investment period (5 MW in 2025; 5 + 8 = 13 MW in 2030), plus half
    # of the snapshot's SQ load (60, 80, 100 and 120 MW in each investment
    # period). So 2025 00:00 is 10 + 5 + 30 = 45 and 2030 00:00 is
    # 10 + 13 + 30 = 53. FLOWCAP_expansion_limit has only fixed capacity
    # terms, so it is left out.
    expected = csv_str_to_df(
        """
        period,  timestep,             NQ-SQ
        2025,    2025-01-01 00:00:00,  45.0
        2025,    2025-01-01 01:00:00,  55.0
        2025,    2025-01-01 02:00:00,  65.0
        2025,    2025-01-01 03:00:00,  75.0
        2030,    2030-01-01 00:00:00,  53.0
        2030,    2030-01-01 01:00:00,  63.0
        2030,    2030-01-01 02:00:00,  73.0
        2030,    2030-01-01 03:00:00,  83.0
        """,
        parse_dates=["timestep"],
    )
    pd.testing.assert_frame_equal(
        _link_flow(network), expected, check_dtype=False, check_exact=False
    )


def test_rolling_horizon_without_custom_constraints(csv_str_to_df, new_table_format):
    network = _two_bus_network(load=[100.0] * 8)
    tables = {
        "custom_constraints_rhs": pd.DataFrame(columns=_RHS_COLUMNS),
        "custom_constraints_lhs": pd.DataFrame(columns=_LHS_COLUMNS),
    }
    timeslice_snapshots = pd.DataFrame(columns=_TIMESLICE_SNAPSHOTS_COLUMNS)

    optimize_with_rolling_horizon(
        network, tables, horizon=3, overlap=1, timeslice_snapshots=timeslice_snapshots
    )

    expected = csv_str_to_df(
        """
        period,  timestep,             NQ-SQ
        2025,    2025-01-01 00:00:00,  100.0
        2025,    2025-01-01 01:00:00,  100.0
        2025,    2025-01-01 02:00:00,  100.0
        2025,    2025-01-01 03:00:00,  100.0
        2030,    2030-01-01 00:00:00,  100.0
        2030,    2030-01-01 01:00:00,  100.0
        2030,    2030-01-01 02:00:00,  100.0
        2030,    2030-01-01 03:00:00,  100.0
        """,
        parse_dates=["timestep"],
    )
    pd.testing.assert_frame_equal(
        _link_flow(network), expected, check_dtype=False, check_exact=False
    )


def _network_with_fixed_and_extendable_capacities() -> pypsa.Network:
    """A network with a fixed 300 MW link (CQ-NQ_exp_2025), two fixed
    relaxation generators (SWQLD1_exp_2025 at 20 MW and SWQLD1_exp_2030 at
    10 MW) and an extendable generator (wind_new)."""
    network = pypsa.Network()
    network.add("Bus", ["CQ", "NQ"])
    network.add("Link", "NSW-QLD", bus0="CQ", bus1="NQ", p_nom=1400.0)
    network.add("Link", "CQ-NQ_exp_2025", bus0="CQ", bus1="NQ", p_nom=300.0)
    network.add("Generator", "SWQLD1_exp_2025", bus="NQ", p_nom=20.0)
    network.add("Generator", "SWQLD1_exp_2030", bus="NQ", p_nom=10.0)
    network.add("Generator", "wind_new", bus="NQ", p_nom=150.0, p_nom_extendable=True)
    return network


def test_move_fixed_capacity_terms_to_rhs_by_investment_period(csv_str_to_df):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = csv_str_to_df("""
        constraint_name,        investment_period,  timeslice,        rhs,     constraint_type
        SWQLD1,                 2025,               qld_peak_demand,  3000.0,  <=
        SWQLD1,                 2025,               ,                 3500.0,  <=
        SWQLD1,                 2030,               ,                 3500.0,  <=
        NQ_LOAD,                2025,               ,                 70.0,    <=
        wind_limit,             ,                   ,                 200.0,   <=
        CQ-NQ_expansion_limit,  ,                   ,                 1000.0,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,        investment_period,  variable_name,    component,  attribute,  coefficient
        SWQLD1,                 2025,               NSW-QLD,          Link,       p,          0.84
        SWQLD1,                 2025,               SWQLD1_exp_2025,  Generator,  p_nom,      -1.0
        SWQLD1,                 2030,               NSW-QLD,          Link,       p,          0.84
        SWQLD1,                 2030,               SWQLD1_exp_2025,  Generator,  p_nom,      -1.0
        SWQLD1,                 2030,               SWQLD1_exp_2030,  Generator,  p_nom,      -1.0
        NQ_LOAD,                2025,               load_NQ,          Load,       p_set,      1.0
        NQ_LOAD,                2025,               SWQLD1_exp_2025,  Generator,  p_nom,      -1.0
        wind_limit,             ,                   wind_new,         Generator,  p_nom,      1.0
        CQ-NQ_expansion_limit,  ,                   CQ-NQ_exp_2025,   Link,       p_nom,      1.0
    """)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs_by_investment_period(
        network, rhs, lhs
    )

    # SWQLD1's fixed capacity terms are SWQLD1_exp_2025 (20 MW) in 2025, which
    # adds 20 to both of its 2025 rows, and SWQLD1_exp_2025 plus
    # SWQLD1_exp_2030 (10 MW) in 2030, which adds 30 to its 2030 row.
    # NQ_LOAD is left with only a load term and CQ-NQ_expansion_limit with no
    # terms, so both are dropped. wind_new is extendable, so wind_limit is
    # unchanged.
    expected_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,     constraint_type
        SWQLD1,           2025,               qld_peak_demand,  3020.0,  <=
        SWQLD1,           2025,               ,                 3520.0,  <=
        SWQLD1,           2030,               ,                 3530.0,  <=
        wind_limit,       ,                   ,                 200.0,   <=
    """)
    pd.testing.assert_frame_equal(result_rhs, expected_rhs)

    expected_lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD,        Link,       p,          0.84
        SWQLD1,           2030,               NSW-QLD,        Link,       p,          0.84
        NQ_LOAD,          2025,               load_NQ,        Load,       p_set,      1.0
        wind_limit,       ,                   wind_new,       Generator,  p_nom,      1.0
    """)
    pd.testing.assert_frame_equal(result_lhs.reset_index(drop=True), expected_lhs)


def test_move_fixed_capacity_terms_to_rhs_by_investment_period_rhs_empty(
    csv_str_to_df,
):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = pd.DataFrame(columns=_RHS_COLUMNS)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,    component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD,          Link,       p,          0.84
        SWQLD1,           2025,               SWQLD1_exp_2025,  Generator,  p_nom,      -1.0
    """)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs_by_investment_period(
        network, rhs, lhs
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(result_rhs, expected_rhs, check_dtype=False)

    # The fixed capacity term is still taken off the LHS.
    expected_lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD,        Link,       p,          0.84
    """)
    pd.testing.assert_frame_equal(result_lhs, expected_lhs)


def test_move_fixed_capacity_terms_to_rhs_by_investment_period_lhs_empty(
    csv_str_to_df,
):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,     constraint_type
        SWQLD1,           2025,               ,           3500.0,  <=
    """)
    lhs = pd.DataFrame(columns=_LHS_COLUMNS)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs_by_investment_period(
        network, rhs, lhs
    )

    # SWQLD1 has no LHS terms, so it is dropped.
    expected_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(result_rhs, expected_rhs, check_dtype=False)

    expected_lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
    """)
    pd.testing.assert_frame_equal(result_lhs, expected_lhs, check_dtype=False)


def test_move_fixed_capacity_terms_to_rhs_by_investment_period_both_empty(
    csv_str_to_df,
):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = pd.DataFrame(columns=_RHS_COLUMNS)
    lhs = pd.DataFrame(columns=_LHS_COLUMNS)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs_by_investment_period(
        network, rhs, lhs
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(result_rhs, expected_rhs, check_dtype=False)

    expected_lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
    """)
    pd.testing.assert_frame_equal(result_lhs, expected_lhs, check_dtype=False)
