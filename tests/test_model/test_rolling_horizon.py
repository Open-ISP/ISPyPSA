from pathlib import Path

import pandas as pd
import pypsa

from ispypsa.pypsa_build import (
    build_pypsa_network,
    optimize_with_rolling_horizon,
    update_network_timeseries,
)
from ispypsa.pypsa_build.custom_constraints import _move_fixed_capacity_terms_to_rhs

_RHS_COLUMNS = ["constraint_name", "rhs", "constraint_type"]
_LHS_COLUMNS = [
    "constraint_name",
    "variable_name",
    "component",
    "attribute",
    "coefficient",
]


def _expand_then_operate_with_rolling_horizon(
    tmp_path: Path,
    csv_str_to_df,
    custom_constraints_rhs: pd.DataFrame,
    custom_constraints_lhs: pd.DataFrame,
) -> pd.DataFrame:
    """Runs the capacity expansion and then the operational model the way the
    CLI does, and returns the operational flow on the NQ-SQ link.

    The network has cheap generation at NQ and expensive generation at SQ,
    joined by the 200 MW NQ-SQ link, serving a 100 MW load at SQ. Left
    unconstrained, the link carries the whole 100 MW at every snapshot, so a
    custom constraint on the link shows up as a lower flow. The SQ_relax
    generator sits on the custom constraint bus and can be built, at a small
    cost, for constraints to reference its capacity.

    The operational model has four snapshots in each of two investment
    periods, solved in windows of three snapshots that overlap by one, so one
    window spans both investment periods.
    """
    tables = {
        "buses": csv_str_to_df("""
            name
            NQ
            SQ
        """),
        "generators": csv_str_to_df("""
            name,       carrier,  bus,  p_nom,  p_nom_extendable,  capital_cost,  marginal_cost
            cheap,      Gas,      NQ,   200.0,  False,             0.0,           10.0
            expensive,  Gas,      SQ,   200.0,  False,             0.0,           100.0
        """),
        "links": csv_str_to_df("""
            name,   carrier,  bus0,  bus1,  p_nom,  p_nom_extendable,  p_min_pu,  p_max_pu
            NQ-SQ,  AC,       NQ,    SQ,    200.0,  False,             -1.0,      1.0
        """),
        "custom_constraints_generators": csv_str_to_df("""
            name,      bus,                             p_nom,  p_nom_extendable,  capital_cost
            SQ_relax,  bus_for_custom_constraint_gens,  0.0,    True,              1.0
        """),
        "investment_period_weights": csv_str_to_df("""
            period,  years,  objective
            2025,    1,      1.0
            2026,    1,      0.5
        """),
        "custom_constraints_rhs": custom_constraints_rhs,
        "custom_constraints_lhs": custom_constraints_lhs,
    }
    tables["snapshots"] = csv_str_to_df(
        """
        investment_periods,  snapshots,            generators,  objective,  stores
        2025,                2025-01-01 00:00:00,  1.0,         1.0,        1.0
        2026,                2026-01-01 00:00:00,  1.0,         1.0,        1.0
        """,
        parse_dates=["snapshots"],
    )
    operational_snapshots = csv_str_to_df(
        """
        investment_periods,  snapshots,            generators,  objective,  stores
        2025,                2025-01-01 00:00:00,  1.0,         1.0,        1.0
        2025,                2025-01-01 01:00:00,  1.0,         1.0,        1.0
        2025,                2025-01-01 02:00:00,  1.0,         1.0,        1.0
        2025,                2025-01-01 03:00:00,  1.0,         1.0,        1.0
        2026,                2026-01-01 00:00:00,  1.0,         1.0,        1.0
        2026,                2026-01-01 01:00:00,  1.0,         1.0,        1.0
        2026,                2026-01-01 02:00:00,  1.0,         1.0,        1.0
        2026,                2026-01-01 03:00:00,  1.0,         1.0,        1.0
        """,
        parse_dates=["snapshots"],
    )

    _write_demand_traces(tmp_path, tables["snapshots"])
    network = build_pypsa_network(tables, tmp_path)
    network.optimize.solve_model(solver_name="highs")

    _write_demand_traces(tmp_path, operational_snapshots)
    update_network_timeseries(network, tables, operational_snapshots, tmp_path)
    network.optimize.fix_optimal_capacities()
    optimize_with_rolling_horizon(network, tables, horizon=3, overlap=1)

    return network.links_t.p0[["NQ-SQ"]].reset_index().rename_axis(columns=None)


def _write_demand_traces(directory: Path, snapshots: pd.DataFrame) -> None:
    """Writes a flat demand trace for each bus at the given snapshots: no load
    at NQ and 100 MW at SQ."""
    (directory / "demand_traces").mkdir(exist_ok=True)
    demand = snapshots[["investment_periods", "snapshots"]]
    demand.assign(p_set=0.0).to_parquet(directory / "demand_traces" / "NQ.parquet")
    demand.assign(p_set=100.0).to_parquet(directory / "demand_traces" / "SQ.parquet")


def test_rolling_horizon_respects_dispatch_constraint(tmp_path, csv_str_to_df):
    rhs = csv_str_to_df("""
        constraint_name,  rhs,   constraint_type
        SQ_flow,          40.0,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
        SQ_flow,          NQ-SQ,          Link,       p,          1.0
    """)

    flows = _expand_then_operate_with_rolling_horizon(tmp_path, csv_str_to_df, rhs, lhs)

    # Without SQ_flow the link would carry the whole 100 MW load. SQ_flow has
    # no SQ_relax term, so SQ_relax can't lift the 40 MW limit.
    expected = csv_str_to_df(
        """
        period,  timestep,             NQ-SQ
        2025,    2025-01-01 00:00:00,  40.0
        2025,    2025-01-01 01:00:00,  40.0
        2025,    2025-01-01 02:00:00,  40.0
        2025,    2025-01-01 03:00:00,  40.0
        2026,    2026-01-01 00:00:00,  40.0
        2026,    2026-01-01 01:00:00,  40.0
        2026,    2026-01-01 02:00:00,  40.0
        2026,    2026-01-01 03:00:00,  40.0
        """,
        parse_dates=["timestep"],
    )
    pd.testing.assert_frame_equal(flows, expected, check_dtype=False)


def test_rolling_horizon_respects_dispatch_and_capacity_constraint(
    tmp_path, csv_str_to_df
):
    rhs = csv_str_to_df("""
        constraint_name,  rhs,   constraint_type
        SQ_flow,          40.0,  <=
        SQ_relax_limit,   20.0,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
        SQ_flow,          NQ-SQ,          Link,       p,          1.0
        SQ_flow,          SQ_relax,       Generator,  p_nom,      -1.0
        SQ_relax_limit,   SQ_relax,       Generator,  p_nom,      1.0
    """)

    flows = _expand_then_operate_with_rolling_horizon(tmp_path, csv_str_to_df, rhs, lhs)

    # The capacity expansion model builds SQ_relax up to its 20 MW limit
    # (SQ_relax_limit), which lifts the SQ_flow limit on the link from 40 MW
    # to 60 MW. Without SQ_flow the link would carry the whole 100 MW load.
    expected = csv_str_to_df(
        """
        period,  timestep,             NQ-SQ
        2025,    2025-01-01 00:00:00,  60.0
        2025,    2025-01-01 01:00:00,  60.0
        2025,    2025-01-01 02:00:00,  60.0
        2025,    2025-01-01 03:00:00,  60.0
        2026,    2026-01-01 00:00:00,  60.0
        2026,    2026-01-01 01:00:00,  60.0
        2026,    2026-01-01 02:00:00,  60.0
        2026,    2026-01-01 03:00:00,  60.0
        """,
        parse_dates=["timestep"],
    )
    pd.testing.assert_frame_equal(flows, expected, check_dtype=False)


def test_rolling_horizon_tracks_state_of_charge_across_windows(csv_str_to_df):
    # The load is 90 MW with a 110 MW peak at the third and sixth snapshots.
    # Gas (10 $/MWh) can serve up to 100 MW, so a peak needs the peaker
    # (100 $/MWh) or the battery.
    snapshots = csv_str_to_df(
        """
        investment_periods,  snapshots,            load
        2025,                2025-01-01 00:00:00,  90.0
        2025,                2025-01-01 01:00:00,  90.0
        2025,                2025-01-01 02:00:00,  110.0
        2026,                2026-01-01 00:00:00,  90.0
        2026,                2026-01-01 01:00:00,  90.0
        2026,                2026-01-01 02:00:00,  110.0
        """,
        parse_dates=["snapshots"],
    )
    snapshots = snapshots.set_index(["investment_periods", "snapshots"])
    network = pypsa.Network(snapshots=snapshots.index, investment_periods=[2025, 2026])
    network.add("Bus", "NQ")
    network.add("Load", "NQ_load", bus="NQ", p_set=snapshots["load"])
    network.add("Generator", "gas", bus="NQ", p_nom=100.0, marginal_cost=10.0)
    network.add("Generator", "peaker", bus="NQ", p_nom=100.0, marginal_cost=100.0)
    # A 5 MW battery holding up to 20 MWh (4 hours). It is added with
    # cyclic_state_of_charge=True, which is what the translator sets on every
    # battery, so the test shows optimize_with_rolling_horizon turning it off.
    # Left on, the battery would ignore the level carried from the previous
    # window.
    network.add(
        "StorageUnit",
        "battery",
        bus="NQ",
        p_nom=5.0,
        max_hours=4.0,
        cyclic_state_of_charge=True,
    )
    tables = {
        "custom_constraints_rhs": pd.DataFrame(columns=_RHS_COLUMNS),
        "custom_constraints_lhs": pd.DataFrame(columns=_LHS_COLUMNS),
    }

    # Three windows of two snapshots each, with no overlap.
    optimize_with_rolling_horizon(network, tables, horizon=2, overlap=0)

    state_of_charge = network.storage_units_t.state_of_charge[["battery"]]
    state_of_charge = state_of_charge.reset_index().rename_axis(columns=None)

    # The battery starts half full, at 10 MWh.
    # Window 1 has no peak in it, so the battery discharges its full 5 MW at
    # both snapshots to save gas, and ends empty.
    # Window 2 starts from that empty level, not from 10 MWh again. It opens
    # with the peak that window 1 couldn't see, so the battery has nothing to
    # give, and with no peak later in the window it stays empty.
    # Window 3 has the second peak in view, so the battery charges 5 MWh
    # ahead of it and discharges into it.
    expected = csv_str_to_df(
        """
        period,  timestep,             battery
        2025,    2025-01-01 00:00:00,  5.0
        2025,    2025-01-01 01:00:00,  0.0
        2025,    2025-01-01 02:00:00,  0.0
        2026,    2026-01-01 00:00:00,  0.0
        2026,    2026-01-01 01:00:00,  5.0
        2026,    2026-01-01 02:00:00,  0.0
        """,
        parse_dates=["timestep"],
    )
    pd.testing.assert_frame_equal(
        state_of_charge, expected, check_dtype=False, check_exact=False
    )


def _network_with_fixed_and_extendable_capacities() -> pypsa.Network:
    """A network with a fixed 1400 MW link (CQ-NQ), a fixed 20 MW generator
    (NQ_relax) and an extendable generator (wind_new)."""
    network = pypsa.Network()
    network.add("Bus", ["CQ", "NQ"])
    network.add("Link", "CQ-NQ", bus0="CQ", bus1="NQ", p_nom=1400.0)
    network.add("Generator", "NQ_relax", bus="NQ", p_nom=20.0)
    network.add("Generator", "wind_new", bus="NQ", p_nom=150.0, p_nom_extendable=True)
    return network


def test_move_fixed_capacity_terms_to_rhs(csv_str_to_df):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = csv_str_to_df("""
        constraint_name,  rhs,    constraint_type
        NQ_flow,          40.0,   <=
        CQ-NQ_limit,      500.0,  <=
        wind_limit,       200.0,  <=
        NQ_dispatch,      30.0,   >=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
        NQ_flow,          CQ-NQ,          Link,       p,          1.0
        NQ_flow,          NQ_relax,       Generator,  p_nom,      -1.0
        NQ_flow,          CQ-NQ,          Link,       p_nom,      -0.01
        CQ-NQ_limit,      CQ-NQ,          Link,       p_nom,      1.0
        wind_limit,       wind_new,       Generator,  p_nom,      1.0
        NQ_dispatch,      NQ_relax,       Generator,  p,          1.0
    """)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs(network, rhs, lhs)

    # NQ_flow has two fixed capacity terms, NQ_relax (20 MW) and CQ-NQ
    # (1400 MW), so its rhs becomes 40 - (-1.0 x 20) - (-0.01 x 1400) = 74.
    # CQ-NQ_limit's only term is a fixed capacity, so the constraint is dropped.
    # wind_new is extendable and NQ_dispatch has no capacity term, so wind_limit
    # and NQ_dispatch are unchanged.
    expected_rhs = csv_str_to_df("""
        constraint_name,  rhs,    constraint_type
        NQ_flow,          74.0,   <=
        wind_limit,       200.0,  <=
        NQ_dispatch,      30.0,   >=
    """)
    pd.testing.assert_frame_equal(result_rhs.reset_index(drop=True), expected_rhs)

    expected_lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
        NQ_flow,          CQ-NQ,          Link,       p,          1.0
        wind_limit,       wind_new,       Generator,  p_nom,      1.0
        NQ_dispatch,      NQ_relax,       Generator,  p,          1.0
    """)
    pd.testing.assert_frame_equal(result_lhs.reset_index(drop=True), expected_lhs)


def test_move_fixed_capacity_terms_to_rhs_rhs_empty(csv_str_to_df):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = pd.DataFrame(columns=_RHS_COLUMNS)
    lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
        NQ_flow,          NQ_relax,       Generator,  p_nom,      -1.0
    """)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs(network, rhs, lhs)

    # With no rhs rows there are no constraints, so both tables are returned
    # as they were given.
    expected_rhs = csv_str_to_df("""
        constraint_name,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(result_rhs, expected_rhs, check_dtype=False)

    expected_lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
        NQ_flow,          NQ_relax,       Generator,  p_nom,      -1.0
    """)
    pd.testing.assert_frame_equal(result_lhs, expected_lhs)


def test_move_fixed_capacity_terms_to_rhs_lhs_empty(csv_str_to_df):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = csv_str_to_df("""
        constraint_name,  rhs,   constraint_type
        NQ_flow,          40.0,  <=
    """)
    lhs = pd.DataFrame(columns=_LHS_COLUMNS)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs(network, rhs, lhs)

    # NQ_flow has no LHS terms, so it is dropped.
    expected_rhs = csv_str_to_df("""
        constraint_name,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(
        result_rhs.reset_index(drop=True), expected_rhs, check_dtype=False
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
    """)
    pd.testing.assert_frame_equal(result_lhs, expected_lhs, check_dtype=False)


def test_move_fixed_capacity_terms_to_rhs_both_empty(csv_str_to_df):
    network = _network_with_fixed_and_extendable_capacities()
    rhs = pd.DataFrame(columns=_RHS_COLUMNS)
    lhs = pd.DataFrame(columns=_LHS_COLUMNS)

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs(network, rhs, lhs)

    expected_rhs = csv_str_to_df("""
        constraint_name,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(result_rhs, expected_rhs, check_dtype=False)

    expected_lhs = csv_str_to_df("""
        constraint_name,  variable_name,  component,  attribute,  coefficient
    """)
    pd.testing.assert_frame_equal(result_lhs, expected_lhs, check_dtype=False)


def test_move_fixed_capacity_terms_to_rhs_tables_without_columns():
    # The old-format translator returns tables with no columns when there are
    # no custom constraints.
    network = _network_with_fixed_and_extendable_capacities()

    result_rhs, result_lhs = _move_fixed_capacity_terms_to_rhs(
        network, pd.DataFrame(), pd.DataFrame()
    )

    pd.testing.assert_frame_equal(result_rhs, pd.DataFrame())
    pd.testing.assert_frame_equal(result_lhs, pd.DataFrame())
