"""Tests that build_pypsa_network and update_network_timeseries apply the
new-format (use_new_table_format=true) timeslice link limits and custom
constraints, at the snapshots of whichever model phase the network holds.

FEATURE_FLAG_CLEANUP[use_new_table_format]: merge into
test_updating_time_series.py and drop the flag fixture.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from ispypsa.pypsa_build import build_pypsa_network, update_network_timeseries


@pytest.fixture
def new_table_format():
    with patch.dict(
        "ispypsa.feature_flags.FEATURE_FLAGS", {"use_new_table_format": True}
    ):
        yield


def _pypsa_friendly_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """A two-bus model: 200 MW of demand at B, a cheap generator at A, an
    expensive one at B, and a 100 MW link from A to B. The cheapest dispatch
    sends as much as possible from A to B, so the flow on the link shows
    which limit binds at each snapshot:

    - 100 MW where neither timeslice is active (the link's fallback limit).
    - 50 MW in the "peak" timeslice (the link's limit there is 0.5 per unit).
    - 30 MW in the "shoulder" timeslice (the CHEAP_CAP custom constraint caps
      the cheap generator's output there).

    The snapshots and timeslice_snapshots tables are added by each test.
    """
    tables = {}
    tables["investment_period_weights"] = csv_str_to_df("""
        period,  years,  objective
        2025,    1,      1
    """)
    tables["buses"] = csv_str_to_df("""
        name
        A
        B
    """)
    tables["generators"] = csv_str_to_df("""
        name,       carrier,  bus,  p_nom,  p_nom_extendable,  capital_cost,  marginal_cost
        cheap,      Gas,      A,    1000,   False,             0,             1
        expensive,  Gas,      B,    1000,   False,             0,             100
    """)
    # p_max_pu / p_min_pu are the translator's placeholders: the limits come
    # from link_timeslice_limits.
    tables["links"] = csv_str_to_df("""
        name,          bus0,  bus1,  carrier,  p_nom,  p_max_pu,  p_min_pu,  p_nom_extendable
        A-B_existing,  A,     B,     AC,       100,    1.0,       0.0,       False
    """)
    tables["link_timeslice_limits"] = csv_str_to_df("""
        name,          attribute,  timeslice,  value
        A-B_existing,  p_max_pu,   peak,       0.5
        A-B_existing,  p_max_pu,   ,           1.0
        A-B_existing,  p_min_pu,   ,           -1.0
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        CHEAP_CAP,        2025,               shoulder,   30,   <=
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        CHEAP_CAP,        2025,               cheap,          Generator,  p,          1.0
    """)
    return tables


def _snapshots(timesteps: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "investment_periods": 2025,
            "snapshots": pd.to_datetime(timesteps),
            "generators": 1.0,
            "objective": 1.0,
            "stores": 1.0,
        }
    )


def _save_demand_trace(snapshots: pd.DataFrame, timeseries_dir) -> None:
    """Writes a flat 200 MW demand trace for bus B at the given snapshots."""
    demand = snapshots.loc[:, ["investment_periods", "snapshots"]]
    demand["p_set"] = 200.0
    (timeseries_dir / "demand_traces").mkdir(exist_ok=True)
    demand.to_parquet(timeseries_dir / "demand_traces" / "B.parquet")


def _link_flow(network, name: str) -> pd.DataFrame:
    """The link's flow out of bus0, one row per snapshot."""
    flow = network.links_t.p0[name].rename("flow")
    flow.index = flow.index.set_names(["investment_periods", "snapshots"])
    return flow.reset_index()


def test_build_applies_timeslice_link_limits_and_custom_constraints(
    csv_str_to_df, tmp_path, new_table_format
):
    tables = _pypsa_friendly_tables(csv_str_to_df)
    tables["snapshots"] = _snapshots(
        ["2025-01-01 00:00", "2025-01-01 01:00", "2025-01-01 02:00"]
    )
    tables["timeslice_snapshots"] = csv_str_to_df("""
        timeslice,  investment_periods,  snapshots
        peak,       2025,                2025-01-01 01:00:00
        shoulder,   2025,                2025-01-01 02:00:00
    """)
    _save_demand_trace(tables["snapshots"], tmp_path)

    network = build_pypsa_network(tables, tmp_path)
    network.optimize.solve_model(solver_name="highs")

    # 00:00 is in neither timeslice (fallback limit), 01:00 is peak (0.5 per
    # unit of 100 MW) and 02:00 is shoulder (CHEAP_CAP holds cheap to 30 MW).
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            flow
        2025,                2025-01-01 00:00:00,  100.0
        2025,                2025-01-01 01:00:00,  50.0
        2025,                2025-01-01 02:00:00,  30.0
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(
        _link_flow(network, "A-B_existing"), expected, check_exact=False
    )


def test_build_puts_committed_links_in_service_for_their_years(
    csv_str_to_df, tmp_path, new_table_format
):
    """A link with a later build_year only carries flow from that investment
    period, and one with a finite lifetime stops when it retires."""
    tables = _pypsa_friendly_tables(csv_str_to_df)
    tables["investment_period_weights"] = csv_str_to_df("""
        period,  years,  objective
        2025,    3,      1
        2028,    1,      1
    """)
    tables["links"] = csv_str_to_df("""
        name,                      bus0,  bus1,  carrier,  p_nom,  p_max_pu,  p_min_pu,  p_nom_extendable,  build_year,  lifetime
        A-B_existing,              A,     B,     AC,       100,    1.0,       0.0,       False,             2024,        inf
        A-B_committed_2027-12-02,  A,     B,     AC,       50,     1.0,       0.0,       False,             2028,        inf
        A-B_retiring_2027-07-01,   A,     B,     AC,       30,     1.0,       0.0,       False,             2024,        4
    """)
    tables["link_timeslice_limits"] = csv_str_to_df("""
        name,                      attribute,  timeslice,  value
        A-B_existing,              p_max_pu,   peak,       1.0
        A-B_existing,              p_max_pu,   ,           1.0
        A-B_existing,              p_min_pu,   ,           -1.0
        A-B_committed_2027-12-02,  p_max_pu,   ,           1.0
        A-B_committed_2027-12-02,  p_min_pu,   ,           -1.0
        A-B_retiring_2027-07-01,   p_max_pu,   ,           1.0
        A-B_retiring_2027-07-01,   p_min_pu,   ,           0.0
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
    """)
    tables["snapshots"] = pd.DataFrame(
        {
            "investment_periods": [2025, 2028],
            "snapshots": pd.to_datetime(["2025-01-01 00:00", "2028-01-01 00:00"]),
            "generators": 1.0,
            "objective": 1.0,
            "stores": 1.0,
        }
    )
    tables["timeslice_snapshots"] = csv_str_to_df("""
        timeslice,  investment_periods,  snapshots
        peak,       2025,                2025-01-01 00:00:00
    """)
    _save_demand_trace(tables["snapshots"], tmp_path)

    network = build_pypsa_network(tables, tmp_path)
    network.optimize.solve_model(solver_name="highs", multi_investment_periods=True)

    # The cheap generator at A sends as much as the links in service allow. The
    # committed link is built in 2028, and the retiring link (2024 + 4) is out
    # of service from 2028.
    flows = network.links_t.p0.reset_index(drop=True)
    flows.columns.name = None
    expected = csv_str_to_df("""
        A-B_existing,  A-B_committed_2027-12-02,  A-B_retiring_2027-07-01
        100.0,         0.0,                       30.0
        100.0,         50.0,                      0.0
    """)
    pd.testing.assert_frame_equal(flows, expected, check_exact=False)


def test_update_rebuilds_link_limits_and_custom_constraints_for_new_snapshots(
    csv_str_to_df, tmp_path, new_table_format
):
    tables = _pypsa_friendly_tables(csv_str_to_df)
    tables["snapshots"] = _snapshots(
        ["2025-01-01 00:00", "2025-01-01 01:00", "2025-01-01 02:00"]
    )
    tables["timeslice_snapshots"] = csv_str_to_df("""
        timeslice,  investment_periods,  snapshots
        peak,       2025,                2025-01-01 01:00:00
        shoulder,   2025,                2025-01-01 02:00:00
    """)
    _save_demand_trace(tables["snapshots"], tmp_path)
    network = build_pypsa_network(tables, tmp_path)

    # The first two operational snapshots are also capacity expansion
    # snapshots, but in different timeslices: 01:00 was peak and is now in
    # neither, 02:00 was shoulder and is now peak. The last two are a new day.
    operational_snapshots = _snapshots(
        [
            "2025-01-01 01:00",
            "2025-01-01 02:00",
            "2025-06-01 00:00",
            "2025-06-01 01:00",
        ]
    )
    operational_timeslice_snapshots = csv_str_to_df("""
        timeslice,  investment_periods,  snapshots
        peak,       2025,                2025-01-01 02:00:00
        shoulder,   2025,                2025-06-01 00:00:00
        peak,       2025,                2025-06-01 01:00:00
    """)
    _save_demand_trace(operational_snapshots, tmp_path)

    update_network_timeseries(
        network,
        tables,
        operational_snapshots,
        tmp_path,
        timeslice_snapshots=operational_timeslice_snapshots,
    )
    network.optimize.solve_model(solver_name="highs")

    # Peak is 0.5 per unit (50 MW), shoulder has CHEAP_CAP (30 MW), and a
    # snapshot in neither timeslice takes the fallback limit (100 MW). The two
    # shared snapshots take their operational timeslices: a limit kept from
    # capacity expansion would give 50 MW at 01:00 and 30 MW at 02:00.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            flow
        2025,                2025-01-01 01:00:00,  100.0
        2025,                2025-01-01 02:00:00,  50.0
        2025,                2025-06-01 00:00:00,  30.0
        2025,                2025-06-01 01:00:00,  50.0
    """)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])
    pd.testing.assert_frame_equal(
        _link_flow(network, "A-B_existing"), expected, check_exact=False
    )
