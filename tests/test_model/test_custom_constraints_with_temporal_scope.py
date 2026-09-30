import numpy as np
import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build.custom_constraints import (
    _add_custom_constraints_with_temporal_scope,
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


def _network() -> pypsa.Network:
    """Two investment periods of two hourly snapshots, with one of each
    component a custom constraint term can reference, and the linopy model
    built so constraints can be added."""
    periods = [2025, 2025, 2030, 2030]
    snapshots = pd.to_datetime(
        [
            "2025-01-01 00:00",
            "2025-01-01 01:00",
            "2030-01-01 00:00",
            "2030-01-01 01:00",
        ]
    )
    index = pd.MultiIndex.from_arrays([periods, snapshots])
    network = pypsa.Network(snapshots=index, investment_periods=[2025, 2030])
    network.add("Bus", ["NQ", "SQ"])
    network.add("Link", "NSW-QLD_existing", bus0="NQ", bus1="SQ", p_nom=1000)
    network.add(
        "Link",
        "NSW-QLD_exp_2025",
        bus0="NQ",
        bus1="SQ",
        p_nom_extendable=True,
        capital_cost=1,
    )
    network.add("Generator", "KINGASF1", bus="SQ", p_nom=100, marginal_cost=1)
    network.add("StorageUnit", "Q8 Battery - 2h", bus="SQ", p_nom=50, max_hours=2)
    load = pd.Series([600.0, 700.0, 800.0, 900.0], index=network.snapshots)
    network.add("Load", "load_SQ", bus="SQ", p_set=load)
    network.optimize.create_model(multi_investment_periods=True)
    return network


def _constraint_terms(network: pypsa.Network, name: str) -> pd.DataFrame:
    """Flattens one named linopy constraint into a DataFrame for comparison
    with assert_frame_equal: one row per (snapshot, variable) term, sorted.

    The snapshot is the constraint row's own, so it is blank only for a
    constraint with no snapshot dimension (e.g. an expansion limit). The rhs
    includes any load offset linopy moved across.

    Example row (0.84 x Link-p[NSW-QLD_existing] <= 3000 at 01:00):
        investment_periods  snapshots            variable                  coefficient  sign  rhs
        2025                2025-01-01 01:00:00  Link-p[NSW-QLD_existing]  0.84         <=    3000
    """
    model = network.model
    flat = model.constraints[name].flat
    rows = [model.constraints.get_label_position(label) for label in flat["labels"]]
    snapshots = [coords.get("snapshot", (np.nan, np.nan)) for _, coords in rows]
    variables = [model.variables.get_label_position(label) for label in flat["vars"]]
    terms = pd.DataFrame(
        {
            "investment_periods": [period for period, _ in snapshots],
            "snapshots": [t if pd.isna(t) else str(t) for _, t in snapshots],
            "variable": [f"{var}[{coords['name']}]" for var, coords in variables],
            "coefficient": flat["coeffs"].to_numpy(),
            "sign": flat["sign"].to_numpy(),
            "rhs": flat["rhs"].to_numpy(),
        }
    )
    return terms.sort_values(["snapshots", "variable"]).reset_index(drop=True)


def _custom_constraint_names(network: pypsa.Network) -> list[str]:
    """The names of the constraints not added by PyPSA itself."""
    pypsa_prefixes = ("Bus-", "Generator-", "Link-", "StorageUnit-", "Kirchhoff")
    return sorted(
        name
        for name in network.model.constraints
        if not name.startswith(pypsa_prefixes)
    )


def test_named_timeslice_binds_only_at_its_snapshots(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2025,               qld_peak_demand,  3000,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD_existing,  Link,       p,          0.84
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
        qld_peak_demand,  2030,                2030-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    assert _custom_constraint_names(network) == ["SWQLD1_2025_qld_peak_demand"]
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                     coefficient,  sign,  rhs
        2025,                2025-01-01 01:00:00,  Link-p[NSW-QLD_existing],     0.84,         <=,    3000
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD1_2025_qld_peak_demand"),
        expected,
        check_dtype=False,
    )


def test_fallback_applies_only_where_named_timeslices_do_not(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2025,               qld_peak_demand,  3000,  <=
        SWQLD1,           2025,               ,                 3500,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD_existing,  Link,       p,          0.84
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    assert _custom_constraint_names(network) == [
        "SWQLD1_2025",
        "SWQLD1_2025_qld_peak_demand",
    ]
    # The fallback holds at 2025's off-peak snapshot only: not at the peak
    # snapshot its named row covers, and not in 2030.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                  coefficient,  sign,  rhs
        2025,                2025-01-01 00:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3500
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD1_2025"), expected, check_dtype=False
    )


def test_fallback_ignores_other_constraints_named_timeslices(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2025,               qld_peak_demand,  3000,  <=
        SWQLD2,           2025,               ,                 3500,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD2,           2025,               NSW-QLD_existing,  Link,       p,          0.84
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    # SWQLD1's peak timeslice doesn't carve 01:00 out of SWQLD2's fallback:
    # SWQLD2 has no named rows, so its fallback covers all of 2025.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                  coefficient,  sign,  rhs
        2025,                2025-01-01 00:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3500
        2025,                2025-01-01 01:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3500
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD2_2025"), expected, check_dtype=False
    )


def test_fallback_only_constraint_applies_across_its_period(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,   constraint_type
        SWQLD1,           2030,               ,           3500,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2030,               NSW-QLD_existing,  Link,       p,          0.84
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    expected = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                  coefficient,  sign,  rhs
        2030,                2030-01-01 00:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3500
        2030,                2030-01-01 01:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3500
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD1_2030"), expected, check_dtype=False
    )


def test_lhs_terms_apply_only_in_their_own_investment_period(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,   constraint_type
        SWQLD1,           2025,               ,           3500,  <=
        SWQLD1,           2030,               ,           3500,  <=
    """)
    # The blank-period term matches neither period-specific RHS row: blank
    # periods only pair with blank periods (the expansion-limit shape). The
    # translator doesn't currently emit a blank-period term alongside
    # period-specific ones; it's included for completeness, to pin down that
    # such a term is left out rather than added in every period.
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,           2030,               NSW-QLD_existing,  Link,       p,          0.9
        SWQLD1,           2025,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,           ,                   NSW-QLD_exp_2025,  Link,       p,          1.0
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    expected_2025 = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                  coefficient,  sign,  rhs
        2025,                2025-01-01 00:00:00,  Generator-p[KINGASF1],     0.14,         <=,    3500
        2025,                2025-01-01 00:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3500
        2025,                2025-01-01 01:00:00,  Generator-p[KINGASF1],     0.14,         <=,    3500
        2025,                2025-01-01 01:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3500
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD1_2025"), expected_2025, check_dtype=False
    )
    expected_2030 = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                  coefficient,  sign,  rhs
        2030,                2030-01-01 00:00:00,  Link-p[NSW-QLD_existing],  0.9,          <=,    3500
        2030,                2030-01-01 01:00:00,  Link-p[NSW-QLD_existing],  0.9,          <=,    3500
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD1_2030"), expected_2030, check_dtype=False
    )


def test_expansion_limit_has_no_snapshot_dimension(csv_str_to_df):
    # The translator's expansion-limit shape: blank investment_period and
    # timeslice, p_nom terms only. An all-blank timeslice column reads as
    # float64 (Open-ISP/ISPyPSA#138), which this also exercises.
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,  rhs,   constraint_type
        NSW-QLD_expansion_limit,  ,                   ,           1000,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2025,  Link,       p_nom,      1.0
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    expected = csv_str_to_df("""
        investment_periods,  snapshots,  variable,                      coefficient,  sign,  rhs
        ,                    ,           Link-p_nom[NSW-QLD_exp_2025],  1.0,          <=,    1000
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "NSW-QLD_expansion_limit"),
        expected,
        check_dtype=False,
    )


def test_storage_term_is_net_dispatch(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2025,               qld_peak_demand,  3000,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               Q8__Battery__-__2h,  Storage,    p,          0.43
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    expected = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                                    coefficient,  sign,  rhs
        2025,                2025-01-01 01:00:00,  StorageUnit-p_dispatch[Q8__Battery__-__2h],  0.43,         <=,    3000
        2025,                2025-01-01 01:00:00,  StorageUnit-p_store[Q8__Battery__-__2h],     -0.43,        <=,    3000
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD1_2025_qld_peak_demand"),
        expected,
        check_dtype=False,
    )


def test_load_term_offsets_the_rhs_at_each_snapshot(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,   constraint_type
        SWQLD1,           2025,               ,           3000,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,           2025,               load_SQ,           Load,       p_set,      -0.33
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    # 3000 + 0.33 x load: 600 at 00:00, 700 at 01:00.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                  coefficient,  sign,  rhs
        2025,                2025-01-01 00:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3198
        2025,                2025-01-01 01:00:00,  Link-p[NSW-QLD_existing],  0.84,         <=,    3231
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "SWQLD1_2025"), expected, check_dtype=False
    )


def test_load_term_gives_a_p_nom_only_constraint_a_snapshot_dimension(
    csv_str_to_df,
):
    # Build at least 10% of demand: the load term varies by snapshot, so the
    # capacity-only constraint holds once per snapshot. The rows must sit on
    # the network's "snapshot" dimension: on a stray unnamed one, their
    # snapshots would come back blank and the comparison would fail. The
    # translator doesn't currently emit a p_nom-only constraint with a load
    # term; this pins down how the current implementation handles one.
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        FLOOR,            2025,               ,           0,    >=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        FLOOR,            2025,               NSW-QLD_exp_2025,  Link,       p_nom,      1.0
        FLOOR,            2025,               load_SQ,           Load,       p_set,      -0.1
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    # 0 + 0.1 x load: 600 at 00:00, 700 at 01:00.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            variable,                      coefficient,  sign,  rhs
        2025,                2025-01-01 00:00:00,  Link-p_nom[NSW-QLD_exp_2025],  1.0,          >=,    60
        2025,                2025-01-01 01:00:00,  Link-p_nom[NSW-QLD_exp_2025],  1.0,          >=,    70
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "FLOOR_2025"), expected, check_dtype=False
    )


def test_constraint_types_set_the_sign(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        FLOOR,            ,                   ,           10,   >=
        FIXED,            ,                   ,           20,   ==
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        FLOOR,            ,                   NSW-QLD_exp_2025,  Link,       p_nom,      1.0
        FIXED,            ,                   NSW-QLD_exp_2025,  Link,       p_nom,      1.0
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    expected_floor = csv_str_to_df("""
        investment_periods,  snapshots,  variable,                      coefficient,  sign,  rhs
        ,                    ,           Link-p_nom[NSW-QLD_exp_2025],  1.0,          >=,    10
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "FLOOR"), expected_floor, check_dtype=False
    )
    expected_fixed = csv_str_to_df("""
        investment_periods,  snapshots,  variable,                      coefficient,  sign,  rhs
        ,                    ,           Link-p_nom[NSW-QLD_exp_2025],  1.0,          =,     20
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "FIXED"), expected_fixed, check_dtype=False
    )


def test_named_timeslice_with_no_snapshots_is_skipped(csv_str_to_df):
    # e.g. tas_peak_demand, which never activates in the Draft 2026 ISP calendar.
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2025,               tas_peak_demand,  3000,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD_existing,  Link,       p,          0.84
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    assert _custom_constraint_names(network) == []


def test_unsupported_term_raises(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,   constraint_type
        SWQLD1,           2025,               ,           3000,  <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        SWQLD1,           2025,               SQ,             Bus,        p,          1.0
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    with pytest.raises(
        ValueError,
        match=r"unsupported \(component, attribute\): \[\('Bus', 'p'\)\]",
    ):
        _add_custom_constraints_with_temporal_scope(
            network, rhs, lhs, timeslice_snapshots
        )


def test_rhs_row_without_variable_terms_raises(csv_str_to_df):
    network = _network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,   constraint_type
        SWQLD1,           2025,               ,           3000,  <=
    """)
    lhs = pd.DataFrame(columns=_LHS_COLUMNS)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    with pytest.raises(
        ValueError,
        match="Custom constraint SWQLD1_2025 has no LHS terms on model variables.",
    ):
        _add_custom_constraints_with_temporal_scope(
            network, rhs, lhs, timeslice_snapshots
        )


def test_empty_rhs_adds_no_constraints(csv_str_to_df):
    network = _network()
    rhs = pd.DataFrame(columns=_RHS_COLUMNS)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2025,               NSW-QLD_existing,  Link,       p,          0.84
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    assert _custom_constraint_names(network) == []


def test_both_tables_empty_adds_no_constraints():
    network = _network()
    rhs = pd.DataFrame(columns=_RHS_COLUMNS)
    lhs = pd.DataFrame(columns=_LHS_COLUMNS)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)

    assert _custom_constraint_names(network) == []


def _two_bus_network(
    snapshots: list[tuple[int, str]], load: list[float], link_p_nom: float = 100
) -> pypsa.Network:
    """A small network to solve: cheap generation at NQ and expensive
    generation at SQ, joined by the NSW-QLD_existing link, serving the SQ load
    given per snapshot. Unconstrained, the link carries the whole load, so a
    custom constraint on the link shows up as the expensive generator making
    up the difference.

    snapshots are (investment period, timestamp) pairs, e.g.
    [(2025, "2025-01-01 00:00"), (2025, "2025-01-01 01:00")].
    """
    periods = [period for period, _ in snapshots]
    timestamps = pd.to_datetime([timestamp for _, timestamp in snapshots])
    index = pd.MultiIndex.from_arrays([periods, timestamps])
    network = pypsa.Network(snapshots=index, investment_periods=sorted(set(periods)))
    network.add("Bus", ["NQ", "SQ"])
    network.add("Link", "NSW-QLD_existing", bus0="NQ", bus1="SQ", p_nom=link_p_nom)
    network.add("Generator", "cheap", bus="NQ", p_nom=100, marginal_cost=1)
    network.add("Generator", "expensive", bus="SQ", p_nom=100, marginal_cost=100)
    network.add("Load", "load_SQ", bus="SQ", p_set=pd.Series(load, index=index))
    return network


_ONE_PERIOD = [(2025, "2025-01-01 00:00"), (2025, "2025-01-01 01:00")]
_TWO_PERIODS = _ONE_PERIOD + [(2030, "2030-01-01 00:00"), (2030, "2030-01-01 01:00")]


def test_peak_only_constraint_binds_at_peak_in_the_solved_model(csv_str_to_df):
    # Cheap generation at NQ serves the SQ load over the link; a peak-only
    # cap on the link's flow forces the expensive SQ generator on at the peak
    # snapshot only.
    network = _two_bus_network(_ONE_PERIOD, load=[50, 50])
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,  constraint_type
        FLOWCAP,          2025,               qld_peak_demand,  20,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        FLOWCAP,          2025,               NSW-QLD_existing,  Link,       p,          1.0
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    flows = network.links_t.p0["NSW-QLD_existing"].reset_index(drop=True)
    expected = pd.Series([50.0, 20.0], name="NSW-QLD_existing")
    pd.testing.assert_series_equal(flows, expected, check_names=False)


def test_named_and_fallback_rows_split_a_period_in_the_solved_model(
    csv_str_to_df,
):
    # Each snapshot is bound by exactly one row: the peak cap at 01:00 and
    # the fallback cap everywhere else in 2025. Both applying at a snapshot,
    # or neither, would show up as a 20 or a 50 in the wrong place.
    network = _two_bus_network(_ONE_PERIOD, load=[50, 50])
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,  constraint_type
        FLOWCAP,          2025,               qld_peak_demand,  20,   <=
        FLOWCAP,          2025,               ,                 40,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        FLOWCAP,          2025,               NSW-QLD_existing,  Link,       p,          1.0
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    flows = network.links_t.p0["NSW-QLD_existing"].reset_index(drop=True)
    expected = pd.Series([40.0, 20.0])
    pd.testing.assert_series_equal(flows, expected, check_names=False, atol=1e-6)


def test_each_investment_period_uses_its_own_rhs_in_the_solved_model(
    csv_str_to_df,
):
    network = _two_bus_network(_TWO_PERIODS, load=[50, 50, 50, 50])
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        FLOWCAP,          2025,               ,           20,   <=
        FLOWCAP,          2030,               ,           40,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        FLOWCAP,          2025,               NSW-QLD_existing,  Link,       p,          1.0
        FLOWCAP,          2030,               NSW-QLD_existing,  Link,       p,          1.0
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    flows = network.links_t.p0["NSW-QLD_existing"].reset_index(drop=True)
    expected = pd.Series([20.0, 20.0, 40.0, 40.0])
    pd.testing.assert_series_equal(flows, expected, check_names=False, atol=1e-6)


def test_expansion_limit_caps_the_build_in_the_solved_model(csv_str_to_df):
    # With no existing link capacity, building the expansion link is far
    # cheaper than running the expensive generator, so unconstrained it would
    # be built to carry the whole 50 MW load. The expansion limit (blank
    # period and timeslice, p_nom term) caps it at 30.
    network = _two_bus_network(_ONE_PERIOD, load=[50, 50], link_p_nom=0)
    network.add(
        "Link",
        "NSW-QLD_exp_2025",
        bus0="NQ",
        bus1="SQ",
        p_nom_extendable=True,
        capital_cost=1,
    )
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,  rhs,  constraint_type
        NSW-QLD_expansion_limit,  ,                   ,           30,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2025,  Link,       p_nom,      1.0
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    built = network.links.loc[["NSW-QLD_exp_2025"], "p_nom_opt"]
    expected = pd.Series([30.0], index=["NSW-QLD_exp_2025"])
    pd.testing.assert_series_equal(built, expected, check_names=False, atol=1e-6)


def test_load_term_scales_the_limit_with_demand_in_the_solved_model(
    csv_str_to_df,
):
    # flow - 0.5 x load <= 0, i.e. the link carries at most half the load.
    # The load term moves to the right-hand side, so a sign error there would
    # loosen or invert the limit rather than track demand.
    network = _two_bus_network(_ONE_PERIOD, load=[40, 60])
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        HALFLOAD,         2025,               ,           0,    <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        HALFLOAD,         2025,               NSW-QLD_existing,  Link,       p,          1.0
        HALFLOAD,         2025,               load_SQ,           Load,       p_set,      -0.5
    """)
    timeslice_snapshots = pd.DataFrame(
        columns=["timeslice", "investment_periods", "snapshots"]
    )

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    flows = network.links_t.p0["NSW-QLD_existing"].reset_index(drop=True)
    expected = pd.Series([20.0, 30.0])
    pd.testing.assert_series_equal(flows, expected, check_names=False, atol=1e-6)


def test_generator_floor_binds_only_in_its_timeslice_in_the_solved_model(
    csv_str_to_df,
):
    # A >= row on a generator term: the expensive generator, idle when
    # unconstrained, must run at 10 MW at the peak and nowhere else.
    network = _two_bus_network(_ONE_PERIOD, load=[50, 50])
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,  constraint_type
        FLOOR,            2025,               qld_peak_demand,  10,   >=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        FLOOR,            2025,               expensive,      Generator,  p,          1.0
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    dispatch = network.generators_t.p["expensive"].reset_index(drop=True)
    expected = pd.Series([0.0, 10.0])
    pd.testing.assert_series_equal(dispatch, expected, check_names=False, atol=1e-6)


def test_storage_term_constrains_net_dispatch_in_the_solved_model(csv_str_to_df):
    # Net dispatch (discharging minus charging) >= 10 at the peak. The
    # battery starts empty, so it must charge 10 at 00:00 to discharge 10 at
    # 01:00. Its dispatch cost stops it cycling more than it has to. Were
    # charging counted as positive, charging 10 at the peak would satisfy
    # the constraint instead, giving [0, -10].
    network = _two_bus_network(_ONE_PERIOD, load=[50, 50])
    network.add(
        "StorageUnit", "battery", bus="SQ", p_nom=50, max_hours=2, marginal_cost=0.1
    )
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,  constraint_type
        DISCHARGE,        2025,               qld_peak_demand,  10,   >=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        DISCHARGE,        2025,               battery,        Storage,    p,          1.0
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,        investment_periods,  snapshots
        qld_peak_demand,  2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    net_dispatch = network.storage_units_t.p["battery"].reset_index(drop=True)
    expected = pd.Series([-10.0, 10.0])
    pd.testing.assert_series_equal(net_dispatch, expected, check_names=False, atol=1e-6)


def test_overlapping_timeslice_constraints_both_bind_in_the_solved_model(
    csv_str_to_df,
):
    # Regions' timeslices overlap: nsw_summer_typical covers both snapshots
    # and qld_peak_demand covers 01:00. Each constraint binds over its own
    # timeslice, so at 01:00 both apply and the tighter QLD cap wins.
    network = _two_bus_network(_ONE_PERIOD, load=[50, 50])
    network.optimize.create_model(multi_investment_periods=True)
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,           rhs,  constraint_type
        QLDCAP,           2025,               qld_peak_demand,     20,   <=
        NSWCAP,           2025,               nsw_summer_typical,  30,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        QLDCAP,           2025,               NSW-QLD_existing,  Link,       p,          1.0
        NSWCAP,           2025,               NSW-QLD_existing,  Link,       p,          1.0
    """)
    timeslice_snapshots = csv_str_to_df("""
        timeslice,           investment_periods,  snapshots
        nsw_summer_typical,  2025,                2025-01-01 00:00:00
        nsw_summer_typical,  2025,                2025-01-01 01:00:00
        qld_peak_demand,     2025,                2025-01-01 01:00:00
    """)

    _add_custom_constraints_with_temporal_scope(network, rhs, lhs, timeslice_snapshots)
    network.optimize.solve_model()

    flows = network.links_t.p0["NSW-QLD_existing"].reset_index(drop=True)
    expected = pd.Series([30.0, 20.0])
    pd.testing.assert_series_equal(flows, expected, check_names=False, atol=1e-6)
