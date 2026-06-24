import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build.custom_constraints import (
    _add_custom_constraints_with_temporal_scope,
)


def _two_period_network() -> pypsa.Network:
    """Two investment periods (2025, 2030) with four hourly snapshots each,
    one bus, a cheap and an expensive generator, and a constant 100 MW load."""
    snapshots_2025 = pd.date_range("2025-01-01", periods=4, freq="h")
    snapshots_2030 = pd.date_range("2030-01-01", periods=4, freq="h")
    index = pd.MultiIndex.from_arrays(
        [[2025] * 4 + [2030] * 4, list(snapshots_2025) + list(snapshots_2030)]
    )
    network = pypsa.Network(snapshots=index, investment_periods=[2025, 2030])
    network.investment_period_weightings = pd.DataFrame(
        {"years": [5, 5], "objective": [1.0, 1.0]},
        index=pd.Index([2025, 2030], name="period"),
    )
    network.add("Bus", "bus1")
    network.add("Generator", "cheap_gen", bus="bus1", p_nom=200, marginal_cost=10)
    network.add("Generator", "dear_gen", bus="bus1", p_nom=200, marginal_cost=100)
    network.add("Load", "load1", bus="bus1")
    network.loads_t.p_set = pd.DataFrame({"load1": [100] * 8}, index=network.snapshots)
    return network


def _timeslice_snapshots(csv_str_to_df) -> pd.DataFrame:
    """peak_2025 tags the middle two snapshots of the 2025 period."""
    mapping = csv_str_to_df("""
        timeslice_id,  investment_periods,  snapshots
        peak_2025,     2025,                2025-01-01 01:00:00
        peak_2025,     2025,                2025-01-01 02:00:00
    """)
    return mapping


def test_timeslice_restricted_constraint_binds_only_at_tagged_snapshots(
    csv_str_to_df,
):
    network = _two_period_network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        cheap_gen_limit,  2025,               peak_2025,  40,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        cheap_gen_limit,  2025,               cheap_gen,      Generator,  p,          1.0
    """)

    network.optimize.create_model(multi_investment_periods=True)
    _add_custom_constraints_with_temporal_scope(
        network, rhs, lhs, _timeslice_snapshots(csv_str_to_df)
    )
    network.optimize.solve_model()

    cheap_gen_output = network.generators_t.p["cheap_gen"]
    # Constrained to 40 at the two tagged 2025 snapshots; free (meets the
    # whole 100 MW load) everywhere else, including all of 2030.
    expected = pd.Series(
        [100.0, 40.0, 40.0, 100.0, 100.0, 100.0, 100.0, 100.0],
        index=network.snapshots,
        name="cheap_gen",
    )
    pd.testing.assert_series_equal(cheap_gen_output, expected, check_names=False)


def test_rhs_varies_by_investment_period(csv_str_to_df):
    network = _two_period_network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        cheap_gen_limit,  2025,               ,           40,   <=
        cheap_gen_limit,  2030,               ,           60,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        cheap_gen_limit,  2025,               cheap_gen,      Generator,  p,          1.0
        cheap_gen_limit,  2030,               cheap_gen,      Generator,  p,          1.0
    """)

    network.optimize.create_model(multi_investment_periods=True)
    _add_custom_constraints_with_temporal_scope(
        network, rhs, lhs, _timeslice_snapshots(csv_str_to_df)
    )
    network.optimize.solve_model()

    cheap_gen_output = network.generators_t.p["cheap_gen"]
    assert (cheap_gen_output.loc[(2025,)] == 40.0).all()
    assert (cheap_gen_output.loc[(2030,)] == 60.0).all()


def test_unrestricted_p_nom_constraint(csv_str_to_df):
    """NaN investment_period and timeslice (the expansion-limit pattern)
    creates one global constraint over p_nom variables."""
    network = _two_period_network()
    network.add(
        "Generator",
        "exp_gen",
        bus="bus1",
        p_nom_extendable=True,
        capital_cost=1,
        marginal_cost=1,
        build_year=2025,
        lifetime=100,
    )
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        exp_limit,        ,                   ,           30,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        exp_limit,        ,                   exp_gen,        Generator,  p_nom,      1.0
    """)

    network.optimize.create_model(multi_investment_periods=True)
    _add_custom_constraints_with_temporal_scope(
        network, rhs, lhs, _timeslice_snapshots(csv_str_to_df)
    )
    network.optimize.solve_model()

    # exp_gen is the cheapest source so it builds to the constraint's cap.
    assert network.generators.loc["exp_gen", "p_nom_opt"] == pytest.approx(30.0)


def test_terms_for_components_not_in_model_skipped_and_logged(csv_str_to_df, caplog):
    """Generator and battery terms reference IASR IDs until generator
    translation lands — they are skipped per-miss with a log line and the
    rest of the constraint still applies."""
    network = _two_period_network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        cheap_gen_limit,  2025,               peak_2025,  40,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        cheap_gen_limit,  2025,               cheap_gen,      Generator,  p,          1.0
        cheap_gen_limit,  2025,               KINGASF1,       Generator,  p,          0.5
    """)

    network.optimize.create_model(multi_investment_periods=True)
    with caplog.at_level("INFO"):
        _add_custom_constraints_with_temporal_scope(
            network, rhs, lhs, _timeslice_snapshots(csv_str_to_df)
        )
    network.optimize.solve_model()

    assert (
        "Generator KINGASF1 not in model, custom constraint term skipped."
    ) in caplog.text
    # The cheap_gen term still binds at the tagged snapshots.
    assert network.generators_t.p["cheap_gen"].loc[(2025,)].iloc[1] == pytest.approx(
        40.0
    )


def test_constraint_with_no_terms_in_model_skipped(csv_str_to_df):
    network = _two_period_network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
        ghost_limit,      2025,               peak_2025,  40,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        ghost_limit,      2025,               KINGASF1,       Generator,  p,          1.0
    """)

    network.optimize.create_model(multi_investment_periods=True)
    _add_custom_constraints_with_temporal_scope(
        network, rhs, lhs, _timeslice_snapshots(csv_str_to_df)
    )
    network.optimize.solve_model()

    # No constraint applied — the cheap generator meets the whole load.
    assert (network.generators_t.p["cheap_gen"] == 100.0).all()


def test_constraint_for_timeslice_with_no_snapshots_skipped(csv_str_to_df):
    network = _two_period_network()
    rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,  constraint_type
        cheap_gen_limit,  2025,               tas_peak_demand,  40,   <=
    """)
    lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
        cheap_gen_limit,  2025,               cheap_gen,      Generator,  p,          1.0
    """)

    network.optimize.create_model(multi_investment_periods=True)
    _add_custom_constraints_with_temporal_scope(
        network, rhs, lhs, _timeslice_snapshots(csv_str_to_df)
    )
    network.optimize.solve_model()

    assert (network.generators_t.p["cheap_gen"] == 100.0).all()
