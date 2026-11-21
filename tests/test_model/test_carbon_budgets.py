import pandas as pd
import pypsa

from ispypsa.pypsa_build.carbon_budgets import (
    _add_carbon_budgets,
    _weight_snapshots_by_model_year,
)

_CARBON_BUDGET_COLUMNS = ["constraint_name", "year_from", "year_to", "rhs"]
_SNAPSHOT_COLUMNS = ["investment_periods", "snapshots", "model_year", "stores"]
_GENERATOR_COLUMNS = ["name", "emissions_intensity"]


def _network() -> pypsa.Network:
    """Two investment periods (2027 and 2031), each with one snapshot in each
    of two financial years, a 50 MW load, and four generators:

    - BW01 and TALWA1, which are in service throughout.
    - ER01, which retires after the 2027 investment period.
    - NQ OCGT_2031, which is built in the 2031 investment period.

    The linopy model is built, so constraints can be added.
    """
    periods = [2027, 2027, 2031, 2031]
    timesteps = pd.to_datetime(
        [
            "2026-07-01 12:00:00",
            "2027-07-01 12:00:00",
            "2030-07-01 12:00:00",
            "2031-07-01 12:00:00",
        ]
    )
    index = pd.MultiIndex.from_arrays([periods, timesteps])
    network = pypsa.Network(snapshots=index, investment_periods=[2027, 2031])
    network.add("Bus", "NSW")
    network.add("Generator", "BW01", bus="NSW", p_nom=100, marginal_cost=1)
    network.add("Generator", "TALWA1", bus="NSW", p_nom=100, marginal_cost=2)
    network.add(
        "Generator",
        "ER01",
        bus="NSW",
        p_nom=100,
        marginal_cost=1,
        build_year=2020,
        lifetime=8,
    )
    network.add(
        "Generator",
        "NQ OCGT_2031",
        bus="NSW",
        p_nom=100,
        marginal_cost=3,
        build_year=2031,
        lifetime=30,
    )
    network.add("Load", "load_NSW", bus="NSW", p_set=50)
    network.optimize.create_model(multi_investment_periods=True)
    return network


def _snapshots(csv_str_to_df) -> pd.DataFrame:
    """The network's snapshots as the pypsa-friendly snapshots table. Each is
    the only snapshot in its model year, so it represents all 8760 hours of
    it."""
    return csv_str_to_df("""
        investment_periods,  snapshots,            model_year,  stores
        2027,                2026-07-01 12:00:00,  2027,        12.0
        2027,                2027-07-01 12:00:00,  2028,        12.0
        2031,                2030-07-01 12:00:00,  2031,        12.0
        2031,                2031-07-01 12:00:00,  2032,        12.0
    """)


def _constraint_terms(network: pypsa.Network, name: str) -> pd.DataFrame:
    """Flattens one named linopy constraint into a DataFrame for comparison
    with assert_frame_equal: one row per term, sorted by snapshot and
    generator.

    Example row (0.00876 x Generator-p[BW01] at 2026-07-01 12:00, <= 290):
        investment_periods  snapshots            generator  coefficient  sign  rhs
        2027                2026-07-01 12:00:00  BW01       0.00876      <=    290.0
    """
    model = network.model
    flat = model.constraints[name].flat
    variables = [model.variables.get_label_position(label) for label in flat["vars"]]
    terms = pd.DataFrame(
        {
            "investment_periods": [coords["snapshot"][0] for _, coords in variables],
            "snapshots": [str(coords["snapshot"][1]) for _, coords in variables],
            "generator": [coords["name"] for _, coords in variables],
            "coefficient": flat["coeffs"].to_numpy(),
            "sign": flat["sign"].to_numpy(),
            "rhs": flat["rhs"].to_numpy(),
        }
    )
    return terms.sort_values(["snapshots", "generator"]).reset_index(drop=True)


def _carbon_budget_constraint_names(network: pypsa.Network) -> list[str]:
    """The names of the constraints not added by PyPSA itself."""
    pypsa_prefixes = ("Bus-", "Generator-", "Link-", "StorageUnit-", "Kirchhoff")
    return sorted(
        name
        for name in network.model.constraints
        if not name.startswith(pypsa_prefixes)
    )


# --- _add_carbon_budgets ---


def test_budget_sums_the_emitters_over_the_snapshots_in_its_window(csv_str_to_df):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2028,   2027,       2028,       290.0
    """)
    generators = csv_str_to_df("""
        name,       emissions_intensity
        BW01,       1.0
        TALWA1,     0.5
    """)

    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    assert _carbon_budget_constraint_names(network) == ["nem_2027_to_2028"]
    # Each snapshot represents 8760 hours, so a MW of BW01 (1.0 t/MWh) emits
    # 8760 t, or 0.00876 Mt, and a MW of TALWA1 (0.5 t/MWh) half that. Only the
    # 2027 and 2028 snapshots are in the window.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            generator,  coefficient,  sign,  rhs
        2027,                2026-07-01 12:00:00,  BW01,       0.00876,      <=,    290.0
        2027,                2026-07-01 12:00:00,  TALWA1,     0.00438,      <=,    290.0
        2027,                2027-07-01 12:00:00,  BW01,       0.00876,      <=,    290.0
        2027,                2027-07-01 12:00:00,  TALWA1,     0.00438,      <=,    290.0
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "nem_2027_to_2028"), expected, check_dtype=False
    )


def test_window_can_start_and_end_part_way_through_investment_periods(csv_str_to_df):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2028_to_2031,   2028,       2031,       100.0
    """)
    generators = csv_str_to_df("""
        name,       emissions_intensity
        BW01,       1.0
    """)

    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    # The window takes the second snapshot of the 2027 investment period
    # (model year 2028) and the first of the 2031 one (model year 2031), and
    # leaves out the other snapshot of each.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            generator,  coefficient,  sign,  rhs
        2027,                2027-07-01 12:00:00,  BW01,       0.00876,      <=,    100.0
        2031,                2030-07-01 12:00:00,  BW01,       0.00876,      <=,    100.0
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "nem_2028_to_2031"), expected, check_dtype=False
    )


def test_each_snapshot_is_weighted_by_its_share_of_its_model_year(csv_str_to_df):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2027,   2027,       2027,       290.0
    """)
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            model_year,  stores
        2027,                2026-07-01 12:00:00,  2027,        1.0
        2027,                2027-07-01 12:00:00,  2027,        3.0
        2031,                2030-07-01 12:00:00,  2031,        12.0
        2031,                2031-07-01 12:00:00,  2031,        12.0
    """)
    generators = csv_str_to_df("""
        name,       emissions_intensity
        BW01,       1.0
    """)

    _add_carbon_budgets(network, carbon_budgets, snapshots, generators)

    # Model year 2027 has four hours modelled. The first snapshot covers one of
    # them, so it represents a quarter of the year (2190 hours), and the second
    # for the other three quarters (6570 hours). The weighting is within the
    # model year: the 2031 snapshots, in another investment period and model
    # year, don't come into it.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            generator,  coefficient,  sign,  rhs
        2027,                2026-07-01 12:00:00,  BW01,       0.00219,      <=,    290.0
        2027,                2027-07-01 12:00:00,  BW01,       0.00657,      <=,    290.0
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "nem_2027_to_2027"), expected, check_dtype=False
    )


def test_generators_that_emit_nothing_are_left_out(csv_str_to_df):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2027,   2027,       2027,       50.0
    """)
    generators = csv_str_to_df("""
        name,       emissions_intensity
        BW01,       0.0
        TALWA1,     0.5
        ER01,
    """)

    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    # BW01's intensity is zero and ER01's is blank, so only TALWA1 has a term.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            generator,  coefficient,  sign,  rhs
        2027,                2026-07-01 12:00:00,  TALWA1,     0.00438,      <=,    50.0
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "nem_2027_to_2027"), expected, check_dtype=False
    )


def test_generators_add_nothing_outside_their_build_and_retirement_years(
    csv_str_to_df,
):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2032,   2027,       2032,       500.0
    """)
    generators = csv_str_to_df("""
        name,           emissions_intensity
        ER01,           1.0
        NQ OCGT_2031,   0.5
    """)

    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    # ER01 (built 2020, 8 year life) has retired by the 2031 investment period,
    # and NQ OCGT_2031 isn't built until it, so each has terms in one
    # investment period only.
    expected = csv_str_to_df("""
        investment_periods,  snapshots,            generator,     coefficient,  sign,  rhs
        2027,                2026-07-01 12:00:00,  ER01,          0.00876,      <=,    500.0
        2027,                2027-07-01 12:00:00,  ER01,          0.00876,      <=,    500.0
        2031,                2030-07-01 12:00:00,  NQ OCGT_2031,  0.00438,      <=,    500.0
        2031,                2031-07-01 12:00:00,  NQ OCGT_2031,  0.00438,      <=,    500.0
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "nem_2027_to_2032"), expected, check_dtype=False
    )


def test_each_budget_gets_its_own_constraint(csv_str_to_df):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2032,   2027,       2032,       500.0
        nem_2031_to_2031,   2031,       2031,       20.0
    """)
    generators = csv_str_to_df("""
        name,       emissions_intensity
        BW01,       1.0
    """)

    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    assert _carbon_budget_constraint_names(network) == [
        "nem_2027_to_2032",
        "nem_2031_to_2031",
    ]
    # The 2031 snapshot counts against both budgets.
    expected_long = csv_str_to_df("""
        investment_periods,  snapshots,            generator,  coefficient,  sign,  rhs
        2027,                2026-07-01 12:00:00,  BW01,       0.00876,      <=,    500.0
        2027,                2027-07-01 12:00:00,  BW01,       0.00876,      <=,    500.0
        2031,                2030-07-01 12:00:00,  BW01,       0.00876,      <=,    500.0
        2031,                2031-07-01 12:00:00,  BW01,       0.00876,      <=,    500.0
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "nem_2027_to_2032"),
        expected_long,
        check_dtype=False,
    )
    expected_single_year = csv_str_to_df("""
        investment_periods,  snapshots,            generator,  coefficient,  sign,  rhs
        2031,                2030-07-01 12:00:00,  BW01,       0.00876,      <=,    20.0
    """)
    pd.testing.assert_frame_equal(
        _constraint_terms(network, "nem_2031_to_2031"),
        expected_single_year,
        check_dtype=False,
    )


def test_no_constraints_added_without_budgets(csv_str_to_df):
    network = _network()
    carbon_budgets = pd.DataFrame(columns=_CARBON_BUDGET_COLUMNS)
    generators = csv_str_to_df("""
        name,       emissions_intensity
        BW01,       1.0
    """)

    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    assert _carbon_budget_constraint_names(network) == []


def test_no_constraints_added_without_emitting_generators(csv_str_to_df):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2028,   2027,       2028,       290.0
    """)
    generators = pd.DataFrame(columns=_GENERATOR_COLUMNS)

    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    assert _carbon_budget_constraint_names(network) == []


def test_no_constraints_added_without_budgets_or_emitting_generators():
    network = _network()
    carbon_budgets = pd.DataFrame(columns=_CARBON_BUDGET_COLUMNS)
    snapshots = pd.DataFrame(columns=_SNAPSHOT_COLUMNS)
    generators = pd.DataFrame(columns=_GENERATOR_COLUMNS)

    _add_carbon_budgets(network, carbon_budgets, snapshots, generators)

    assert _carbon_budget_constraint_names(network) == []


def test_budget_limits_emissions_in_the_solved_model(csv_str_to_df):
    network = _network()
    carbon_budgets = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2028,   2027,       2028,       0.438
    """)
    generators = csv_str_to_df("""
        name,       emissions_intensity
        BW01,       1.0
        ER01,       1.0
        TALWA1,     0.0
    """)
    _add_carbon_budgets(network, carbon_budgets, _snapshots(csv_str_to_df), generators)

    network.optimize.solve_model(solver_name="highs")

    output = network.generators_t.p.loc[2027]
    emitters_output = (output["BW01"] + output["ER01"]).sum()
    result = pd.DataFrame({"emitters_output": [emitters_output]})
    # BW01 and ER01 are the cheapest, so without the budget they would meet
    # the whole 50 MW load in both 2027 snapshots (100 MW in total), emitting
    # 100 MW x 8760 h x 1 t/MWh = 0.876 Mt. The budget is half that, so their
    # output over the two snapshots is held to 50 MW and clean TALWA1 covers
    # the rest.
    expected = csv_str_to_df("""
        emitters_output
        50.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False)


# --- _weight_snapshots_by_model_year ---


def test_weight_snapshots_by_model_year(csv_str_to_df):
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            model_year,  stores
        2027,                2026-07-01 01:00:00,  2027,        1.0
        2027,                2026-07-01 04:00:00,  2027,        3.0
        2027,                2027-07-01 12:00:00,  2028,        12.0
        2031,                2030-07-01 12:00:00,  2031,        12.0
    """)

    result = _weight_snapshots_by_model_year(snapshots)

    # Model year 2027 has four hours modelled, split one to three between its
    # snapshots. 2028 has a single snapshot, which represents the whole year
    # even though it shares an investment period with 2027's. The snapshots
    # arrive as text, as they would from a CSV, and come back as timestamps.
    expected = csv_str_to_df("""
        period,  timestep,             model_year,  hours_represented
        2027,    2026-07-01 01:00:00,  2027,        2190.0
        2027,    2026-07-01 04:00:00,  2027,        6570.0
        2027,    2027-07-01 12:00:00,  2028,        8760.0
        2031,    2030-07-01 12:00:00,  2031,        8760.0
    """)
    expected["timestep"] = pd.to_datetime(expected["timestep"])
    pd.testing.assert_frame_equal(result.reset_index(), expected)
