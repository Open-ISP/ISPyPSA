import pandas as pd
import pytest

from ispypsa.translator.constraints import (
    _translate_custom_constraints,
)
from ispypsa.translator.helpers import _annuitised_investment_costs

# Annuitised $1/MW at the sample_model_config's wacc (0.06) and annuitisation
# lifetime (25).
_ANNUITY_PER_DOLLAR = _annuitised_investment_costs(1.0, 0.06, 25)


def _constraint_tables(csv_str_to_df) -> dict[str, pd.DataFrame]:
    """One PLEXOS-derived constraint (SWQLD1) with a link, a generator and a
    storage term, timeslice-varying RHS, and a relaxation expansion option.
    The sample_model_config's investment periods are 2026 and 2028."""
    tables = {}
    tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         <=
    """)
    tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,    coefficient,  date_from
        SWQLD1,         link_flow,         NSW-QLD,          0.84,
        SWQLD1,         generator_output,  KINGASF1,         0.14,
        SWQLD1,         storage_output,    Q8 Battery - 2h,  0.43,
    """)
    tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,             rhs,   date_from
        SWQLD1,         qld_peak_demand,       3000,
        SWQLD1,         qld_winter_reference,  3500,
    """)
    tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        NSW-QLD,       forward,                1000,               NSW-QLD Option 1
        NSW-QLD,       reverse,                900,                NSW-QLD Option 1
        SWQLD1,        constraint_relaxation,  400,                SWQLD1 Option 2
    """)
    tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        NSW-QLD,       2026,  500000
        NSW-QLD,       2028,  500000
        SWQLD1,        2026,  100000
        SWQLD1,        2028,  80000
    """)
    return tables


def _links(csv_str_to_df) -> pd.DataFrame:
    return csv_str_to_df("""
        isp_name,  name,              p_nom_extendable,  build_year,  lifetime
        NSW-QLD,   NSW-QLD_existing,  False,             2025,        inf
        NSW-QLD,   NSW-QLD_exp_2026,  True,              2026,        inf
    """)


def _generators(csv_str_to_df) -> pd.DataFrame:
    """Existing units carry their own name as isp_name, a build year before
    the first investment period, and an infinite lifetime (no scheduled
    closure). LATEGEN backs the date_from-after-all-periods test."""
    return csv_str_to_df("""
        isp_name,  name,      build_year,  lifetime
        KINGASF1,  KINGASF1,  2025,        inf
        LATEGEN,   LATEGEN,   2025,        inf
    """)


def _storage(csv_str_to_df) -> pd.DataFrame:
    return csv_str_to_df("""
        isp_name,         name,             build_year,  lifetime
        Q8 Battery - 2h,  Q8 Battery - 2h,  2025,        inf
    """)


def _demand_nodes(csv_str_to_df) -> pd.DataFrame:
    """The buses with demand attached — at the fixture's sub_regions
    granularity, the sub-region buses."""
    return csv_str_to_df("""
        name
        SQ
    """)


def test_translate_custom_constraints_rhs(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,             rhs,   constraint_type
        SWQLD1,                   2026,               qld_peak_demand,       3000,  <=
        SWQLD1,                   2026,               qld_winter_reference,  3500,  <=
        SWQLD1,                   2028,               qld_peak_demand,       3000,  <=
        SWQLD1,                   2028,               qld_winter_reference,  3500,  <=
        NSW-QLD_expansion_limit,  ,                   ,                      1000,  <=
        SWQLD1_expansion_limit,   ,                   ,                      400,   <=
    """)
    sort_cols = ["constraint_name", "investment_period", "timeslice"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_rhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_rhs_row_with_blank_timeslice_is_kept_as_fallback(
    csv_str_to_df, sample_model_config
):
    """A blank-timeslice RHS row is the constraint's fallback limit, applying
    at the snapshots no named-timeslice row covers, and passes through
    alongside the named rows rather than being dropped as a NaN group."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        SWQLD1,         ,                 2800,
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,                   2026,               qld_peak_demand,  3000,  <=
        SWQLD1,                   2026,               ,                 2800,  <=
        SWQLD1,                   2028,               qld_peak_demand,  3000,  <=
        SWQLD1,                   2028,               ,                 2800,  <=
        NSW-QLD_expansion_limit,  ,                   ,                 1000,  <=
        SWQLD1_expansion_limit,   ,                   ,                 400,   <=
    """)
    sort_cols = ["constraint_name", "investment_period", "timeslice"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_rhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_translate_custom_constraints_lhs(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,       component,  attribute,  coefficient
        SWQLD1,                   2026,               NSW-QLD_existing,    Link,       p,          0.84
        SWQLD1,                   2026,               NSW-QLD_exp_2026,    Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_existing,    Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_exp_2026,    Link,       p,          0.84
        SWQLD1,                   2026,               KINGASF1,            Generator,  p,          0.14
        SWQLD1,                   2028,               KINGASF1,            Generator,  p,          0.14
        SWQLD1,                   2026,               Q8 Battery - 2h,     Storage,    p,          0.43
        SWQLD1,                   2028,               Q8 Battery - 2h,     Storage,    p,          0.43
        SWQLD1,                   2026,               SWQLD1_exp_2026,     Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,     Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,     Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,    Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,     Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,     Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_translate_custom_constraints_relaxation_generators(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_generators = csv_str_to_df(f"""
        name,             isp_name,  bus,                             p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
        SWQLD1_exp_2026,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2026,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
        SWQLD1_exp_2028,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2028,        inf,       {80000 * _ANNUITY_PER_DOLLAR}
    """)
    generators = result["custom_constraints_generators"]
    pd.testing.assert_frame_equal(
        generators.sort_values("name").reset_index(drop=True),
        expected_generators,
        check_dtype=False,
        rtol=1e-5,
    )


def test_translate_custom_constraints_rez_expansion_disabled(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    sample_model_config.network.rez_transmission_expansion = False

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_generators = csv_str_to_df("""
        name,  isp_name,  bus,  p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_generators"], expected_generators, check_dtype=False
    )
    # No relaxation terms and no SWQLD1_expansion_limit; the path's expansion
    # limit is unaffected by the flag.
    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,                   2026,               NSW-QLD_exp_2026,  Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_exp_2026,  Link,       p,          0.84
        SWQLD1,                   2026,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2028,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2026,               Q8 Battery - 2h,   Storage,    p,          0.43
        SWQLD1,                   2028,               Q8 Battery - 2h,   Storage,    p,          0.43
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
    """)
    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,             rhs,   constraint_type
        SWQLD1,                   2026,               qld_peak_demand,       3000,  <=
        SWQLD1,                   2026,               qld_winter_reference,  3500,  <=
        SWQLD1,                   2028,               qld_peak_demand,       3000,  <=
        SWQLD1,                   2028,               qld_winter_reference,  3500,  <=
        NSW-QLD_expansion_limit,  ,                   ,                      1000,  <=
    """)
    _assert_lhs_and_rhs_equal(result, expected_lhs, expected_rhs)


def test_date_from_resolved_at_period_starts(csv_str_to_df, sample_model_config):
    """A value dated mid-FY2027 (i.e. before FY2028 starts on 2027-07-01) does
    not apply in the 2026 period but does in 2028."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        SWQLD1,         qld_peak_demand,  2500,  2026-12-01T00:00:00
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    rhs = result["custom_constraints_rhs"]
    rhs = rhs[rhs["constraint_name"] == "SWQLD1"]
    expected = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2026,               qld_peak_demand,  3000,  <=
        SWQLD1,           2028,               qld_peak_demand,  2500,  <=
    """)
    pd.testing.assert_frame_equal(
        rhs.sort_values("investment_period").reset_index(drop=True),
        expected,
        check_dtype=False,
    )


def test_date_from_exactly_on_period_start_applies_in_that_period(
    csv_str_to_df, sample_model_config
):
    """The value active at a period's start includes one dated exactly on it:
    FY2028 starts 2027-07-01, so a 2027-07-01 value supersedes the baseline
    for 2028 — the boundary is inclusive."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        SWQLD1,         qld_peak_demand,  2500,  2027-07-01T00:00:00
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    rhs = result["custom_constraints_rhs"]
    rhs = rhs[rhs["constraint_name"] == "SWQLD1"]
    expected = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2026,               qld_peak_demand,  3000,  <=
        SWQLD1,           2028,               qld_peak_demand,  2500,  <=
    """)
    pd.testing.assert_frame_equal(
        rhs.sort_values("investment_period").reset_index(drop=True),
        expected,
        check_dtype=False,
    )


def test_calendar_year_periods_start_in_january(csv_str_to_df, sample_model_config):
    """Under calendar years the 2026 period starts 2026-01-01, so a value
    dated 2025-12-01 is already active in the first period — under fy it
    would miss the 2025-07-01 period start and the 2026 rows would drop."""
    sample_model_config.temporal.year_type = "calendar"
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  2500,  2025-12-01T00:00:00
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    rhs = result["custom_constraints_rhs"]
    rhs = rhs[rhs["constraint_name"] == "SWQLD1"]
    expected = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,           2026,               qld_peak_demand,  2500,  <=
        SWQLD1,           2028,               qld_peak_demand,  2500,  <=
    """)
    pd.testing.assert_frame_equal(
        rhs.sort_values("investment_period").reset_index(drop=True),
        expected,
        check_dtype=False,
    )


def test_date_from_after_all_periods_contributes_nothing(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  KINGASF1,       0.14,
        SWQLD1,         generator_output,  LATEGEN,        0.5,          2040-01-01T00:00:00
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2028,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_lhs_coefficient_superseded_mid_horizon(csv_str_to_df, sample_model_config):
    """A dated LHS row supersedes the baseline coefficient in the periods
    whose start it falls before, mirroring the RHS date_from resolution."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  KINGASF1,       0.14,
        SWQLD1,         generator_output,  KINGASF1,       0.3,          2026-12-01T00:00:00
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2028,               KINGASF1,          Generator,  p,          0.3
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_equality_direction_becomes_double_equals(csv_str_to_df, sample_model_config):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         =
    """)
    # No relaxation option: the network_expansion_options schema forbids
    # relaxing an "=" constraint.
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        NSW-QLD,       forward,         1000,               NSW-QLD Option 1
        NSW-QLD,       reverse,         900,                NSW-QLD Option 1
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,             rhs,   constraint_type
        SWQLD1,                   2026,               qld_peak_demand,       3000,  ==
        SWQLD1,                   2026,               qld_winter_reference,  3500,  ==
        SWQLD1,                   2028,               qld_peak_demand,       3000,  ==
        SWQLD1,                   2028,               qld_winter_reference,  3500,  ==
        NSW-QLD_expansion_limit,  ,                   ,                      1000,  <=
    """)
    sort_cols = ["constraint_name", "investment_period", "timeslice"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_rhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_relaxation_on_greater_equal_constraint_adds_capacity_to_lhs(
    csv_str_to_df, sample_model_config
):
    """A ">=" constraint is loosened by lowering its floor, so the relaxation
    generator's p_nom is added to the LHS (+1.0) rather than subtracted."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         >=
    """)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  KINGASF1,       0.14,
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2028,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_link_term_not_in_model_raises(csv_str_to_df, sample_model_config):
    """TAS-SEV has no links in the model, so the constraint can't be applied
    as written — dropping the term would silently weaken it, so raise."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         link_flow,         TAS-SEV,        0.5,
        SWQLD1,         generator_output,  KINGASF1,       0.14,
    """)

    with pytest.raises(ValueError) as excinfo:
        _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Custom constraint LHS terms reference components not in the model, "
        "as (constraint_id, variable_name): [('SWQLD1', 'TAS-SEV')]"
    ) in str(excinfo.value)


def test_generator_and_storage_terms_not_in_model_raise(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  UNKNOWNGEN,     0.14,
        SWQLD1,         storage_output,    Big Battery,    0.43,
    """)

    with pytest.raises(ValueError) as excinfo:
        _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Custom constraint LHS terms reference components not in the model, "
        "as (constraint_id, variable_name): "
        "[('SWQLD1', 'Big Battery'), ('SWQLD1', 'UNKNOWNGEN')]"
    ) in str(excinfo.value)


def test_term_type_without_component_mapping_raises(csv_str_to_df, sample_model_config):
    """A term_type with no entry in the term-type-to-component mappings (the
    schema's allowed term_types and the translator's mappings drifting apart)
    halts the run rather than silently dropping the term."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,    coefficient,  date_from
        SWQLD1,         storage_capacity,  Q8 Battery - 2h,  0.43,
    """)

    with pytest.raises(ValueError) as excinfo:
        _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Custom constraint LHS term_types with no component mapping: "
        "['storage_capacity']"
    ) in str(excinfo.value)


def test_load_term_resolves_to_the_demand_nodes_load_component(
    csv_str_to_df, sample_model_config
):
    """A load term is a data term on the sub-region's demand: it maps to the
    load_<bus> Load component at its demand node, whose p_set pypsa_build
    resolves from the demand trace."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,  variable_name,  coefficient,  date_from
        SWQLD1,         load,       SQ,             -0.33,
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               load_SQ,           Load,       p_set,      -0.33
        SWQLD1,                   2028,               load_SQ,           Load,       p_set,      -0.33
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_load_term_for_sub_region_without_demand_node_raises(
    csv_str_to_df, sample_model_config
):
    """A sub-region that isn't a demand node (it's outside the model, or the
    granularity aggregates it away) has no demand for a load term to
    reference."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,  variable_name,  coefficient,  date_from
        SWQLD1,         load,       CNSW,           -0.33,
    """)

    with pytest.raises(ValueError) as excinfo:
        _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Custom constraint LHS terms reference components not in the model, "
        "as (constraint_id, variable_name): [('SWQLD1', 'CNSW')]"
    ) in str(excinfo.value)


def test_new_entrant_terms_expand_to_per_build_year_components(
    csv_str_to_df, sample_model_config
):
    """A term naming a new entrant generator's isp_name covers each of its
    per-build-year components from their build years onward — the 2028
    component's dispatch is fixed at zero in 2026, so its term only enters
    the constraint once built."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  N2 Solar,       0.5,
    """)
    generators = csv_str_to_df("""
        isp_name,  name,           build_year,  lifetime
        N2 Solar,  N2 Solar_2026,  2026,        30
        N2 Solar,  N2 Solar_2028,  2028,        30
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        generators,
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               N2 Solar_2026,     Generator,  p,          0.5
        SWQLD1,                   2028,               N2 Solar_2026,     Generator,  p,          0.5
        SWQLD1,                   2028,               N2 Solar_2028,     Generator,  p,          0.5
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_new_entrant_storage_term_expands_to_per_build_year_components(
    csv_str_to_df, sample_model_config
):
    """A storage_output term naming a new entrant unit's isp_name covers each
    of its per-build-year components from their build years onward, matching
    the generator behaviour."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,       variable_name,  coefficient,  date_from
        SWQLD1,         storage_output,  SQ BESS,        0.43,
    """)
    storage = csv_str_to_df("""
        isp_name,  name,          build_year,  lifetime
        SQ BESS,   SQ BESS_2026,  2026,        30
        SQ BESS,   SQ BESS_2028,  2028,        30
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        storage,
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               SQ BESS_2026,      Storage,    p,          0.43
        SWQLD1,                   2028,               SQ BESS_2026,      Storage,    p,          0.43
        SWQLD1,                   2028,               SQ BESS_2028,      Storage,    p,          0.43
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_generator_capacity_term_targets_p_nom_of_built_components(
    csv_str_to_df, sample_model_config
):
    """A generator_capacity term is a term on installed capacity (p_nom)
    rather than dispatch, and on a new entrant it covers each per-build-year
    component from its build year onward — a component's p_nom is a single
    horizon-wide variable, so counting it earlier would let capacity built
    for 2028 alter the 2026 constraint. Here alongside an output (p) term on
    the same unit."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,           variable_name,  coefficient,  date_from
        SWQLD1,         generator_capacity,  N2 Solar,       1.0,
        SWQLD1,         generator_output,    N2 Solar,       0.5,
    """)
    generators = csv_str_to_df("""
        isp_name,  name,           build_year,  lifetime
        N2 Solar,  N2 Solar_2026,  2026,        30
        N2 Solar,  N2 Solar_2028,  2028,        30
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        generators,
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               N2 Solar_2026,     Generator,  p_nom,      1.0
        SWQLD1,                   2028,               N2 Solar_2026,     Generator,  p_nom,      1.0
        SWQLD1,                   2028,               N2 Solar_2028,     Generator,  p_nom,      1.0
        SWQLD1,                   2026,               N2 Solar_2026,     Generator,  p,          0.5
        SWQLD1,                   2028,               N2 Solar_2026,     Generator,  p,          0.5
        SWQLD1,                   2028,               N2 Solar_2028,     Generator,  p,          0.5
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_generator_terms_dropped_from_retirement_year(
    csv_str_to_df, sample_model_config
):
    """A component's terms leave the constraint from its retirement year
    (build_year + lifetime) onward — the bound is exclusive, matching when
    PyPSA deactivates the component, so a unit built in 2025 with a 3-year
    lifetime contributes in 2026 but not in the 2028 period itself."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         link_flow,         NSW-QLD,        0.84,
        SWQLD1,         generator_output,  KINGASF1,       0.14,
    """)
    generators = csv_str_to_df("""
        isp_name,  name,      build_year,  lifetime
        KINGASF1,  KINGASF1,  2025,        3
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        generators,
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2026,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,                   2026,               NSW-QLD_exp_2026,  Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,                   2028,               NSW-QLD_exp_2026,  Link,       p,          0.84
        SWQLD1,                   2026,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2026,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    sort_cols = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_lhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def test_constraint_with_no_lhs_terms_dropped_and_logged(
    csv_str_to_df, sample_model_config, caplog
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         <=
        NQ1,            <=
    """)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        NQ1,            qld_peak_demand,  2650,
    """)

    with caplog.at_level("INFO"):
        result = _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Custom constraint RHS rows dropped (no LHS terms in that period): "
        "[('NQ1', 2026), ('NQ1', 2028)]"
    ) in caplog.text
    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,                   2026,               qld_peak_demand,  3000,  <=
        SWQLD1,                   2028,               qld_peak_demand,  3000,  <=
        NSW-QLD_expansion_limit,  ,                   ,                 1000,  <=
        SWQLD1_expansion_limit,   ,                   ,                 400,   <=
    """)
    sort_cols = ["constraint_name", "investment_period", "timeslice"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].sort_values(sort_cols).reset_index(drop=True),
        expected_rhs.sort_values(sort_cols).reset_index(drop=True),
        check_dtype=False,
    )


def _one_sided_period_expected_outputs(csv_str_to_df):
    """SWQLD1 binding in 2028 only, with a single generator term: the outputs
    both mid-horizon date_from cases below converge on. Both relaxation
    generators are still built, but only enter the 2028 constraint."""
    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,                   2028,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,                   2028,               SWQLD1_exp_2026,   Generator,  p_nom,      -1.0
        SWQLD1,                   2028,               SWQLD1_exp_2028,   Generator,  p_nom,      -1.0
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2026,   Generator,  p_nom,      1.0
        SWQLD1_expansion_limit,   ,                   SWQLD1_exp_2028,   Generator,  p_nom,      1.0
    """)
    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,        rhs,   constraint_type
        SWQLD1,                   2028,               qld_peak_demand,  3000,  <=
        NSW-QLD_expansion_limit,  ,                   ,                 1000,  <=
        SWQLD1_expansion_limit,   ,                   ,                 400,   <=
    """)
    return expected_lhs, expected_rhs


def _assert_lhs_and_rhs_equal(result, expected_lhs, expected_rhs):
    lhs_sort = ["constraint_name", "investment_period", "variable_name", "attribute"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"].sort_values(lhs_sort).reset_index(drop=True),
        expected_lhs.sort_values(lhs_sort).reset_index(drop=True),
        check_dtype=False,
    )
    rhs_sort = ["constraint_name", "investment_period", "timeslice"]
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"].sort_values(rhs_sort).reset_index(drop=True),
        expected_rhs.sort_values(rhs_sort).reset_index(drop=True),
        check_dtype=False,
    )


def test_rhs_starting_mid_horizon_drops_lhs_for_earlier_periods_and_logs(
    csv_str_to_df, sample_model_config, caplog
):
    """An RHS whose date_from falls after the 2026 period start (2025-07-01)
    but before 2028's (2027-07-01) binds only in 2028, so SWQLD1's 2026 LHS
    terms have nothing to pair with and are dropped — including the
    relaxation generator's, which only enter periods the constraint binds in."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  KINGASF1,       0.14,
    """)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,  2027-01-01T00:00:00
    """)

    with caplog.at_level("INFO"):
        result = _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Custom constraint LHS terms dropped (no RHS row in that period): "
        "[('SWQLD1', 2026)]"
    ) in caplog.text
    expected_lhs, expected_rhs = _one_sided_period_expected_outputs(csv_str_to_df)
    _assert_lhs_and_rhs_equal(result, expected_lhs, expected_rhs)


def test_lhs_starting_mid_horizon_drops_rhs_for_earlier_periods_and_logs(
    csv_str_to_df, sample_model_config, caplog
):
    """The mirror case: LHS terms that only start after the 2026 period start
    leave SWQLD1's 2026 RHS row with nothing to constrain, so it is dropped
    rather than emitted as an empty (or relaxation-only) constraint."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  KINGASF1,       0.14,         2027-01-01T00:00:00
    """)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
    """)

    with caplog.at_level("INFO"):
        result = _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Custom constraint RHS rows dropped (no LHS terms in that period): "
        "[('SWQLD1', 2026)]"
    ) in caplog.text
    expected_lhs, expected_rhs = _one_sided_period_expected_outputs(csv_str_to_df)
    _assert_lhs_and_rhs_equal(result, expected_lhs, expected_rhs)


def test_no_one_sided_drop_logs_when_both_sides_cover_both_periods(
    csv_str_to_df, sample_model_config, caplog
):
    """Both sides of the base fixture's constraint cover both periods, so
    neither one-sided INFO drop line fires."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)

    with caplog.at_level("INFO"):
        _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert "RHS rows dropped" not in caplog.text
    assert "LHS terms dropped" not in caplog.text


def test_empty_custom_constraint_tables(csv_str_to_df, sample_model_config):
    """At coarser granularities the custom-constraint tables are header-only;
    the expansion-limit constraints for links are still produced."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = pd.DataFrame(
        columns=["constraint_id", "direction"]
    )
    ispypsa_tables["custom_constraints_lhs"] = pd.DataFrame(
        columns=[
            "constraint_id",
            "term_type",
            "variable_name",
            "coefficient",
            "date_from",
        ]
    )
    ispypsa_tables["custom_constraints_rhs"] = pd.DataFrame(
        columns=["constraint_id", "timeslice", "rhs", "date_from"]
    )
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        NSW-QLD,       forward,         1000,               Option 1
        NSW-QLD,       reverse,         900,                Option 1
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        pd.DataFrame(columns=["isp_name", "name", "build_year", "lifetime"]),
        pd.DataFrame(columns=["isp_name", "name", "build_year", "lifetime"]),
        pd.DataFrame(columns=["name"]),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,          investment_period,  variable_name,     component,  attribute,  coefficient
        NSW-QLD_expansion_limit,  ,                   NSW-QLD_exp_2026,  Link,       p_nom,      1.0
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )

    expected_rhs = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,  rhs,   constraint_type
        NSW-QLD_expansion_limit,  ,                   ,           1000,  <=
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )


def test_no_constraints_and_no_expansion_yields_header_only_tables(
    csv_str_to_df, sample_model_config
):
    """With no custom constraints, no expansion options and no extendable
    links, no constraint block produces rows and every output is
    all-columns-no-rows."""
    ispypsa_tables = {
        "custom_constraints": pd.DataFrame(columns=["constraint_id", "direction"]),
        "custom_constraints_lhs": pd.DataFrame(
            columns=[
                "constraint_id",
                "term_type",
                "variable_name",
                "coefficient",
                "date_from",
            ]
        ),
        "custom_constraints_rhs": pd.DataFrame(
            columns=["constraint_id", "timeslice", "rhs", "date_from"]
        ),
        "network_expansion_options": pd.DataFrame(
            columns=[
                "expansion_id",
                "expansion_type",
                "allowed_expansion",
                "expansion_option",
            ]
        ),
        "network_transmission_path_expansion_costs": pd.DataFrame(
            columns=["expansion_id", "year", "cost"]
        ),
    }
    links = csv_str_to_df("""
        isp_name,  name,              p_nom_extendable,  build_year,  lifetime
        NSW-QLD,   NSW-QLD_existing,  False,             2025,        inf
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        links,
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,  component,  attribute,  coefficient
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_lhs"], expected_lhs, check_dtype=False
    )
    expected_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,  rhs,  constraint_type
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_rhs"], expected_rhs, check_dtype=False
    )
    expected_generators = csv_str_to_df("""
        name,  isp_name,  bus,  p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_generators"], expected_generators, check_dtype=False
    )


def test_constraints_translate_without_any_expansion_options(
    csv_str_to_df, sample_model_config
):
    """The mirror of the empty-constraint-tables case: populated constraint
    tables with an empty expansion options table (and so no extendable links)
    translate as usual, while every expansion block — relaxation generators
    and expansion limits — is empty."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["network_expansion_options"] = pd.DataFrame(
        columns=[
            "expansion_id",
            "expansion_type",
            "allowed_expansion",
            "expansion_option",
        ]
    )
    ispypsa_tables["network_transmission_path_expansion_costs"] = pd.DataFrame(
        columns=["expansion_id", "year", "cost"]
    )
    links = csv_str_to_df("""
        isp_name,  name,              p_nom_extendable,  build_year,  lifetime
        NSW-QLD,   NSW-QLD_existing,  False,             2025,        inf
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        links,
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_lhs = csv_str_to_df("""
        constraint_name,  investment_period,  variable_name,     component,  attribute,  coefficient
        SWQLD1,           2026,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,           2028,               NSW-QLD_existing,  Link,       p,          0.84
        SWQLD1,           2026,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,           2028,               KINGASF1,          Generator,  p,          0.14
        SWQLD1,           2026,               Q8 Battery - 2h,   Storage,    p,          0.43
        SWQLD1,           2028,               Q8 Battery - 2h,   Storage,    p,          0.43
    """)
    expected_rhs = csv_str_to_df("""
        constraint_name,  investment_period,  timeslice,             rhs,   constraint_type
        SWQLD1,           2026,               qld_peak_demand,       3000,  <=
        SWQLD1,           2026,               qld_winter_reference,  3500,  <=
        SWQLD1,           2028,               qld_peak_demand,       3000,  <=
        SWQLD1,           2028,               qld_winter_reference,  3500,  <=
    """)
    _assert_lhs_and_rhs_equal(result, expected_lhs, expected_rhs)
    expected_generators = csv_str_to_df("""
        name,  isp_name,  bus,  p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
    """)
    pd.testing.assert_frame_equal(
        result["custom_constraints_generators"], expected_generators, check_dtype=False
    )


def test_path_expansion_limit_is_max_of_forward_and_reverse(
    csv_str_to_df, sample_model_config
):
    """Expansion links carry forward/reverse per unit of max(forward, reverse),
    so the cap on their total p_nom must be that max, not the forward value."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        NSW-QLD,       forward,                800,                NSW-QLD Option 1
        NSW-QLD,       reverse,                1000,               NSW-QLD Option 1
        SWQLD1,        constraint_relaxation,  400,                SWQLD1 Option 2
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    rhs = result["custom_constraints_rhs"]
    expected = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,  rhs,   constraint_type
        NSW-QLD_expansion_limit,  ,                   ,           1000,  <=
    """)
    pd.testing.assert_frame_equal(
        rhs[rhs["constraint_name"] == "NSW-QLD_expansion_limit"].reset_index(drop=True),
        expected,
        check_dtype=False,
    )


def test_wildcard_relaxation_option_and_cost_apply_to_every_constraint(
    csv_str_to_df, sample_model_config
):
    """A blank expansion_id relaxation option (and a blank expansion_id, blank
    year cost) is a default for every constraint in the model; a specific
    row still wins for its own constraint."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        SWQLD1,         <=
        NQ1,            <=
    """)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        SWQLD1,         generator_output,  KINGASF1,       0.14,
        NQ1,            generator_output,  KINGASF1,       0.5,
    """)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        SWQLD1,         qld_peak_demand,  3000,
        NQ1,            qld_peak_demand,  2650,
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        NSW-QLD,       forward,                1000,               NSW-QLD Option 1
        NSW-QLD,       reverse,                900,                NSW-QLD Option 1
        SWQLD1,        constraint_relaxation,  400,                SWQLD1 Option 2
        ,              constraint_relaxation,  200,                Default relaxation
    """)
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        NSW-QLD,       2026,  500000
        ,              ,      100000
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_generators = csv_str_to_df(f"""
        name,             isp_name,  bus,                             p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
        NQ1_exp_2026,     NQ1,       bus_for_custom_constraint_gens,  0.0,    True,              2026,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
        NQ1_exp_2028,     NQ1,       bus_for_custom_constraint_gens,  0.0,    True,              2028,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
        SWQLD1_exp_2026,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2026,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
        SWQLD1_exp_2028,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2028,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
    """)
    generators = result["custom_constraints_generators"]
    pd.testing.assert_frame_equal(
        generators.sort_values("name").reset_index(drop=True),
        expected_generators,
        check_dtype=False,
        rtol=1e-5,
    )
    rhs = result["custom_constraints_rhs"]
    expected_limits = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,  rhs,   constraint_type
        NQ1_expansion_limit,      ,                   ,           200,   <=
        NSW-QLD_expansion_limit,  ,                   ,           1000,  <=
        SWQLD1_expansion_limit,   ,                   ,           400,   <=
    """)
    limits = rhs[rhs["constraint_name"].str.endswith("_expansion_limit")]
    pd.testing.assert_frame_equal(
        limits.sort_values("constraint_name").reset_index(drop=True),
        expected_limits,
        check_dtype=False,
    )


def test_blank_expansion_type_option_covers_constraint_relaxation(
    csv_str_to_df, sample_model_config
):
    """An option row with a blank expansion_type is a wildcard covering
    forward, reverse and constraint_relaxation alike, so a row keyed only by
    the constraint's expansion_id still yields its relaxation."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,  allowed_expansion,  expansion_option
        NSW-QLD,       forward,         1000,               NSW-QLD Option 1
        NSW-QLD,       reverse,         900,                NSW-QLD Option 1
        SWQLD1,        ,                400,                SWQLD1 Option 2
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_generators = csv_str_to_df(f"""
        name,             isp_name,  bus,                             p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
        SWQLD1_exp_2026,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2026,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
        SWQLD1_exp_2028,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2028,        inf,       {80000 * _ANNUITY_PER_DOLLAR}
    """)
    generators = result["custom_constraints_generators"]
    pd.testing.assert_frame_equal(
        generators.sort_values("name").reset_index(drop=True),
        expected_generators,
        check_dtype=False,
        rtol=1e-5,
    )
    rhs = result["custom_constraints_rhs"]
    expected_limits = csv_str_to_df("""
        constraint_name,          investment_period,  timeslice,  rhs,   constraint_type
        NSW-QLD_expansion_limit,  ,                   ,           1000,  <=
        SWQLD1_expansion_limit,   ,                   ,           400,   <=
    """)
    limits = rhs[rhs["constraint_name"].str.endswith("_expansion_limit")]
    pd.testing.assert_frame_equal(
        limits.sort_values("constraint_name").reset_index(drop=True),
        expected_limits,
        check_dtype=False,
    )


def test_relaxation_option_for_constraint_not_in_model_is_dropped(
    csv_str_to_df, sample_model_config
):
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        NSW-QLD,       forward,                1000,               NSW-QLD Option 1
        NSW-QLD,       reverse,                900,                NSW-QLD Option 1
        SWQLD1,        constraint_relaxation,  400,                SWQLD1 Option 2
        NQ1,           constraint_relaxation,  300,                NQ1 Option 1
    """)
    # Blank years: a static cost across the investment periods.
    ispypsa_tables["network_transmission_path_expansion_costs"] = csv_str_to_df("""
        expansion_id,  year,  cost
        NSW-QLD,       ,      500000
        SWQLD1,        ,      100000
        NQ1,           ,      100000
    """)

    result = _translate_custom_constraints(
        ispypsa_tables,
        _links(csv_str_to_df),
        _generators(csv_str_to_df),
        _storage(csv_str_to_df),
        _demand_nodes(csv_str_to_df),
        sample_model_config,
    )

    expected_generators = csv_str_to_df(f"""
        name,             isp_name,  bus,                             p_nom,  p_nom_extendable,  build_year,  lifetime,  capital_cost
        SWQLD1_exp_2026,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2026,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
        SWQLD1_exp_2028,  SWQLD1,    bus_for_custom_constraint_gens,  0.0,    True,              2028,        inf,       {100000 * _ANNUITY_PER_DOLLAR}
    """)
    generators = result["custom_constraints_generators"]
    pd.testing.assert_frame_equal(
        generators.sort_values("name").reset_index(drop=True),
        expected_generators,
        check_dtype=False,
        rtol=1e-5,
    )


def test_constraint_named_after_a_path_raises_on_colliding_expansion_limits(
    csv_str_to_df, sample_model_config
):
    """A relaxable constraint sharing its ID with an expandable path gives
    both the same "<id>_expansion_limit" RHS row — pypsa_build would build
    two constraints with the same name, so the final duplicate check halts."""
    ispypsa_tables = _constraint_tables(csv_str_to_df)
    ispypsa_tables["custom_constraints"] = csv_str_to_df("""
        constraint_id,  direction
        NSW-QLD,        <=
    """)
    ispypsa_tables["custom_constraints_lhs"] = csv_str_to_df("""
        constraint_id,  term_type,         variable_name,  coefficient,  date_from
        NSW-QLD,        generator_output,  KINGASF1,       0.14,
    """)
    ispypsa_tables["custom_constraints_rhs"] = csv_str_to_df("""
        constraint_id,  timeslice,        rhs,   date_from
        NSW-QLD,        qld_peak_demand,  3000,
    """)
    ispypsa_tables["network_expansion_options"] = csv_str_to_df("""
        expansion_id,  expansion_type,         allowed_expansion,  expansion_option
        NSW-QLD,       forward,                1000,               NSW-QLD Option 1
        NSW-QLD,       reverse,                900,                NSW-QLD Option 1
        NSW-QLD,       constraint_relaxation,  400,                NSW-QLD Option 2
    """)

    with pytest.raises(ValueError) as excinfo:
        _translate_custom_constraints(
            ispypsa_tables,
            _links(csv_str_to_df),
            _generators(csv_str_to_df),
            _storage(csv_str_to_df),
            _demand_nodes(csv_str_to_df),
            sample_model_config,
        )

    assert (
        "Duplicate custom constraint RHS rows for: ['NSW-QLD_expansion_limit']"
    ) in str(excinfo.value)
