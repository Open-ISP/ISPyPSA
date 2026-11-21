import numpy as np
import pandas as pd
import pytest

from ispypsa.translator.carbon_budgets import (
    _build_emissions_trajectory,
    _drop_budgets_with_no_modelled_years,
    _emissions_rate_at_each_knot,
    _pair_budgets_with_modelled_years,
    _raise_if_budgets_are_not_nested,
    _raise_if_emissions_would_rise,
    _raise_on_partly_modelled_windows,
    _scale_budgets_to_modelled_years,
    _share_budgets_along_trajectory,
    _sum_shares_into_rhs,
    _translate_carbon_budgets,
)

_CARBON_BUDGET_COLUMNS = ["budget_id", "year_from", "year_to", "budget"]


def _step_change_budgets(csv_str_to_df) -> pd.DataFrame:
    """The 7.5 workbook's Step Change budgets. Under linear_trajectory_to_zero
    they give a trajectory falling from 115.7 Mt a year at the start of 2027 to
    29.3 at the end of 2030, then to zero at the end of 2050."""
    return csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2035_to_2035,   2035,       2035,       23.0
    """)


# --- _translate_carbon_budgets ---


def test_translate_carbon_budgets(csv_str_to_df, sample_model_config):
    sample_model_config.carbon_budget_scaling = "prorata"
    carbon_budgets = _step_change_budgets(csv_str_to_df)

    result = _translate_carbon_budgets(carbon_budgets, sample_model_config)

    # The sample config models 2026 to 2028, which covers 2027 and 2028 of the
    # two multi-year budgets' windows: 290 x 2 / 4 and 583 x 2 / 24. The 2035
    # budget has no modelled year, so it is dropped.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       145.0
        nem_2027_to_2050,   2027,       2050,       48.583333
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_translate_carbon_budgets_no_budgets(csv_str_to_df, sample_model_config):
    carbon_budgets = pd.DataFrame(columns=_CARBON_BUDGET_COLUMNS)

    result = _translate_carbon_budgets(carbon_budgets, sample_model_config)

    expected = csv_str_to_df("""
        constraint_name,  year_from,  year_to,  rhs
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_translate_carbon_budgets_warns_when_model_is_region_filtered(
    csv_str_to_df, sample_model_config, caplog
):
    sample_model_config.carbon_budget_scaling = "prorata"
    sample_model_config.filter_by_nem_regions = ["VIC", "NSW"]

    with caplog.at_level("WARNING"):
        _translate_carbon_budgets(
            _step_change_budgets(csv_str_to_df), sample_model_config
        )

    assert (
        "The carbon budgets are for the whole NEM but the model is filtered to "
        "['NSW', 'VIC']: the budgets are applied to it unscaled."
    ) in caplog.text


def test_translate_carbon_budgets_does_not_warn_when_model_is_not_filtered(
    csv_str_to_df, sample_model_config, caplog
):
    sample_model_config.carbon_budget_scaling = "prorata"

    with caplog.at_level("WARNING"):
        _translate_carbon_budgets(
            _step_change_budgets(csv_str_to_df), sample_model_config
        )

    assert "The carbon budgets are for the whole NEM" not in caplog.text


# --- _scale_budgets_to_modelled_years ---


@pytest.mark.parametrize("scaling", ["none", "prorata", "linear_trajectory_to_zero"])
def test_scale_budgets_keeps_fully_modelled_budgets_whole(csv_str_to_df, scaling):
    carbon_budgets = _step_change_budgets(csv_str_to_df)

    result = _scale_budgets_to_modelled_years(carbon_budgets, 2025, 2052, scaling)

    # Every window sits inside 2025 to 2052, so each rhs is the budget
    # itself whatever the scaling.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_scale_budgets_prorata(csv_str_to_df):
    carbon_budgets = _step_change_budgets(csv_str_to_df)

    result = _scale_budgets_to_modelled_years(carbon_budgets, 2029, 2040, "prorata")

    # 2029 and 2030 are 2 of the first budget's 4 years: 290 x 2 / 4. 2029 to
    # 2040 are 12 of the second's 24 years: 583 x 12 / 24. The 2035 budget is
    # on one modelled year, so it is whole.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       145.0
        nem_2027_to_2050,   2027,       2050,       291.5
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_scale_budgets_linear_trajectory_to_zero(csv_str_to_df):
    carbon_budgets = _step_change_budgets(csv_str_to_df)

    result = _scale_budgets_to_modelled_years(
        carbon_budgets, 2029, 2040, "linear_trajectory_to_zero"
    )

    # The trajectory gives 2029 and 2030 61.7 + 40.1 = 101.8, which is the
    # first budget's rhs. The second stretch (293 over 2031 to 2050) falls in
    # a straight line to zero, so its first half (2031 to 2040) has three
    # quarters of it, 219.75, and the second budget's rhs is 101.8 + 219.75.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       101.8
        nem_2027_to_2050,   2027,       2050,       321.55
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_scale_budgets_linear_trajectory_uses_budgets_with_no_modelled_years(
    csv_str_to_df,
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)

    result = _scale_budgets_to_modelled_years(
        carbon_budgets, 2050, 2050, "linear_trajectory_to_zero"
    )

    # nem_2027_to_2030 has no modelled years, so it is dropped, but it still
    # leaves only 293 of the longer budget for 2031 to 2050. The last of
    # those 20 years has 1/400 of the area under a line to zero: 0.7325.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2050,   2027,       2050,       0.7325
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_scale_budgets_none_raises_on_partly_modelled_window(csv_str_to_df):
    carbon_budgets = _step_change_budgets(csv_str_to_df)

    with pytest.raises(ValueError, match=r"\['nem_2027_to_2030', 'nem_2027_to_2050'\]"):
        _scale_budgets_to_modelled_years(carbon_budgets, 2029, 2040, "none")


def test_scale_budgets_none_ignores_budgets_with_no_modelled_years(csv_str_to_df):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2035_to_2035,   2035,       2035,       23.0
    """)

    result = _scale_budgets_to_modelled_years(carbon_budgets, 2035, 2035, "none")

    # nem_2027_to_2030 has no modelled year at all, so it is dropped and
    # doesn't raise. Only a window that is partly modelled raises.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("scaling", ["none", "prorata", "linear_trajectory_to_zero"])
def test_scale_budgets_empty_table(csv_str_to_df, scaling):
    carbon_budgets = pd.DataFrame(columns=_CARBON_BUDGET_COLUMNS)

    result = _scale_budgets_to_modelled_years(carbon_budgets, 2029, 2040, scaling)

    expected = csv_str_to_df("""
        constraint_name,  year_from,  year_to,  rhs
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _sum_shares_into_rhs ---


def test_sum_shares_into_rhs(csv_str_to_df):
    budget_years = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget,     year,   budget_share
        nem_2027_to_2030,   2027,       2030,       290.0,      2029,   72.5
        nem_2027_to_2030,   2027,       2030,       290.0,      2030,   72.5
        nem_2035_to_2035,   2035,       2035,       23.0,       2035,   23.0000004
    """)

    result = _sum_shares_into_rhs(budget_years)

    # The rhs is rounded to the nearest tonne (six decimal places of a Mt).
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       145.0
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


# --- _drop_budgets_with_no_modelled_years ---


def test_drop_budgets_with_no_modelled_years(csv_str_to_df, caplog):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2031_to_2031,   2031,       2031,       30.0
        nem_2055_to_2055,   2055,       2055,       1.0
    """)

    with caplog.at_level("INFO"):
        result = _drop_budgets_with_no_modelled_years(carbon_budgets, 2031, 2050)

    # 2027 to 2030 ends before the modelled years and 2055 is after them. A
    # window that only touches the first modelled year (2031) is kept.
    expected = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2031_to_2031,   2031,       2031,       30.0
    """)
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert (
        "Carbon budgets with no modelled year (2031 to 2050) in their window "
        "(not applied): ['nem_2027_to_2030', 'nem_2055_to_2055']"
    ) in caplog.text


def test_drop_budgets_with_no_modelled_years_does_not_log_when_none_dropped(
    csv_str_to_df, caplog
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)

    with caplog.at_level("INFO"):
        result = _drop_budgets_with_no_modelled_years(carbon_budgets, 2027, 2050)

    pd.testing.assert_frame_equal(result, carbon_budgets)
    assert "Carbon budgets with no modelled year" not in caplog.text


# --- _raise_on_partly_modelled_windows ---


def test_raise_on_partly_modelled_windows(csv_str_to_df):
    budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to
        nem_2027_to_2030,   2027,       2030
        nem_2035_to_2035,   2035,       2035
        nem_2040_to_2060,   2040,       2060
    """)

    # 2027 and 2028 come before the modelled years, and 2051 to 2060 after.
    expected_message = (
        r"Carbon budgets with only part of their window in the modelled years "
        r"\(2029 to 2050\): \['nem_2027_to_2030', 'nem_2040_to_2060'\]. Set "
        r"carbon_budget_scaling to 'prorata' or 'linear_trajectory_to_zero' in "
        r"the config to scale them to the modelled years, or remove them from "
        r"the carbon_budgets table."
    )
    with pytest.raises(ValueError, match=expected_message):
        _raise_on_partly_modelled_windows(budgets, 2029, 2050)


def test_raise_on_partly_modelled_windows_passes_when_windows_are_modelled(
    csv_str_to_df,
):
    budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to
        nem_2029_to_2050,   2029,       2050
        nem_2035_to_2035,   2035,       2035
    """)

    _raise_on_partly_modelled_windows(budgets, 2029, 2050)


# --- _pair_budgets_with_modelled_years ---


def test_pair_budgets_with_modelled_years(csv_str_to_df):
    budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2035_to_2035,   2035,       2035,       23.0
        nem_2049_to_2052,   2049,       2052,       10.0
    """)

    result = _pair_budgets_with_modelled_years(budgets, 2029, 2050)

    # The first budget loses 2027 and 2028, which are before the modelled
    # years, and the last loses 2051 and 2052, which are after them.
    expected = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget,     year
        nem_2027_to_2030,   2027,       2030,       290.0,      2029
        nem_2027_to_2030,   2027,       2030,       290.0,      2030
        nem_2035_to_2035,   2035,       2035,       23.0,       2035
        nem_2049_to_2052,   2049,       2052,       10.0,       2049
        nem_2049_to_2052,   2049,       2052,       10.0,       2050
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_pair_budgets_with_modelled_years_no_budgets(csv_str_to_df):
    budgets = pd.DataFrame(columns=["budget_id", "year_from", "year_to", "budget"])

    result = _pair_budgets_with_modelled_years(budgets, 2029, 2050)

    expected = csv_str_to_df("""
        budget_id,  year_from,  year_to,    budget,     year
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _share_budgets_along_trajectory ---


def test_share_budgets_along_trajectory(csv_str_to_df):
    budget_years = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget,     year
        nem_2027_to_2030,   2027,       2030,       290.0,      2030
        nem_2027_to_2050,   2027,       2050,       583.0,      2030
        nem_2027_to_2050,   2027,       2050,       583.0,      2035
        nem_2035_to_2035,   2035,       2035,       23.0,       2035
    """)

    result = _share_budgets_along_trajectory(
        budget_years, _step_change_budgets(csv_str_to_df)
    )

    # Both multi-year budgets take the trajectory's 2030 emissions (40.1). In
    # 2035 the long budget takes the trajectory's (29.3 less 4.5 years' fall
    # of 1.465), while the one-year 2035 budget takes its own 23.
    expected = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget,     year,   budget_share
        nem_2027_to_2030,   2027,       2030,       290.0,      2030,   40.1
        nem_2027_to_2050,   2027,       2050,       583.0,      2030,   40.1
        nem_2027_to_2050,   2027,       2050,       583.0,      2035,   22.7075
        nem_2035_to_2035,   2035,       2035,       23.0,       2035,   23.0
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False)


def test_share_budgets_along_trajectory_no_budgets(csv_str_to_df):
    columns = ["budget_id", "year_from", "year_to", "budget"]
    budget_years = pd.DataFrame(columns=[*columns, "year"])
    carbon_budgets = pd.DataFrame(columns=columns)

    result = _share_budgets_along_trajectory(budget_years, carbon_budgets)

    expected = csv_str_to_df("""
        budget_id,  year_from,  year_to,    budget,     year,   budget_share
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- _build_emissions_trajectory ---


def test_build_emissions_trajectory(csv_str_to_df):
    modelled_years = csv_str_to_df("""
        year
        2026
        2027
        2030
        2031
        2050
        2051
    """)

    result = _build_emissions_trajectory(
        _step_change_budgets(csv_str_to_df), modelled_years
    )

    # 2026 and 2051 are outside the multi-year budgets, so not on the
    # trajectory.
    # 2027 to 2030 falls from 115.7 to 29.3 (21.6 a year) and 2031 to 2050
    # from 29.3 to zero (1.465 a year); each year takes the level half way
    # through it.
    expected = csv_str_to_df("""
        year,   emissions
        2027,   104.9
        2030,   40.1
        2031,   28.5675
        2050,   0.7325
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False)


def test_build_emissions_trajectory_adds_up_to_each_budget(csv_str_to_df):
    modelled_years = pd.DataFrame({"year": range(2027, 2051)})

    result = _build_emissions_trajectory(
        _step_change_budgets(csv_str_to_df), modelled_years
    )

    result["window"] = result["year"].map(
        lambda year: "2027_to_2030" if year <= 2030 else "2031_to_2050"
    )
    totals = result.groupby("window", as_index=False)["emissions"].sum()
    # The trajectory's emissions over 2027 to 2030 are the first budget (290),
    # and over 2031 to 2050 are what the second leaves (583 - 290).
    expected = csv_str_to_df("""
        window,         emissions
        2027_to_2030,   290.0
        2031_to_2050,   293.0
    """)
    pd.testing.assert_frame_equal(totals, expected, check_exact=False)


def test_build_emissions_trajectory_no_multi_year_budgets(csv_str_to_df):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    modelled_years = pd.DataFrame({"year": range(2030, 2041)})

    result = _build_emissions_trajectory(carbon_budgets, modelled_years)

    expected = csv_str_to_df("""
        year,   emissions
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_build_emissions_trajectory_three_nested_budgets(csv_str_to_df):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2040,   2027,       2040,       500.0
    """)
    modelled_years = csv_str_to_df("""
        year
        2027
        2031
        2041
        2050
    """)

    result = _build_emissions_trajectory(carbon_budgets, modelled_years)

    # Whatever order the rows are in, the trajectory runs through the budgets
    # in order of length. 2041 to 2050 is allowed 83 Mt and ends at zero, so it
    # starts at 16.6 Mt a year. 2031 to 2040 is allowed 210 Mt (21 a year) and
    # ends at 16.6, so it starts at 25.4. 2027 to 2030 is allowed 290 Mt (72.5
    # a year) and ends at 25.4, so it starts at 119.6. Each year below is the
    # rate half way through it.
    expected = csv_str_to_df("""
        year,   emissions
        2027,   107.825
        2031,   24.96
        2041,   15.77
        2050,   0.83
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=False)


def test_build_emissions_trajectory_raises_when_budgets_are_not_nested(csv_str_to_df):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2031_to_2050,   2031,       2050,       293.0
    """)
    modelled_years = pd.DataFrame({"year": range(2027, 2051)})

    with pytest.raises(ValueError, match=r"\['nem_2031_to_2050'\]"):
        _build_emissions_trajectory(carbon_budgets, modelled_years)


def test_build_emissions_trajectory_raises_when_longer_budget_is_smaller(
    csv_str_to_df,
):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2050,   2027,       2050,       200.0
    """)
    modelled_years = pd.DataFrame({"year": range(2027, 2051)})

    # The longer budget leaves -90 Mt for 2031 to 2050, which would need a
    # rate below zero at the end of 2030 rising to zero by the end of 2050.
    with pytest.raises(ValueError, match=r"rise over the years \[\(2031, 2050\)\]"):
        _build_emissions_trajectory(carbon_budgets, modelled_years)


# --- _raise_if_budgets_are_not_nested ---


def test_raise_if_budgets_are_not_nested_passes_nested_budgets(csv_str_to_df):
    budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to
        nem_2027_to_2030,   2027,       2030
        nem_2027_to_2040,   2027,       2040
        nem_2027_to_2050,   2027,       2050
    """)

    _raise_if_budgets_are_not_nested(budgets)


def test_raise_if_budgets_are_not_nested_raises_on_a_later_start(csv_str_to_df):
    budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to
        nem_2027_to_2030,   2027,       2030
        nem_2031_to_2050,   2031,       2050
        nem_2029_to_2060,   2029,       2060
    """)

    expected_message = (
        r"Carbon budgets that start later than the earliest budget, or end in the "
        r"same year as another: \['nem_2029_to_2060', 'nem_2031_to_2050'\]. With "
        r"carbon_budget_scaling 'linear_trajectory_to_zero', the multi-year "
        r"budgets must all start in the same year and end in different years."
    )
    with pytest.raises(ValueError, match=expected_message):
        _raise_if_budgets_are_not_nested(budgets)


def test_raise_if_budgets_are_not_nested_raises_on_a_shared_end_year(csv_str_to_df):
    budgets = csv_str_to_df("""
        budget_id,  year_from,  year_to
        federal,    2027,       2050
        state,      2027,       2050
        nem_2030,   2027,       2030
    """)

    with pytest.raises(ValueError, match=r"as another: \['federal', 'state'\]"):
        _raise_if_budgets_are_not_nested(budgets)


# --- _emissions_rate_at_each_knot ---


def test_emissions_rate_at_each_knot():
    knot_years = np.array([2026.0, 2030.0, 2050.0])
    cumulative_budget = np.array([0.0, 290.0, 583.0])

    result = _emissions_rate_at_each_knot(knot_years, cumulative_budget)

    # 2030 to 2050 is allowed 293 Mt over 20 years, an average of 14.65, and
    # ends at zero, so it starts at 29.3. 2026 to 2030 is allowed 290 Mt over
    # 4 years, an average of 72.5, and ends at 29.3, so it starts at 115.7.
    expected = np.array([115.7, 29.3, 0.0])
    np.testing.assert_allclose(result, expected)


def test_emissions_rate_at_each_knot_single_budget():
    knot_years = np.array([2026.0, 2050.0])
    cumulative_budget = np.array([0.0, 480.0])

    result = _emissions_rate_at_each_knot(knot_years, cumulative_budget)

    # One budget is a single straight line to zero: 480 Mt over 24 years
    # averages 20, so it starts at 40.
    expected = np.array([40.0, 0.0])
    np.testing.assert_allclose(result, expected)


def test_emissions_rate_at_each_knot_no_budgets():
    knot_years = np.array([np.nan])
    cumulative_budget = np.array([0.0])

    result = _emissions_rate_at_each_knot(knot_years, cumulative_budget)

    # With no multi-year budgets there is only the starting knot.
    expected = np.array([0.0])
    np.testing.assert_allclose(result, expected)


# --- _raise_if_emissions_would_rise ---


def test_raise_if_emissions_would_rise():
    # 2031 to 2040 is allowed 6 Mt a year but 2041 to 2050 is allowed 10, so
    # the rate at the end of 2040 (20) is above the rate at the end of 2030.
    knot_years = np.array([2026.0, 2030.0, 2040.0, 2050.0])
    rates = np.array([153.0, -8.0, 20.0, 0.0])

    expected_message = (
        r"With carbon_budget_scaling 'linear_trajectory_to_zero', emissions would "
        r"have to rise over the years \[\(2031, 2040\)\] to use the carbon budgets "
        r"in full. Either the budgets allow more emissions a year after those "
        r"years than during them, or a longer budget is smaller than a shorter "
        r"one."
    )
    with pytest.raises(ValueError, match=expected_message):
        _raise_if_emissions_would_rise(knot_years, rates)


def test_raise_if_emissions_would_rise_passes_falling_and_flat_rates():
    # Flat from 2030 to 2040 is allowed: only a rise is an error.
    knot_years = np.array([2026.0, 2030.0, 2040.0, 2050.0])
    rates = np.array([115.7, 29.3, 29.3, 0.0])

    _raise_if_emissions_would_rise(knot_years, rates)
