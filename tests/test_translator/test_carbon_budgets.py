import re

import pandas as pd
import pytest

from ispypsa.translator.carbon_budgets import _translate_carbon_budgets

_CARBON_BUDGET_COLUMNS = ["budget_id", "year_from", "year_to", "budget"]
_SCALING_OPTIONS = ["none", "prorata", "linear_trajectory_to_zero"]


def _config(sample_model_config, start_year: int, end_year: int, scaling: str):
    """The sample config with the modelled years and carbon_budget_scaling
    set."""
    sample_model_config.temporal.range.start_year = start_year
    sample_model_config.temporal.range.end_year = end_year
    sample_model_config.carbon_budget_scaling = scaling
    return sample_model_config


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


# --- Whatever the scaling ---


@pytest.mark.parametrize("scaling", _SCALING_OPTIONS)
def test_fully_modelled_budgets_are_kept_whole(
    csv_str_to_df, sample_model_config, scaling
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2025, 2052, scaling)

    result = _translate_carbon_budgets(carbon_budgets, config)

    # Every window sits inside 2025 to 2052, so each rhs is the budget
    # itself whatever the scaling.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("scaling", _SCALING_OPTIONS)
def test_budgets_with_no_modelled_years_are_dropped_and_logged(
    csv_str_to_df, sample_model_config, caplog, scaling
):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2035_to_2035,   2035,       2035,       23.0
        nem_2055_to_2055,   2055,       2055,       5.0
    """)
    config = _config(sample_model_config, 2035, 2050, scaling)

    with caplog.at_level("INFO"):
        result = _translate_carbon_budgets(carbon_budgets, config)

    # nem_2027_to_2030 ends before 2035 and nem_2055_to_2055 starts after
    # 2050. Neither has a modelled year, so both are dropped, and under "none"
    # neither raises: only a window that is partly modelled does.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)
    assert (
        "Carbon budgets with no modelled year (2035 to 2050) in their window "
        "(not applied): ['nem_2027_to_2030', 'nem_2055_to_2055']"
    ) in caplog.text


def test_nothing_is_logged_when_every_budget_has_a_modelled_year(
    csv_str_to_df, sample_model_config, caplog
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2025, 2052, "none")

    with caplog.at_level("INFO"):
        _translate_carbon_budgets(carbon_budgets, config)

    assert "Carbon budgets with no modelled year" not in caplog.text


@pytest.mark.parametrize("scaling", _SCALING_OPTIONS)
def test_no_budgets(csv_str_to_df, sample_model_config, scaling):
    carbon_budgets = pd.DataFrame(columns=_CARBON_BUDGET_COLUMNS)
    config = _config(sample_model_config, 2029, 2040, scaling)

    result = _translate_carbon_budgets(carbon_budgets, config)

    expected = csv_str_to_df("""
        constraint_name,  year_from,  year_to,  rhs
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# --- carbon_budget_scaling "none" ---


def test_none_raises_on_partly_modelled_windows(csv_str_to_df, sample_model_config):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2029, 2040, "none")

    # 2027 and 2028 of both multi-year budgets aren't modelled, nor is 2041 to
    # 2050 of the longer one. nem_2035_to_2035 is modelled in full.
    expected_message = (
        "Carbon budgets with only part of their window in the modelled years "
        "(2029 to 2040): ['nem_2027_to_2030', 'nem_2027_to_2050']. Set "
        "carbon_budget_scaling to 'prorata' or 'linear_trajectory_to_zero' in "
        "the config to scale them to the modelled years, or remove them from "
        "the carbon_budgets table."
    )
    with pytest.raises(ValueError, match=re.escape(expected_message)):
        _translate_carbon_budgets(carbon_budgets, config)


# --- carbon_budget_scaling "prorata" ---


def test_prorata(csv_str_to_df, sample_model_config):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2029, 2040, "prorata")

    result = _translate_carbon_budgets(carbon_budgets, config)

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


def test_prorata_rounds_rhs_to_the_nearest_tonne(csv_str_to_df, sample_model_config):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2050,   2027,       2050,       583.0
    """)
    config = _config(sample_model_config, 2027, 2027, "prorata")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # One year of 24 is 583 / 24 = 24.291666... Mt. The rhs is kept to six
    # decimal places of a Mt, which is the nearest tonne.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2050,   2027,       2050,       24.291667
    """)
    pd.testing.assert_frame_equal(result, expected, check_exact=True)


def test_prorata_does_not_need_nested_budgets(csv_str_to_df, sample_model_config):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2031_to_2050,   2031,       2050,       293.0
    """)
    config = _config(sample_model_config, 2029, 2040, "prorata")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # These budgets start in different years, which linear_trajectory_to_zero
    # refuses. Under prorata each is scaled on its own: 290 x 2 / 4 and
    # 293 x 10 / 20.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       145.0
        nem_2031_to_2050,   2031,       2050,       146.5
    """)
    pd.testing.assert_frame_equal(result, expected)


# --- carbon_budget_scaling "linear_trajectory_to_zero" ---


def test_linear_trajectory_to_zero(csv_str_to_df, sample_model_config):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2029, 2040, "linear_trajectory_to_zero")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # The modelled years are 2029 to 2040. Each budget's rhs is the sum of the
    # trajectory's emissions over the modelled years in its window.
    #
    # 1. The budgets are 290 Mt for 2027 to 2030 and 583 Mt for 2027 to 2050,
    #    which leaves 583 - 290 = 293 Mt for 2031 to 2050.
    # 2. 2031 to 2050 is 293 Mt over 20 years, an average of 14.65 Mt a year.
    #    The trajectory is a straight line over those years, so its average
    #    is (start + end) / 2, which gives start = 2 x average - end. It ends
    #    at zero, so it starts at 2 x 14.65 - 0 = 29.3 Mt a year.
    # 3. 2027 to 2030 is 290 Mt over 4 years, an average of 72.5 Mt a year.
    #    It ends where 2031 starts, at 29.3, so by the same rule it starts at
    #    2 x 72.5 - 29.3 = 115.7.
    # 4. From 115.7 to 29.3 over 4 years is a fall of 21.6 a year. A year's
    #    emissions are the rate half way through it:
    #        2027: 104.9    2028: 83.3    2029: 61.7    2030: 40.1
    # 5. From 29.3 to zero over 20 years is a fall of 1.465 a year:
    #        2031: 28.5675    ...    2040: 15.3825
    #    Those ten years average (28.5675 + 15.3825) / 2 = 21.975, so they
    #    add up to 219.75.
    # 6. nem_2027_to_2030 has 2029 and 2030 modelled: 61.7 + 40.1 = 101.8.
    # 7. nem_2027_to_2050 has 2029 to 2040 modelled: 101.8 + 219.75 = 321.55.
    # 8. nem_2035_to_2035 is on a single year, which is modelled, so it keeps
    #    its 23.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2030,   2027,       2030,       101.8
        nem_2027_to_2050,   2027,       2050,       321.55
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_linear_trajectory_uses_budgets_with_no_modelled_years(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2050, 2050, "linear_trajectory_to_zero")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # Only 2050 is modelled.
    #
    # 1. nem_2027_to_2030 has no modelled years, so it is dropped. It still
    #    shapes the trajectory: it uses 290 Mt by the end of 2030, leaving
    #    583 - 290 = 293 Mt of the longer budget for 2031 to 2050.
    # 2. 2031 to 2050 is 293 Mt over 20 years, an average of 14.65 Mt a year.
    #    The trajectory is a straight line over those years, so its average
    #    is (start + end) / 2, which gives start = 2 x average - end. It ends
    #    at zero, so it starts at 2 x 14.65 - 0 = 29.3 Mt a year.
    # 3. From 29.3 to zero over 20 years is a fall of 29.3 / 20 = 1.465 a
    #    year, so the rate is 1.465 at the start of 2050 and zero at its end.
    # 4. 2050's emissions are its average rate times one year:
    #    (1.465 + 0) / 2 x 1 = 0.7325 Mt. That is 1/400 of the 293 Mt.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2050,   2027,       2050,       0.7325
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_linear_trajectory_leaves_single_year_budgets_whole(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2035, 2035, "linear_trajectory_to_zero")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # Only 2035 is modelled, so each rhs is that budget's share for 2035.
    #
    # 1. As in the tests above, the trajectory is at 29.3 Mt a year at the
    #    start of 2031 and falls 1.465 a year.
    # 2. Half way through 2035 is 4.5 years later, so nem_2027_to_2050's
    #    share is 29.3 - 4.5 x 1.465 = 22.7075.
    # 3. nem_2035_to_2035 is on a single year. It takes no part in the
    #    trajectory and keeps its whole 23, not the trajectory's 22.7075.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2050,   2027,       2050,       22.7075
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_linear_trajectory_with_only_single_year_budgets(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2035_to_2035,   2035,       2035,       23.0
        nem_2040_to_2040,   2040,       2040,       10.0
    """)
    config = _config(sample_model_config, 2035, 2040, "linear_trajectory_to_zero")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # With no multi-year budgets there is no trajectory, and nothing to scale.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2035_to_2035,   2035,       2035,       23.0
        nem_2040_to_2040,   2040,       2040,       10.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_linear_trajectory_three_nested_budgets(csv_str_to_df, sample_model_config):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2040,   2027,       2040,       500.0
    """)
    config = _config(sample_model_config, 2031, 2031, "linear_trajectory_to_zero")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # Only 2031 is modelled, so each rhs is the trajectory's emissions in
    # 2031. Whatever order the rows are in, the trajectory runs through the
    # budgets in order of length, and is worked out from the last back.
    #
    # 1. 2041 to 2050 is left 583 - 500 = 83 Mt over 10 years, an average of
    #    8.3. It ends at zero, so it starts at 2 x 8.3 - 0 = 16.6 Mt a year.
    # 2. 2031 to 2040 is left 500 - 290 = 210 Mt over 10 years, an average of
    #    21. It ends at 16.6, so it starts at 2 x 21 - 16.6 = 25.4.
    # 3. From 25.4 to 16.6 over 10 years is a fall of 0.88 a year. Half way
    #    through 2031 the rate is 25.4 - 0.5 x 0.88 = 24.96.
    # 4. 2031 is in the window of both longer budgets, so each gets 24.96.
    #    nem_2027_to_2030 has no modelled year and is dropped.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2040,   2027,       2040,       24.96
        nem_2027_to_2050,   2027,       2050,       24.96
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_linear_trajectory_allows_a_flat_stretch(csv_str_to_df, sample_model_config):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2040,   2027,       2040,       583.0
        nem_2027_to_2050,   2027,       2050,       729.5
    """)
    config = _config(sample_model_config, 2031, 2034, "linear_trajectory_to_zero")

    result = _translate_carbon_budgets(carbon_budgets, config)

    # The modelled years are 2031 to 2034, the first four of the ten years
    # between the first two budgets' ends.
    #
    # 1. 2041 to 2050 is left 729.5 - 583 = 146.5 Mt over 10 years, an average
    #    of 14.65. It ends at zero, so it starts at 2 x 14.65 - 0 = 29.3.
    # 2. 2031 to 2040 is left 583 - 290 = 293 Mt over 10 years, an average of
    #    29.3. It ends at 29.3, so it starts at 2 x 29.3 - 29.3 = 29.3.
    # 3. The trajectory is flat at 29.3 from 2031 to 2040. Only a rise is an
    #    error, so this is allowed.
    # 4. Each modelled year's emissions are 29.3, so the four add up to
    #    4 x 29.3 = 117.2 for both of the budgets they are in.
    #
    # Modelling all of 2031 to 2040 would not show the stretch is flat: those
    # ten years add up to 293 whatever shape the trajectory takes between
    # them. Four years at one end only add up to 117.2 if the rate really is
    # 29.3 throughout.
    expected = csv_str_to_df("""
        constraint_name,    year_from,  year_to,    rhs
        nem_2027_to_2040,   2027,       2040,       117.2
        nem_2027_to_2050,   2027,       2050,       117.2
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_linear_trajectory_raises_when_budgets_start_in_different_years(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2031_to_2050,   2031,       2050,       293.0
        nem_2029_to_2060,   2029,       2060,       400.0
    """)
    config = _config(sample_model_config, 2027, 2050, "linear_trajectory_to_zero")

    # The earliest start is 2027. The other two budgets start later.
    expected_message = (
        "Carbon budgets that start later than the earliest budget, or end in the "
        "same year as another: ['nem_2029_to_2060', 'nem_2031_to_2050']. With "
        "carbon_budget_scaling 'linear_trajectory_to_zero', the multi-year "
        "budgets must all start in the same year and end in different years."
    )
    with pytest.raises(ValueError, match=re.escape(expected_message)):
        _translate_carbon_budgets(carbon_budgets, config)


def test_linear_trajectory_raises_on_unnested_budgets_even_when_fully_modelled(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2031_to_2050,   2031,       2050,       293.0
    """)
    config = _config(sample_model_config, 2025, 2052, "linear_trajectory_to_zero")

    # Both windows sit inside 2025 to 2052, so neither budget needs scaling.
    # The budgets are still checked, and nem_2031_to_2050 starts later than
    # 2027.
    expected_message = (
        "Carbon budgets that start later than the earliest budget, or end in the "
        "same year as another: ['nem_2031_to_2050']. With "
        "carbon_budget_scaling 'linear_trajectory_to_zero', the multi-year "
        "budgets must all start in the same year and end in different years."
    )
    with pytest.raises(ValueError, match=re.escape(expected_message)):
        _translate_carbon_budgets(carbon_budgets, config)


def test_linear_trajectory_raises_when_budgets_end_in_the_same_year(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = csv_str_to_df("""
        budget_id,  year_from,  year_to,    budget
        federal,    2027,       2050,       583.0
        state,      2027,       2050,       600.0
        nem_2030,   2027,       2030,       290.0
    """)
    config = _config(sample_model_config, 2027, 2050, "linear_trajectory_to_zero")

    # Both of the budgets that share an end year are named.
    expected_message = "or end in the same year as another: ['federal', 'state']."
    with pytest.raises(ValueError, match=re.escape(expected_message)):
        _translate_carbon_budgets(carbon_budgets, config)


def test_linear_trajectory_raises_when_emissions_would_rise(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2040,   2027,       2040,       350.0
        nem_2027_to_2050,   2027,       2050,       450.0
    """)
    config = _config(sample_model_config, 2027, 2050, "linear_trajectory_to_zero")

    # 1. 2041 to 2050 is left 450 - 350 = 100 Mt over 10 years, an average of
    #    10. It ends at zero, so it starts at 2 x 10 - 0 = 20 Mt a year.
    # 2. 2031 to 2040 is left 350 - 290 = 60 Mt over 10 years, an average of
    #    6. It ends at 20, so it would start at 2 x 6 - 20 = -8, and rise.
    expected_message = (
        "With carbon_budget_scaling 'linear_trajectory_to_zero', emissions would "
        "have to rise over the years [(2031, 2040)] to use the carbon budgets "
        "in full. Either the budgets allow too many emissions after those "
        "years for the trajectory to keep falling, or a longer budget is "
        "smaller than a shorter one."
    )
    with pytest.raises(ValueError, match=re.escape(expected_message)):
        _translate_carbon_budgets(carbon_budgets, config)


def test_linear_trajectory_raises_when_a_longer_budget_is_smaller(
    csv_str_to_df, sample_model_config
):
    carbon_budgets = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2027_to_2050,   2027,       2050,       200.0
    """)
    config = _config(sample_model_config, 2027, 2050, "linear_trajectory_to_zero")

    # The longer budget leaves 200 - 290 = -90 Mt for 2031 to 2050, which
    # would need a rate below zero at the start of 2031 rising to zero by the
    # end of 2050.
    expected_message = "emissions would have to rise over the years [(2031, 2050)]"
    with pytest.raises(ValueError, match=re.escape(expected_message)):
        _translate_carbon_budgets(carbon_budgets, config)


# --- Models filtered to some regions ---


def test_warns_when_model_is_region_filtered(
    csv_str_to_df, sample_model_config, caplog
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2025, 2052, "none")
    config.filter_by_nem_regions = ["VIC", "NSW"]

    with caplog.at_level("WARNING"):
        _translate_carbon_budgets(carbon_budgets, config)

    assert (
        "The carbon budgets are for the whole NEM but the model is filtered to "
        "['NSW', 'VIC']: the budgets are applied to it unscaled."
    ) in caplog.text


def test_does_not_warn_when_model_is_not_filtered(
    csv_str_to_df, sample_model_config, caplog
):
    carbon_budgets = _step_change_budgets(csv_str_to_df)
    config = _config(sample_model_config, 2025, 2052, "none")

    with caplog.at_level("WARNING"):
        _translate_carbon_budgets(carbon_budgets, config)

    assert "The carbon budgets are for the whole NEM" not in caplog.text
