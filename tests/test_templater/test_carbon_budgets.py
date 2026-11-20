import pandas as pd

from ispypsa.templater.carbon_budgets import (
    _add_window_years,
    _drop_rows_without_a_window,
    _template_carbon_budgets,
)

_NEM_CARBON_BUDGET_COLUMNS = ["Unnamed: 1", "Slower Growth", "Step Change"]


def test_template_carbon_budgets(csv_str_to_df):
    nem_carbon_budget = csv_str_to_df("""
        Unnamed: 1,                                                         Slower Growth,  Step Change
        Global mean temperature increase by 2100 (°C),                      2.6°C,          1.8°C
        Long-term NEM carbon budget from 2026-27 to 2049-50 (Mt CO2-e),     727,            583
        Federal Government's 2030 carbon budget from 2026-27 to 2029-30,    321,            290
        Federal Government's 2035 emissions reduction target in 2034-35,    24,             23
    """)

    result = _template_carbon_budgets(nem_carbon_budget, "Step Change")

    # The Step Change column is taken. The temperature row names no financial
    # year, so it is dropped. The 2035 target names one year (2034-35), so its
    # window is 2035 alone.
    expected = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2050,   2027,       2050,       583.0
        nem_2027_to_2030,   2027,       2030,       290.0
        nem_2035_to_2035,   2035,       2035,       23.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_carbon_budgets_selects_the_scenario(csv_str_to_df):
    nem_carbon_budget = csv_str_to_df("""
        Unnamed: 1,                                     Slower Growth,  Step Change
        NEM carbon budget from 2026-27 to 2049-50,      727,            583
    """)

    result = _template_carbon_budgets(nem_carbon_budget, "Slower Growth")

    expected = csv_str_to_df("""
        budget_id,          year_from,  year_to,    budget
        nem_2027_to_2050,   2027,       2050,       727.0
    """)
    pd.testing.assert_frame_equal(result, expected)


def test_template_carbon_budgets_empty_table(csv_str_to_df):
    nem_carbon_budget = pd.DataFrame(columns=_NEM_CARBON_BUDGET_COLUMNS)

    result = _template_carbon_budgets(nem_carbon_budget, "Step Change")

    expected = csv_str_to_df("""
        budget_id,  year_from,  year_to,    budget
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_drop_rows_without_a_window(csv_str_to_df, caplog):
    budgets = csv_str_to_df("""
        description,                                        budget
        Global mean temperature increase by 2100 (°C),      1.8°C
        2035 emissions reduction target in 2034-35,         23
    """)

    with caplog.at_level("INFO"):
        result = _drop_rows_without_a_window(budgets)

    # The budget column still holds text here, as the temperature row's value
    # ("1.8°C") isn't a number.
    expected = csv_str_to_df(
        """
        description,                                        budget
        2035 emissions reduction target in 2034-35,         23
    """,
        dtype={"budget": str},
    )
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)
    assert (
        "Rows of nem_carbon_budget with no financial year in their description "
        "(not templated as carbon budgets): "
        "['Global mean temperature increase by 2100 (°C)']"
    ) in caplog.text


def test_drop_rows_without_a_window_does_not_log_when_nothing_is_dropped(
    csv_str_to_df, caplog
):
    budgets = csv_str_to_df("""
        description,                                        budget
        2035 emissions reduction target in 2034-35,         23
    """)

    with caplog.at_level("INFO"):
        result = _drop_rows_without_a_window(budgets)

    pd.testing.assert_frame_equal(result, budgets)
    assert "not templated as carbon budgets" not in caplog.text


def test_add_window_years(csv_str_to_df):
    budgets = csv_str_to_df("""
        description
        NEM carbon budget from 2026-27 to 2049-50 (Mt CO2-e)
        2035 emissions reduction target in 2034-35
        Budget from 2098-99 to 2099-00
    """)

    result = _add_window_years(budgets)

    # A description naming one year gives a one-year window. 2099-00 is the
    # financial year ending 2100.
    expected = csv_str_to_df("""
        description,                                            year_from,  year_to
        NEM carbon budget from 2026-27 to 2049-50 (Mt CO2-e),   2027,       2050
        2035 emissions reduction target in 2034-35,             2035,       2035
        Budget from 2098-99 to 2099-00,                         2099,       2100
    """)
    pd.testing.assert_frame_equal(result, expected)
