import logging

import pandas as pd

from ispypsa.templater.helpers import _financial_year_string_to_end_year_int

_CARBON_BUDGET_COLUMNS = ["budget_id", "year_from", "year_to", "budget"]

# The description column has no header in the workbook, so the workbook parser
# names it after its position.
_DESCRIPTION_COLUMN = "Unnamed: 1"

# A financial year as the workbook writes it: four digits, a hyphen, two digits.
# For example it matches "2026-27" and "2049-50" in
# "NEM carbon budget from 2026-27 to 2049-50 (Mt CO2-e)". It has no capture
# groups, so str.findall returns each whole match.
_FINANCIAL_YEAR_PATTERN = r"\d{4}-\d{2}"


def _template_carbon_budgets(
    nem_carbon_budget: pd.DataFrame, scenario: str
) -> pd.DataFrame:
    """Templates the carbon_budgets table, the limits on the whole system's
    emissions, from the IASR workbook's NEM carbon budgets for one scenario.

    Each row of nem_carbon_budget whose description names a financial year
    becomes a budget, sized by the scenario's column (in Mt CO2-e). Its window
    is read from the description: two years ("from 2026-27 to 2049-50") are
    the window's first and last years, and a single year ("in 2034-35") is a
    limit on that year's emissions alone. The years are returned as financial
    years ending. Rows that name no financial year aren't budgets, so they are
    dropped and logged.

    Only the NEM-wide budgets are templated, so every budget_id starts with
    "nem_". The workbook's state budgets (regional_carbon_budget_summary) are
    left out.

    I/O Example (scenario "Step Change"):
        nem_carbon_budget:
            Unnamed: 1                                             Slower Growth  Step Change
            Global mean temperature increase by 2100 (°C)          2.6°C          1.8°C        # no financial year: dropped
            Long-term budget from 2026-27 to 2049-50 (Mt CO2-e)    727            583
            2035 emissions reduction target in 2034-35             24             23           # one year: a limit on 2035 alone

        returns:
            budget_id         year_from  year_to  budget
            nem_2027_to_2050  2027       2050     583.0
            nem_2035_to_2035  2035       2035     23.0
    """
    logging.info("Creating a template for carbon budgets")
    budgets = nem_carbon_budget.rename(
        columns={_DESCRIPTION_COLUMN: "description", scenario: "budget"}
    )
    budgets = _drop_rows_without_a_window(budgets)
    budgets = _add_window_years(budgets)
    budgets["budget"] = pd.to_numeric(budgets["budget"]).astype("float64")
    budgets["budget_id"] = (
        "nem_"
        + budgets["year_from"].astype(str)
        + "_to_"
        + budgets["year_to"].astype(str)
    )
    return budgets[_CARBON_BUDGET_COLUMNS].reset_index(drop=True)


def _drop_rows_without_a_window(budgets: pd.DataFrame) -> pd.DataFrame:
    """Drops the rows whose description names no financial year, as they give
    no window to apply a budget over, and logs them.

    I/O Example:
        budgets:
            description                                      budget
            Global mean temperature increase by 2100 (°C)    1.8°C   # dropped
            2035 emissions reduction target in 2034-35       23

        returns:
            description                                      budget
            2035 emissions reduction target in 2034-35       23

        logs: "Rows of nem_carbon_budget with no financial year in their
        description (not templated as carbon budgets): ['Global mean
        temperature increase by 2100 (°C)']"
    """
    has_window = budgets["description"].str.contains(_FINANCIAL_YEAR_PATTERN)
    dropped = sorted(budgets.loc[~has_window, "description"])
    if dropped:
        logging.info(
            f"Rows of nem_carbon_budget with no financial year in their "
            f"description (not templated as carbon budgets): {dropped}"
        )
    return budgets[has_window].copy()


def _add_window_years(budgets: pd.DataFrame) -> pd.DataFrame:
    """Adds each budget's year_from and year_to: the first and last financial
    years named in its description, as financial years ending.

    Every description must name at least one financial year (see
    _drop_rows_without_a_window).

    I/O Example:
        budgets:
            description
            NEM carbon budget from 2026-27 to 2049-50 (Mt CO2-e)
            2035 emissions reduction target in 2034-35             # one year

        returns:
            description                                            year_from  year_to
            NEM carbon budget from 2026-27 to 2049-50 (Mt CO2-e)   2027       2050
            2035 emissions reduction target in 2034-35             2035       2035
    """
    financial_years = budgets["description"].str.findall(_FINANCIAL_YEAR_PATTERN)
    budgets = budgets.copy()
    budgets["year_from"] = financial_years.str[0].map(
        _financial_year_string_to_end_year_int
    )
    budgets["year_to"] = financial_years.str[-1].map(
        _financial_year_string_to_end_year_int
    )
    return budgets.astype({"year_from": "int64", "year_to": "int64"})
