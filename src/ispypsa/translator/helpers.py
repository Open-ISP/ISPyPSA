import re

import pandas as pd


def _get_iteration_start_and_end_time(year_type: str, start_year: int, end_year: int):
    """Get the model start year, end year, and start/end month for iteration, which depend on
    financial vs calendar year.
    """
    if year_type == "fy":
        start_year = start_year - 1
        end_year = end_year
        month = 7
    else:
        start_year = start_year
        end_year = end_year + 1
        month = 1
    return start_year, end_year, month


def _annuitised_investment_costs(
    capital_cost: float, wacc: float, asset_lifetime: int
) -> float:
    """Calculate the cost of capital cost spread over the asset lifetime.

    Args:
        capital_cost: as float, typically in $/MW
        wacc: as float, weighted average cost of capital, an interest rate specifying
            how expensive it is to borrow money for the asset investment.
        asset_lifetime: as int, asset lifetime in years.

    Returns: float specifying the annuitised cost in $/MW/yr
    """
    return (capital_cost * wacc) / (1 - (1 + wacc) ** (-1.0 * asset_lifetime))


def _get_commissioning_or_build_year_as_int(
    commissioning_date_str: str, default_build_year: int, year_type: str = "fy"
) -> int:
    """Return build year of CAA generator as an int, or 0 if no build year given.

    Build years are related to investment periods, so the year type (financial or
    calendar) is used to determine the correct integer year to return.

    Args:
        commissioning_date_str: string describing commissioning date of committed, anticipated
            or additional generator. Expects a date string in the format "%Y-%m-%d".
        default_build_year: integer to return if no build year is given. Typically
            this will be the first investment period year.
        year_type: str which should be "fy" or "calendar". If "fy" then investment
            periods are interpreted as specifying financial years (according to the
            calendar year the financial year ends in).

    Returns: integer, default_build_year or year of commissioning date.
    """
    if not isinstance(commissioning_date_str, str):
        return default_build_year
    else:
        commissioning_date = pd.to_datetime(commissioning_date_str, format="%Y-%m-%d")
        if commissioning_date.month < 7 or year_type == "calendar":
            return int(commissioning_date.year)
        else:
            return int(commissioning_date.year) + 1


def _get_financial_year_int_from_string(
    input_string: str, quantity: str, year_type: str = "fy"
) -> int:
    """
    Takes a string containing a financial year represented in the format YYYY_YY
    and returns the financial year as an int.

    Financial years are referred to by the end year of the financial year.
    For example, if the input string is "2023_24" then the returned int is 2024.

    Args:
        input_string: string representing a financial year in the format YYYY_YY
        quantity: string noting what quantity is being translated when this function
            is called; used for error messaging. For example, "generator marginal costs".
        year_type: str which should be "fy" or "calendar".

    Returns:
        int representing the financial year. For example, if the input string is "2023_24"
        then the returned int is 2024.

    Raises:
        ValueError if the input string does not match the expected format.
    """
    if year_type == "fy":
        check_format = re.match(
            r"^(?P<start_year>\d{4})_(?P<end_year>\d{2})($|_)", input_string
        )
        if check_format:
            start_year_string = check_format.groupdict()["start_year"]
            # adding 1 to start year instead of just returning end year to avoid
            # any potential century crossover issues
            financial_year_int = int(start_year_string) + 1
            return financial_year_int
        raise ValueError(
            f"Invalid financial year string for {quantity}: {input_string}"
        )
    elif year_type == "calendar":
        raise NotImplementedError(
            f"Calendar years are not implemented yet for {quantity}"
        )
    else:
        raise ValueError(f"Unknown year_type: {year_type}")


def _add_investment_periods_as_build_years(
    df: pd.DataFrame, investment_periods: list[int]
):
    """
    Add investment periods as build years to a pd.DataFrame, adding duplicate rows
    for each investment period as needed.

    Args:
        df (pd.DataFrame): pd.DataFrame to add investment periods to.
        investment_periods (list[int]): list of investment periods.

    Returns:
        pd.DataFrame: pd.DataFrame with added investment periods as build years.
    """
    df["build_year"] = "investment_periods"
    df["build_year"] = df["build_year"].map({"investment_periods": investment_periods})
    df = df.explode("build_year").reset_index(drop=True)
    df["build_year"] = df["build_year"].astype("int64")

    return df
