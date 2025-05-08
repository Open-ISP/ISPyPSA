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


def _get_build_year_as_int(date: str, year_type: str = "fy") -> int:
    """Return build year of CAA generator as an int, or 0 if no build year given.

    Build years are related to investment periods, so the year type (financial or
    calendar) is used to determine the correct integer year to return.

    Args:
        date: string describing commissioning date of committed, anticipated or
            additional generator.
        year_type: str which should be "fy" or "calendar". If "fy" then investment
            period ints are interpreted as specifying financial years (according to the
            calendar year the financial year ends in).

    Returns: integer build year or 0.
    """
    build_year = 0
    if not isinstance(date, str):
        return build_year
    else:
        date = pd.to_datetime(date, format="%Y-%m-%d")
        if date.month < 7 or year_type == "calendar":
            return int(date.year)
        else:
            return int(date.year) + 1
