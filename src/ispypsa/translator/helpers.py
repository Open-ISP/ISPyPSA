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
