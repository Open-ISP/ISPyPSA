"""Helpers that the generators and storage translators share: the build years
and lifetimes of existing units, the bus each unit connects to, and the
capital costs of new entrants.
"""

# TODO: expand this docstring to the CLAUDE.md module layout once the stubs
# are filled in.

import pandas as pd

from ispypsa.model_years import _model_year_of_dates, _model_year_start


def _build_years_from_commissioning_dates(
    commissioning_dates: pd.Series, year_type: str, start_year: int
) -> pd.Series:
    """The build year of each existing/planned unit: the model year of its
    commissioning date, or start_year - 1 when the date is blank or falls
    before the model starts.

    start_year - 1 is the build year for anything already built when the model
    starts, as for the existing links in network.py. The dates arrive as text
    in the templated format, for example "01/08/2025".

    I/O Example (year_type="fy", start_year=2026):
        commissioning_dates  returns
        01/08/2025           2026
        01/07/2027           2028
        01/01/2020           2025     # before the model starts
                             2025     # blank
    """
    dates = pd.to_datetime(commissioning_dates, format="%d/%m/%Y")
    # Blank dates are filled with the first day of model year start_year - 1,
    # so they take the same build year as units built before the model starts.
    dates = dates.fillna(_model_year_start(year_type, start_year - 1))
    build_years = _model_year_of_dates(year_type, dates)
    return build_years.clip(lower=start_year - 1)


def _lifetimes_from_closure_years(
    closure_years: pd.Series, build_years: pd.Series
) -> pd.Series:
    """The PyPSA lifetime of each existing unit: closure_year + 1 - build_year,
    so the unit runs through the model year labelled with its closure year. A
    blank closure_year gives an infinite lifetime, because the unit isn't
    retired during the modelling horizon.

    OPINIONATED: the IASR gives closure_year as the calendar year in which the
    unit expects to stop supplying electricity, with no month. Running the unit
    through that model year puts its closure on 1 July of the closure year under
    financial years (within six months of the real date), and up to 12 months
    late under calendar years.
    """
    raise NotImplementedError


def _drop_units_closed_before_first_investment_period(
    units: pd.DataFrame, first_investment_period: int
) -> pd.DataFrame:
    """Drops the existing units that are never active in the model, that is,
    those with a closure_year earlier than the first investment period, and
    logs their names once at INFO as a sorted list."""
    raise NotImplementedError


def _buses_for_geo_ids(
    geo_ids: pd.Series, network_geography: pd.DataFrame, rezs: str
) -> pd.Series:
    """The bus each unit connects to: its geo_id, except that under
    rezs="attached_to_parent_node" a REZ geo_id is replaced by its parent node
    from network_geography."""
    # TODO: check whether the parent node depends on regional_granularity
    # (subregion_id or region_id), which would need another parameter.
    raise NotImplementedError


def _add_new_entrant_capital_costs(
    units: pd.DataFrame,
    build_costs: pd.DataFrame,
    connection_costs: pd.DataFrame,
    wacc: float,
) -> pd.DataFrame:
    """Adds an annuitised capital_cost to each new-entrant unit and build year,
    from its build, connection and system strength costs and its fixed O&M."""
    units = _merge_build_costs(units, build_costs)
    units = _merge_connection_costs(units, connection_costs)
    _raise_on_missing_costs(units)
    units["capital_cost"] = _annuitised_capital_costs(units, wacc)
    return units


def _merge_build_costs(units: pd.DataFrame, build_costs: pd.DataFrame) -> pd.DataFrame:
    """Adds each unit's build cost (the cost column of costs_new_entrant_build),
    matched on technology and year == build_year."""
    raise NotImplementedError


def _merge_connection_costs(
    units: pd.DataFrame, connection_costs: pd.DataFrame
) -> pd.DataFrame:
    """Adds connection_cost and system_strength_cost from costs_connection,
    matched on geo_id, technology and year == build_year. The match uses the
    unit's own geo_id rather than its bus, so a REZ unit finds its REZ's costs
    even when it connects to the parent node."""
    raise NotImplementedError


def _raise_on_missing_costs(units: pd.DataFrame) -> None:
    """Raises when a unit has no build or connection cost for its build year,
    naming each technology and year that is missing one."""
    raise NotImplementedError


def _annuitised_capital_costs(units: pd.DataFrame, wacc: float) -> pd.Series:
    """The capital cost of each unit in $/MW/year: the annuity of
    ((cost + connection_cost) × lcf_build / 100 + system_strength_cost) at wacc
    over lifetime_economic (or lifetime_technical where that is blank), plus
    fom × lcf_om / 100."""
    raise NotImplementedError
