"""Helpers that the generators and storage translators share: the build years
and lifetimes of existing units, the bus each unit connects to, and the
capital costs of new entrants.
"""

# TODO: expand this docstring to the CLAUDE.md module layout once the stubs
# are filled in.

import logging

import pandas as pd

from ispypsa.model_years import _model_year_of_dates, _model_year_start

# The network_geography column that holds each REZ's parent node, for each
# regional_granularity. Only the sub_regions geography has a subregion_id column.
_PARENT_NODE_COLUMNS = {
    "sub_regions": "subregion_id",
    "nem_regions": "region_id",
    "single_region": "region_id",
}


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
    blank closure_year gives a NaN lifetime, which PyPSA reads as infinite: the
    unit isn't retired during the modelling horizon.

    OPINIONATED: the IASR gives closure_year as the calendar year in which the
    unit expects to stop supplying electricity, with no month. Running the unit
    through that model year puts its closure on 1 July of the closure year under
    financial years (within six months of the real date), and up to 12 months
    late under calendar years.
    """
    return closure_years - build_years + 1


def _drop_units_closed_before_first_investment_period(
    units: pd.DataFrame, first_investment_period: int, table_name: str
) -> pd.DataFrame:
    """Drops the existing units that have a closure_year earlier than the first
    investment period, because PyPSA would never make them active. Their names
    are logged once at INFO as a sorted list, along with table_name, the table
    the units came from. A unit with a blank closure_year doesn't close, so it
    is kept.

    I/O Example (first_investment_period=2026):
        units:
            name     closure_year
            TORRB1   2026          # runs through 2026: kept
            LIDDELL1 2023          # closed before 2026: dropped
            NEWGEN1                # blank: kept

        returns:
            name     closure_year
            TORRB1   2026
            NEWGEN1
    """
    closed = units["closure_year"] < first_investment_period
    if closed.any():
        logging.info(
            f"Units in {table_name} dropped because they close before the first "
            f"investment period ({first_investment_period}): "
            f"{sorted(units.loc[closed, 'name'])}"
        )
    return units[~closed].reset_index(drop=True)


def _drop_units_built_after_last_investment_period(
    units: pd.DataFrame, last_investment_period: int, table_name: str
) -> pd.DataFrame:
    """Drops the existing units that have a build_year later than the last
    investment period. PyPSA only makes a unit active in an investment period
    that starts in or after its build year, so these units would never be
    active, even when the modelling horizon runs past the last investment
    period. Their names are logged once at INFO as a sorted list, along with
    table_name, the table the units came from.

    I/O Example (last_investment_period=2030):
        units:
            name      build_year
            PLANNED1  2030        # built in the last investment period: kept
            PLANNED2  2031        # built after 2030: dropped

        returns:
            name      build_year
            PLANNED1  2030
    """
    built_late = units["build_year"] > last_investment_period
    if built_late.any():
        logging.info(
            f"Units in {table_name} dropped because they are built after the last "
            f"investment period ({last_investment_period}): "
            f"{sorted(units.loc[built_late, 'name'])}"
        )
    return units[~built_late].reset_index(drop=True)


def _buses_for_geo_ids(
    geo_ids: pd.Series,
    network_geography: pd.DataFrame,
    rezs: str,
    regional_granularity: str,
) -> pd.Series:
    """The bus each unit connects to: its geo_id, except that under
    rezs="attached_to_parent_node" a REZ geo_id is replaced by its parent node
    from network_geography.

    The templater has already mapped each sub-region geo_id to the node at the
    configured granularity (for example NSW under nem_regions), so only REZ
    geo_ids change here. The parent node's column depends on the granularity
    (see _PARENT_NODE_COLUMNS).

    I/O Example (regional_granularity="sub_regions"):
        geo_ids:
            CNSW
            N3

        network_geography:
            geo_id  geo_type   region_id  subregion_id
            CNSW    subregion  NSW        CNSW
            N3      rez        NSW        CNSW

        rezs="discrete_nodes" returns:
            CNSW
            N3

        rezs="attached_to_parent_node" returns:
            CNSW
            CNSW     # N3's parent sub-region
    """
    if rezs == "discrete_nodes":
        return geo_ids
    rez_rows = network_geography[network_geography["geo_type"] == "rez"]
    parent_column = _PARENT_NODE_COLUMNS[regional_granularity]
    parent_nodes = rez_rows.set_index("geo_id")[parent_column]
    # replace (rather than map) leaves the geo_ids that aren't REZs as they are.
    return geo_ids.replace(parent_nodes.to_dict())


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
