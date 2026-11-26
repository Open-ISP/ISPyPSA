"""Helpers that the generators and storage translators share: the build years
and lifetimes of existing units, the bus each unit connects to, and the
capital costs of new entrants.
"""

# TODO: expand this docstring to the CLAUDE.md module layout once the stubs
# are filled in.

import logging

import pandas as pd

from ispypsa.model_years import _model_year_of_dates, _model_year_start
from ispypsa.translator.helpers import (
    _annuitised_investment_costs,
    _resolve_wildcards,
)

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


def _resolve_build_costs(
    build_costs: pd.DataFrame, investment_periods: list[int]
) -> pd.DataFrame:
    """Resolves costs_new_entrant_build to one row per technology and
    investment period.

    A blank year is a wildcard (see the costs_new_entrant_build schema): the
    row's cost applies to every investment period that the technology has no
    row of its own for. Rows for other years are filtered out first, because
    the model doesn't use them and _resolve_wildcards would raise on them. A
    technology that still has no row for an investment period is left without
    one, so _raise_on_missing_build_costs names it after the merge.

    I/O Example (investment_periods=[2026, 2030]):
        build_costs:
            technology  year  cost
            OCGT        2026  1400000
            OCGT              1300000    # blank year: fills OCGT 2030 only
            OCGT        2027  1350000    # not an investment period: filtered out
            CCGT              2500000    # blank year: fills both

        returns:
            technology  year  cost
            OCGT        2026  1400000
            OCGT        2030  1300000
            CCGT        2026  2500000
            CCGT        2030  2500000
    """
    keep = build_costs["year"].isna() | build_costs["year"].isin(investment_periods)
    build_costs = _resolve_wildcards(
        build_costs[keep], {"year": investment_periods}, ["cost"]
    )
    # A blank year makes the column float, so cast it back to match build_year.
    build_costs["year"] = build_costs["year"].astype("int64")
    return build_costs


def _add_new_entrant_capital_costs(
    units: pd.DataFrame,
    build_costs: pd.DataFrame,
    connection_costs: pd.DataFrame,
    wacc: float,
) -> pd.DataFrame:
    """Adds an annuitised capital_cost to each new-entrant unit and build year,
    from its build, connection and system strength costs and its fixed O&M."""
    units = _merge_build_costs(units, build_costs)
    _raise_on_missing_build_costs(units)
    units = _merge_connection_costs(units, connection_costs)
    _raise_on_missing_connection_costs(units)
    units["capital_cost"] = _annuitised_capital_costs(units, wacc)
    return units


def _merge_build_costs(units: pd.DataFrame, build_costs: pd.DataFrame) -> pd.DataFrame:
    """Adds build_cost to each unit: the cost from costs_new_entrant_build for
    the unit's technology in its build year. A unit with no matching row gets a
    NaN build_cost.

    I/O Example:
        units:
            name       technology  build_year
            CNSW OCGT  OCGT        2026
            CNSW OCGT  OCGT        2030
            NQ CCGT    CCGT        2026        # no CCGT row for 2026

        build_costs:
            technology  year  cost
            OCGT        2026  1400000
            OCGT        2030  1300000
            CCGT        2030  2500000

        returns:
            name       technology  build_year  build_cost
            CNSW OCGT  OCGT        2026        1400000
            CNSW OCGT  OCGT        2030        1300000
            NQ CCGT    CCGT        2026        NaN
    """
    build_costs = build_costs.rename(
        columns={"year": "build_year", "cost": "build_cost"}
    )
    return units.merge(build_costs, on=["technology", "build_year"], how="left")


def _merge_connection_costs(
    units: pd.DataFrame, connection_costs: pd.DataFrame
) -> pd.DataFrame:
    """Adds connection_cost and system_strength_cost to each unit, from the
    costs_connection row for the unit's geo_id and technology in its build year.

    The match uses the unit's own geo_id rather than its bus, so a REZ unit
    finds its REZ's costs even when it connects to the parent node. A blank cost
    in costs_connection means there is no extra cost (the schema's nan_fill), so
    it becomes 0. In the 7.5 data these are the offshore wind rows, whose
    connection cost is part of their build cost. A unit with no matching row
    gets NaN costs.

    I/O Example:
        units:
            name        geo_id  technology      build_year
            N1 Wind     N1      Wind            2026
            N10 WOF     N10     Wind offshore   2026        # blank connection_cost
            CNSW OCGT   CNSW    OCGT            2026        # no costs_connection row

        connection_costs:
            geo_id  technology     year  connection_cost  system_strength_cost
            N1      Wind           2026  150000           60000
            N10     Wind offshore  2026                   160000

        returns:
            name        geo_id  technology     build_year  connection_cost  system_strength_cost
            N1 Wind     N1      Wind           2026        150000           60000
            N10 WOF     N10     Wind offshore  2026        0                160000
            CNSW OCGT   CNSW    OCGT           2026        NaN              NaN
    """
    connection_costs = connection_costs.rename(columns={"year": "build_year"})
    connection_costs = connection_costs.fillna(
        {"connection_cost": 0.0, "system_strength_cost": 0.0}
    )
    return units.merge(
        connection_costs, on=["geo_id", "technology", "build_year"], how="left"
    )


def _raise_on_missing_build_costs(units: pd.DataFrame) -> None:
    """Raises when a unit has no build cost for its build year, naming each
    technology and build year that is missing one.

    I/O Example:
        units:
            technology  build_year  build_cost
            OCGT        2026        1400000
            CCGT        2026        NaN

        raises:
            ValueError: New entrants with no build cost (technology, build_year):
            [('CCGT', 2026)]
    """
    missing = units[units["build_cost"].isna()]
    if not missing.empty:
        keys = sorted(set(zip(missing["technology"], missing["build_year"])))
        raise ValueError(
            f"New entrants with no build cost (technology, build_year): {keys}"
        )


def _raise_on_missing_connection_costs(units: pd.DataFrame) -> None:
    """Raises when a unit has no costs_connection row for its build year, naming
    each geo_id, technology and build year that is missing one. A blank cost on
    a row that does exist has already been set to 0 by _merge_connection_costs,
    so only a missing row is NaN here.

    I/O Example:
        units:
            geo_id  technology  build_year  connection_cost
            N1      Wind        2026        150000
            CNSW    OCGT        2026        NaN

        raises:
            ValueError: New entrants with no connection cost
            (geo_id, technology, build_year): [('CNSW', 'OCGT', 2026)]
    """
    missing = units[units["connection_cost"].isna()]
    if not missing.empty:
        keys = sorted(
            set(zip(missing["geo_id"], missing["technology"], missing["build_year"]))
        )
        raise ValueError(
            "New entrants with no connection cost (geo_id, technology, build_year): "
            f"{keys}"
        )


def _annuitised_capital_costs(units: pd.DataFrame, wacc: float) -> pd.Series:
    """The capital cost of each unit in $/MW/year: the annuity of the upfront
    costs at wacc over the unit's economic lifetime, plus its fixed O&M.

    The upfront costs are (build_cost + connection_cost) × lcf_build / 100 +
    system_strength_cost. The schema applies lcf_build to the build and
    connection costs only, so system_strength_cost isn't scaled. The fixed O&M
    is fom × lcf_om / 100. Blank cells take the schema's nan_fill values: a
    blank lcf_build or lcf_om means no locational scaling (100), and a blank
    lifetime_economic falls back to lifetime_technical.

    I/O Example (wacc=0.07):
        units:
            build_cost  connection_cost  system_strength_cost  lcf_build  fom    lcf_om  lifetime_economic  lifetime_technical
            1000000     100000           50000                 110        20000  105     25                 30
            1000000     0                0                                20000          NaN                30

        returns:
            129121.25    # annuity of 1260000 over 25 years (108121.25) + 21000
            100586.40    # annuity of 1000000 over 30 years (80586.40) + 20000
    """
    lcf_build = units["lcf_build"].fillna(100.0)
    lcf_om = units["lcf_om"].fillna(100.0)
    economic_lifetimes = units["lifetime_economic"].fillna(units["lifetime_technical"])
    upfront_costs = (
        units["build_cost"] + units["connection_cost"]
    ) * lcf_build / 100 + units["system_strength_cost"]
    annuities = _annuitised_investment_costs(upfront_costs, wacc, economic_lifetimes)
    return annuities + units["fom"] * lcf_om / 100
