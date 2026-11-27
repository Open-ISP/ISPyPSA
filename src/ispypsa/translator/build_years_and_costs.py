"""The steps that the generators and storage translators share.

_build_existing_units turns an existing/planned table into fixed-capacity
units with build years, lifetimes and buses. _build_new_entrant_units turns a
new-entrant table into extendable units, one per option and investment period,
each with a capital cost. Each translator then renames the columns (see
_EXISTING_UNIT_COLUMN_RENAMES and _NEW_ENTRANT_COLUMN_RENAMES) and adds the
columns that only its own component has.
"""

import logging

import pandas as pd

from ispypsa.config import ModelConfig
from ispypsa.model_years import _model_year_of_dates, _model_year_start
from ispypsa.translator.helpers import (
    _add_investment_periods_as_build_years,
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

# The renames from templated column names to PyPSA (or isp_) names that the
# generators and storage translators share. They're applied after
# _build_existing_units and _build_new_entrant_units, because those steps still
# use the templated names: the cost merges match on technology, and the annuity
# uses lifetime_technical. Callers pass errors="raise", so that a missing
# column stops the run, until schema validation checks the input tables.
_EXISTING_UNIT_COLUMN_RENAMES = {
    "power_station": "isp_name",
    "fuel_type": "carrier",
    "capacity": "p_nom",
    "technology": "isp_technology_type",
}
_NEW_ENTRANT_COLUMN_RENAMES = {
    "fuel_type": "carrier",
    "lifetime_technical": "lifetime",
    "technology": "isp_technology_type",
}


def _build_existing_units(
    units: pd.DataFrame,
    network_geography: pd.DataFrame,
    config: ModelConfig,
    table_name: str,
) -> pd.DataFrame:
    """Turns the rows of an existing/planned table (generators_existing_planned
    or storage_existing_planned, named by table_name) into units with a fixed
    capacity, which PyPSA keeps in the model from their build year until they
    close.

    The build year is the model year of the unit's commissioning date. A unit
    with a blank commissioning date, or one before the model starts, is already
    built, so its build year is start_year - 1.

    The lifetime is closure_year + 1 - build_year, so the unit runs through the
    model year labelled with its closure year. This is an opinionated rule (see
    _lifetimes_from_closure_years). A blank closure_year gives a NaN lifetime,
    which PyPSA reads as a unit that never closes.

    Units that are built after the final investment period or close before the
    first are dropped, and logged at INFO with table_name.

    The input columns keep their templated names (see
    _EXISTING_UNIT_COLUMN_RENAMES).

    I/O Example:
        Inputs:
            units (capacity in MW):
                name      power_station  capacity  geo_id  commissioning_date  closure_year
                BW01      Bayswater      660.0     CNSW                        2033          # already built
                LIDDELL1  Liddell        500.0     CNSW                        2023          # closed before 2026: dropped
                PLANNED1  Wind Farm A    100.0     N3      01/08/2027                        # built in FY2028, no closure
                PLANNED2  Wind Farm B    200.0     N3      01/08/2030                        # built in FY2031: dropped

            network_geography:
                geo_id  geo_type   region_id  subregion_id
                CNSW    subregion  NSW        CNSW
                N3      rez        NSW        CNSW

            config.temporal.year_type: "fy"
            config.temporal.range.start_year: 2026
            config.temporal.capacity_expansion.investment_periods: [2026, 2030]
            config.network.nodes.rezs: "attached_to_parent_node"
            config.network.nodes.regional_granularity: "sub_regions"
            table_name: "generators_existing_planned"

        returns (lifetime in years, capital_cost in $/MW/year; commissioning_date and closure_year aren't shown):
            name      power_station  capacity  geo_id  build_year  lifetime  bus   p_nom_extendable  capital_cost
            BW01      Bayswater      660.0     CNSW    2025        9.0       CNSW  False             0.0
            PLANNED1  Wind Farm A    100.0     N3      2028        NaN       CNSW  False             0.0           # N3's parent node
    """
    units = units.copy()
    units["build_year"] = _build_years_from_commissioning_dates(
        units["commissioning_date"],
        config.temporal.year_type,
        config.temporal.range.start_year,
    )
    units["lifetime"] = _lifetimes_from_closure_years(
        units["closure_year"], units["build_year"]
    )
    investment_periods = config.temporal.capacity_expansion.investment_periods
    units = _drop_units_closed_before_first_investment_period(
        units, investment_periods[0], table_name
    )
    units = _drop_units_built_after_last_investment_period(
        units, investment_periods[-1], table_name
    )
    units["bus"] = _buses_for_geo_ids(
        units["geo_id"],
        network_geography,
        config.network.nodes.rezs,
        config.network.nodes.regional_granularity,
    )
    return _set_existing_unit_defaults(units)


def _build_new_entrant_units(
    units: pd.DataFrame,
    build_costs: pd.DataFrame,
    connection_costs: pd.DataFrame,
    network_geography: pd.DataFrame,
    config: ModelConfig,
) -> pd.DataFrame:
    """Turns the rows of a new-entrant table (generators_new_entrant or
    storage_new_entrant) into extendable units: one per option and investment
    period, each with an annuitised capital cost.

    Blank years in costs_new_entrant_build are resolved first, so that each
    technology has a build cost for every investment period (see
    _resolve_build_costs).

    PyPSA only builds capacity for a unit in its build_year, so each option is
    repeated once per investment period, with that investment period as its
    build year. This lets PyPSA build the option in any investment period. Each
    copy gets the capital cost for its own build year (see
    _add_new_entrant_capital_costs). It is named <name>_<build_year>, and the
    option's name becomes its isp_name. It starts with no capacity, and PyPSA
    chooses how much to build (see _set_new_entrant_defaults).

    The other input columns keep their templated names (see
    _NEW_ENTRANT_COLUMN_RENAMES, which makes lifetime_technical the PyPSA
    lifetime; the economic lifetime is only used in the annuity).

    I/O Example:
        Inputs:
            units (fom in $/MW/year, lcf_build and lcf_om in %, lifetimes in years):
                name       technology  geo_id  fom    lcf_build  lcf_om  lifetime_technical  lifetime_economic
                CNSW OCGT  OCGT        CNSW    20000  110        105     30                  25
                N3 Wind    Wind        N3      30000  100        100     30                  30

            build_costs ($/MW):
                technology  year  cost
                OCGT        2026  1000000
                OCGT              900000     # blank year: OCGT's cost for 2030
                Wind        2026  3000000
                Wind        2030  2800000

            connection_costs ($/MW):
                geo_id  technology  year  connection_cost  system_strength_cost
                CNSW    OCGT        2026  100000           50000
                CNSW    OCGT        2030  100000           50000
                N3      Wind        2026  150000           60000
                N3      Wind        2030  140000           55000

            network_geography:
                geo_id  geo_type   region_id  subregion_id
                CNSW    subregion  NSW        CNSW
                N3      rez        NSW        CNSW

            config.temporal.capacity_expansion.investment_periods: [2026, 2030]
            config.wacc: 0.07    # 7%
            config.network.nodes.rezs: "attached_to_parent_node"
            config.network.nodes.regional_granularity: "sub_regions"

        returns (p_nom in MW, capital_cost in $/MW/year; the input and cost columns that pass through aren't shown):
            isp_name   name            technology  bus   p_nom  p_nom_extendable  build_year  capital_cost
            CNSW OCGT  CNSW OCGT_2026  OCGT        CNSW  0.0    True              2026        129121.25
            CNSW OCGT  CNSW OCGT_2030  OCGT        CNSW  0.0    True              2030        119682.09    # the blank-year build cost
            N3 Wind    N3 Wind_2026    Wind        CNSW  0.0    True              2026        288682.36    # N3's costs, at N3's parent node
            N3 Wind    N3 Wind_2030    Wind        CNSW  0.0    True              2030        271356.28
    """
    investment_periods = config.temporal.capacity_expansion.investment_periods
    build_costs = _resolve_build_costs(build_costs, investment_periods)
    # _add_investment_periods_as_build_years changes its input, so it gets a copy.
    units = _add_investment_periods_as_build_years(units.copy(), investment_periods)
    units = _add_new_entrant_capital_costs(
        units, build_costs, connection_costs, config.wacc
    )
    units["bus"] = _buses_for_geo_ids(
        units["geo_id"],
        network_geography,
        config.network.nodes.rezs,
        config.network.nodes.regional_granularity,
    )
    return _set_new_entrant_defaults(units)


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


def _set_existing_unit_defaults(units: pd.DataFrame) -> pd.DataFrame:
    """Sets the PyPSA columns that every existing/planned unit has:
    p_nom_extendable False, because the model can't change its capacity, and
    capital_cost 0, because the model doesn't decide whether to build it.

    I/O Example:
        units:
            name  capacity
            BW01  660.0

        returns (capital_cost in $/MW/year):
            name  capacity  p_nom_extendable  capital_cost
            BW01  660.0     False             0.0
    """
    units = units.copy()
    units["p_nom_extendable"] = False
    units["capital_cost"] = 0.0
    return units


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
        build_costs ($/MW):
            technology  year  cost
            OCGT        2026  1400000
            OCGT              1300000    # blank year: fills OCGT 2030 only
            OCGT        2027  1350000    # not an investment period: filtered out
            CCGT              2500000    # blank year: fills both

        returns ($/MW):
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
    """Adds capital_cost to each new-entrant unit: the cost per MW per year of
    building the unit in its build year. The generators and storage
    translators both use this.

    The build cost comes from costs_new_entrant_build, matched on technology
    and build year. The connection and system strength costs come from
    costs_connection, matched on geo_id, technology and build year. A unit with
    no matching row in either table stops the run, with an error that names
    what's missing. The upfront costs are then annuitised over the unit's
    economic lifetime, and its fixed O&M is added (see
    _annuitised_capital_costs for the formula).

    I/O Example (wacc=0.07, columns abridged):
        units (lcf_build and lcf_om in %, fom in $/MW/year, lifetime_economic in years):
            name       geo_id  technology  build_year  lcf_build  fom    lcf_om  lifetime_economic
            CNSW OCGT  CNSW    OCGT        2026        110        20000  105     25

        build_costs ($/MW):
            technology  year  cost
            OCGT        2026  1000000

        connection_costs ($/MW):
            geo_id  technology  year  connection_cost  system_strength_cost
            CNSW    OCGT        2026  100000           50000

        returns (build_cost, connection_cost and system_strength_cost in $/MW; capital_cost in $/MW/year):
            name       build_cost  connection_cost  system_strength_cost  capital_cost
            CNSW OCGT  1000000     100000           50000                 129121.25
    """
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

        build_costs ($/MW):
            technology  year  cost
            OCGT        2026  1400000
            OCGT        2030  1300000
            CCGT        2030  2500000

        returns (build_cost in $/MW):
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

        connection_costs ($/MW):
            geo_id  technology     year  connection_cost  system_strength_cost
            N1      Wind           2026  150000           60000
            N10     Wind offshore  2026                   160000

        returns (connection_cost and system_strength_cost in $/MW):
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
        units (build_cost, connection_cost and system_strength_cost in $/MW; fom in $/MW/year;
               lcf_build and lcf_om in %; lifetimes in years):
            build_cost  connection_cost  system_strength_cost  lcf_build  fom    lcf_om  lifetime_economic  lifetime_technical
            1000000     100000           50000                 110        20000  105     25                 30
            1000000     0                0                                20000          NaN                30

        returns ($/MW/year):
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


def _set_new_entrant_defaults(units: pd.DataFrame) -> pd.DataFrame:
    """Sets the PyPSA columns that every new-entrant unit has: isp_name from
    the option's name; name as <isp_name>_<build_year>, so that each build year
    is its own PyPSA component; and p_nom 0 with p_nom_extendable True, so that
    the model starts with none of the option and chooses how much to build.

    I/O Example:
        units:
            name       build_year
            CNSW OCGT  2026
            CNSW OCGT  2030

        returns (p_nom in MW):
            name            build_year  isp_name   p_nom  p_nom_extendable
            CNSW OCGT_2026  2026        CNSW OCGT  0.0    True
            CNSW OCGT_2030  2030        CNSW OCGT  0.0    True
    """
    units = units.copy()
    units["isp_name"] = units["name"]
    units["name"] = units["isp_name"] + "_" + units["build_year"].astype(str)
    units["p_nom"] = 0.0
    units["p_nom_extendable"] = True
    return units
