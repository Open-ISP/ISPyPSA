"""Template the resource_limits table from IASR's initial_resource_limits table.

Resource limits cap how much new entrant capacity the model can build in each REZ.
This module reads IASR's per-REZ resource and land use limits and turns them into
one row per limit, keyed to the new entrant generators that limit applies to. For
now, only new entrant generators are covered by these limits.

Input: initial_resource_limits, one row per REZ. The headers are abbreviated here,
and the offshore floating and Accelerated Transition land use columns are left
out. The full names are in the constants below.

    REZ ID  Wind_High  Wind_Medium  Offshore -fixed  Solar   Penalty ($M/MW)  Land use_Wind  Land use_Solar
    Q1      570        1710                          1100    0.3              6764.2         16234.0
    Q8a     560        1680                          2796.8  0.3              3206.3         7695.2
    V8                              54996

Output: resource_limits, one row per limit:

    geo_id  resource_zone  technology  resource_type        limit_type  limit    relaxation_penalty
    Q1      Q1                         wind_high            resource    570
    Q1      Q1                         wind_medium          resource    1710     300000
    Q1      Q1                         solar                resource    1100     300000
    Q8      Q8a                        wind_high            resource    560
    V8      V8                         wind_offshore_fixed  resource    54996
    Q1      Q1             Wind                             land_use    6764.2
    Q1      Q1                         solar                land_use    16234.0

The geo_id, resource_zone, technology and resource_type columns are the row's
keys. They say which new entrant generators the limit applies to, and a blank key
matches any value. For example, the Q1 wind land use row has technology "Wind"
and a blank resource_type, so it caps wind_high and wind_medium builds in Q1
together. relaxation_penalty is the cost ($/MW) of building past the limit, and
a blank penalty makes the limit a hard one.

Pipeline:

1. Melt the resource limit columns into one row per REZ and resource_type, with
   limit_type "resource".
2. Attach each REZ's relaxation penalty to its resource rows, converting $M/MW to
   $/MW. IASR gives the offshore REZs no penalty, so their limits are hard.
3. Remove the penalty from wind_high rows, making them hard limits too. IASR gives
   one penalty per REZ, but the ISP only relaxes the wind_medium and solar limits.
4. Add the wind and solar land use limits for the scenario, with limit_type
   "land_use" and no penalty. Wind land use is keyed on technology "Wind", so it
   covers both wind resource types. Solar land use is keyed on resource_type
   "solar", so it covers both SAT and CST technologies.
5. Set geo_id from resource_zone. The two are the same except for Q8's sub-areas
   (Q8a, Q8b and Q8c), which share geo_id Q8 but keep their own limits.
6. Drop the limits that match no new entrant generator, since they would have no
   effect in the model. Most of these are blank or zero in IASR (e.g. the offshore
   columns for an onshore REZ) and are dropped quietly. Known mon-zero drops are logged,
   and any non-zero drop outside the known exceptions below raises an error.
7. Set the remaining blank limits to 0. A blank limit that survives step 6 still
   applies to at least one generator. With no penalty, none of that resource can
   be built. With a penalty, it can only be built by paying that penalty on every
   MW.
"""

import logging

import pandas as pd

from ispypsa.templater.mappings import (
    _HARD_LIMIT_RESOURCE_TYPES,
    _SUB_REZ_AREA_TO_REZ_ID,
)

# Literal v7.5 header names in initial_resource_limits.
_REZ_ID_COLUMN = "REZ ID"
_PENALTY_COLUMN = "REZ resource limit violation penalty factor ($M/MW)"

# Resource limit headers -> the resource_type each one limits.
_RESOURCE_LIMIT_COLUMNS = {
    "Wind generation total limits (MW)_High": "wind_high",
    "Wind generation total limits (MW)_Medium": "wind_medium",
    "Wind generation total limits (MW)_Offshore -fixed": "wind_offshore_fixed",
    "Wind generation total limits (MW)_Offshore -floating": "wind_offshore_floating",
    "Solar PV plus Solar thermal Limits (MW)_Solar": "solar",
}

# The land use headers each scenario reads. Accelerated Transition has its own pair.
_DEFAULT_LAND_USE_COLUMNS = {
    "wind": "Land use limits in MW_Wind",
    "solar": "Land use limits in MW_Solar",
}
_LAND_USE_COLUMNS_BY_SCENARIO = {
    "Step Change": _DEFAULT_LAND_USE_COLUMNS,
    "Slower Growth": _DEFAULT_LAND_USE_COLUMNS,
    "Accelerated Transition": {
        "wind": "Land use limits in MW Accelerated Transition scenario_Wind",
        "solar": "Land use limits in MW Accelerated Transition scenario_Solar",
    },
}

# The resource_limits columns that pick out which assets a row applies to. A blank
# key matches any value (see the resource_limits schema description).
_LIMIT_KEY_COLUMNS = ["geo_id", "resource_zone", "technology", "resource_type"]

# Expected column order per schema definition:
_EXPECTED_COLUMN_ORDER = [
    "geo_id",
    "resource_zone",
    "technology",
    "resource_type",
    "limit_type",
    "limit",
    "relaxation_penalty",
]

# Non-zero limits that IASR gives but no new entrant can count against: the
# distribution REZs have wind_medium limits, but only have wind_high and solar new
# entrant options. Labelled per the structure created in _describe_limits.
_KNOWN_UNMATCHED_NON_ZERO_LIMITS = {
    "DN1 wind_medium resource",
    "DN2 wind_medium resource",
    "DN3 wind_medium resource",
}


def _template_resource_limits(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    generators_new_entrant: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the resource_limits table from IASR's ``initial_resource_limits``.

    Melts the REZ resource limits (wind_high, wind_medium, offshore and solar) to
    one row per REZ and resource_type, attaches each REZ's relaxation penalty to
    those rows (except the hard-limited resource types, see
    ``_HARD_LIMIT_RESOURCE_TYPES``), and adds the wind and solar land use limits
    for ``scenario``. Land use rows never get a penalty. Rows matching no new
    entrant generator are dropped (see ``_drop_limits_matching_no_new_entrant``),
    then the remaining blank limits become 0.

    Args:
        iasr_tables: IASR tables; uses ``initial_resource_limits``.
        scenario: ISP scenario, which picks the land use columns (see
            ``_LAND_USE_COLUMNS_BY_SCENARIO``), e.g. "Accelerated Transition".
        generators_new_entrant: templated new entrant generators, used to drop
            limit rows that match no generator.
    """
    logging.info("Creating a template for resource and land-use limits")

    initial_resource_limits = iasr_tables["initial_resource_limits"]
    resource_limits = _melt_resource_limits(initial_resource_limits)
    resource_limits = _add_relaxation_penalty(resource_limits, initial_resource_limits)
    resource_limits = _remove_penalty_for_hard_limited_resources(resource_limits)

    land_use_columns = _LAND_USE_COLUMNS_BY_SCENARIO[scenario]
    wind_land_use = _extract_wind_land_use(
        initial_resource_limits, land_use_columns["wind"]
    )
    solar_land_use = _extract_solar_land_use(
        initial_resource_limits, land_use_columns["solar"]
    )
    all_limits = pd.concat(
        [resource_limits, wind_land_use, solar_land_use], axis=0, ignore_index=True
    )
    # add geo_id column
    all_limits["geo_id"] = all_limits["resource_zone"].replace(_SUB_REZ_AREA_TO_REZ_ID)

    all_limits = _drop_limits_matching_no_new_entrant(
        all_limits, generators_new_entrant
    )
    # set the remaining blank limits to 0
    all_limits["limit"] = all_limits["limit"].fillna(0.0)
    return all_limits[_EXPECTED_COLUMN_ORDER]


def _melt_resource_limits(limits: pd.DataFrame) -> pd.DataFrame:
    """Melts the REZ resource limit columns to one row per REZ and resource_type.

    Blank limits stay NaN here (some offshore columns are blank for onshore REZs,
    for example).

    I/O Example (headers abbreviated; see ``_RESOURCE_LIMIT_COLUMNS``):
        limits:
            REZ ID  ..._High  ..._Medium  ..._Offshore -fixed  ..._Offshore -floating  ..._Solar
            Q1      570       1710                                                     1100

        returns:
            resource_zone  resource_type           limit  limit_type
            Q1             wind_high               570    resource
            Q1             wind_medium             1710   resource
            Q1             wind_offshore_fixed     NaN    resource  # blank in IASR
            Q1             wind_offshore_floating  NaN    resource
            Q1             solar                   1100   resource
    """
    renames = {_REZ_ID_COLUMN: "resource_zone"} | _RESOURCE_LIMIT_COLUMNS
    resource_limits = limits[list(renames)].rename(columns=renames)
    melted = resource_limits.melt(
        id_vars="resource_zone", var_name="resource_type", value_name="limit"
    )
    return melted.assign(limit_type="resource")


def _add_relaxation_penalty(
    resource_limits: pd.DataFrame,
    limits: pd.DataFrame,
) -> pd.DataFrame:
    """Adds each REZ's ``relaxation_penalty`` to its resource limit rows, in $/MW.

    IASR gives one up-front penalty per REZ in $M/MW, which applies to all of that
    REZ's resource limits; it's converted to $/MW. A REZ with a blank penalty
    (the offshore REZs) gets NaN, i.e. hard limits.

    I/O Example:
        limits (relevant columns):
            REZ ID  REZ resource limit violation penalty factor ($M/MW)
            Q1      0.3
            V8                       # offshore REZ, no penalty

        resource_limits:
            resource_zone  resource_type        limit  limit_type
            Q1             wind_medium          1710   resource
            Q1             solar                1100   resource
            V8             wind_offshore_fixed  54996  resource

        returns:
            resource_zone  resource_type        limit  limit_type  relaxation_penalty
            Q1             wind_medium          1710   resource    300000.0  # 0.3 $M/MW
            Q1             solar                1100   resource    300000.0
            V8             wind_offshore_fixed  54996  resource    NaN
    """
    penalty_cols = {
        _REZ_ID_COLUMN: "resource_zone",
        _PENALTY_COLUMN: "relaxation_penalty",
    }
    penalty = limits[list(penalty_cols)].rename(columns=penalty_cols)
    penalty["relaxation_penalty"] *= 1e6  # convert $M/MW -> $/MW
    return resource_limits.merge(penalty, how="left", on="resource_zone")


def _remove_penalty_for_hard_limited_resources(limits: pd.DataFrame) -> pd.DataFrame:
    """Blanks ``relaxation_penalty`` on resource types in ``_HARD_LIMIT_RESOURCE_TYPES``.

    IASR's one penalty per REZ rows would make wind_high soft too. AEMO's PLEXOS model
    relaxes only wind_medium and solar, so wind_high is kept as a hard limit here:
    a deliberate departure from IASR as given.

    I/O Example:
        limits:
            resource_zone  resource_type  limit  relaxation_penalty
            Q1             wind_high      570    300000.0
            Q1             wind_medium    1710   300000.0

        returns:
            resource_zone  resource_type  limit  relaxation_penalty
            Q1             wind_high      570    NaN       # hard limit
            Q1             wind_medium    1710   300000.0
    """
    limits = limits.copy()
    gets_hard_limit = limits["resource_type"].isin(_HARD_LIMIT_RESOURCE_TYPES)
    limits.loc[gets_hard_limit, "relaxation_penalty"] = pd.NA
    return limits


def _extract_wind_land_use(limits: pd.DataFrame, column: str) -> pd.DataFrame:
    """Extracts the wind land use limit per REZ from ``column``.

    Wind land use caps all onshore wind together (wind_high and wind_medium), so
    the rows key on technology "Wind", which doesn't include offshore wind (which
    has it's own explicit technology options).

    I/O Example (column="Land use limits in MW_Wind"):
        limits (relevant columns):
            REZ ID  Land use limits in MW_Wind
            Q1      6764.2

        returns:
            resource_zone  limit   technology  limit_type
            Q1             6764.2  Wind        land_use
    """
    land_use = limits[[_REZ_ID_COLUMN, column]].rename(
        columns={_REZ_ID_COLUMN: "resource_zone", column: "limit"}
    )
    return land_use.assign(technology="Wind", limit_type="land_use")


def _extract_solar_land_use(limits: pd.DataFrame, column: str) -> pd.DataFrame:
    """Extracts the solar land use limit per REZ from ``column``.

    Solar land use caps every solar resource_type asset together (solar PV and
    solar thermal), so the rows key on resource_type "solar".

    I/O Example (column="Land use limits in MW_Solar"):
        limits (relevant columns):
            REZ ID  Land use limits in MW_Solar
            Q1      16234.0

        returns:
            resource_zone  limit    resource_type  limit_type
            Q1             16234.0  solar          land_use
    """
    land_use = limits[[_REZ_ID_COLUMN, column]].rename(
        columns={_REZ_ID_COLUMN: "resource_zone", column: "limit"}
    )
    return land_use.assign(resource_type="solar", limit_type="land_use")


def _drop_limits_matching_no_new_entrant(
    limits: pd.DataFrame, generators_new_entrant: pd.DataFrame
) -> pd.DataFrame:
    """Drops limit rows that no new entrant generator matches.

    Most dropped rows are blank or 0 in IASR (e.g. offshore resource rows for
    onshore REZs): they limit nothing and say there's nothing to build, so they're
    dropped without a log. A non-zero row with no match is IASR resource the model
    has no option to build. The ones in ``_KNOWN_UNMATCHED_NON_ZERO_LIMITS`` are
    logged at INFO, any other non-zero limits with no match raise.

    Raises:
        ValueError: if a non-zero limit outside ``_KNOWN_UNMATCHED_NON_ZERO_LIMITS``
            matches no generator.

    I/O Example:
        limits:
            geo_id  resource_zone  technology  resource_type  limit_type  limit
            Q1      Q1             Wind                       land_use    6764
            Q3      Q3                         wind_high      resource          # blank, but matched
            V8      V8                         solar          resource          # no V8 solar
            DN1     DN1                        wind_medium    resource    792   # known

        generators_new_entrant (key columns):
            geo_id  resource_zone  technology  resource_type
            Q1      Q1             Wind        wind_high
            Q3      Q3             Wind        wind_high
            DN1     DN1            Wind        wind_high

        returns:
            geo_id  resource_zone  technology  resource_type  limit_type  limit
            Q1      Q1             Wind                       land_use    6764
            Q3      Q3                         wind_high      resource          # kept blank

        logs (INFO):
            Non-zero resource limits matching no new entrant generator, dropped:
            ['DN1 wind_medium resource']
    """
    matched = limits.apply(_matches_any_unit, axis=1, units=generators_new_entrant)
    # NaN > 0 is False, so blank limits are also caught here under 'non-zero'
    non_zero_unmatched = limits[~matched & (limits["limit"] > 0)]
    _assert_unmatched_non_zero_limits_are_known(non_zero_unmatched)
    if not non_zero_unmatched.empty:
        logging.info(
            "Non-zero resource limits matching no new entrant generator, dropped: "
            f"{sorted(_describe_limits(non_zero_unmatched))}"
        )
    return limits[matched].reset_index(drop=True)


def _matches_any_unit(limit_row: pd.Series, units: pd.DataFrame) -> bool:
    """Whether any unit matches every key that the limit row fills in.

    Blank keys on the limit row are left out, so they essentially match any value
    that a unit has in the same key column. A unit that has a blank value in a key
    column never matches a non-blank key in the limit row.

    I/O Example:
        limit_row:
            geo_id  resource_zone  technology  resource_type
            Q1      Q1             Wind                        # resource_type blank

        units:
            geo_id  resource_zone  technology            resource_type
            Q1      Q1             Wind                  wind_high   # matches
            Q1      Q1             Large scale Solar PV  solar
            CNSW                   OCGT (small GT)

        returns:
            True
    """
    filled_keys = limit_row[_LIMIT_KEY_COLUMNS].dropna()
    return (units[filled_keys.index] == filled_keys).all(axis=1).any()


def _assert_unmatched_non_zero_limits_are_known(
    non_zero_unmatched: pd.DataFrame,
) -> None:
    """Raises if any row of ``non_zero_unmatched`` isn't in
    ``_KNOWN_UNMATCHED_NON_ZERO_LIMITS``."""
    labels = set(_describe_limits(non_zero_unmatched))
    unexpected = labels - _KNOWN_UNMATCHED_NON_ZERO_LIMITS
    if unexpected:
        raise ValueError(
            "Non-zero limits in 'initial_resource_limits' match no new entrant "
            f"generator: {sorted(unexpected)}"
        )


def _describe_limits(limits: pd.DataFrame) -> pd.Series:
    """Labels each limit row by its zone, what it limits, and its limit_type.

    I/O Example:
        limits:
            geo_id  resource_zone  technology  resource_type  limit_type
            DN1     DN1                        wind_medium    resource
            Q1      Q1             Wind                       land_use
            NQ                     CCGT                       land_use

        returns:
            "DN1 wind_medium resource"
            "Q1 Wind land_use"
            "NQ CCGT land_use"
    """
    location = limits["resource_zone"].fillna(limits["geo_id"])
    limited = limits["resource_type"].fillna(limits["technology"])
    return location + " " + limited + " " + limits["limit_type"]
