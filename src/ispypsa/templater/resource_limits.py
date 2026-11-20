import logging

import pandas as pd

from ispypsa.templater.mappings import _HARD_LIMIT_RESOURCE_TYPES

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


def _template_resource_limits(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    generators_new_entrant: pd.DataFrame,
    storage_new_entrant: pd.DataFrame,
) -> pd.DataFrame:
    """Templates the resource_limits table from IASR's ``initial_resource_limits``.

    Melts the REZ resource limits (wind_high, wind_medium, offshore and solar) to
    one row per REZ and resource_type, attaches each REZ's relaxation penalty to
    those rows (except the hard-limited resource types, see
    ``_HARD_LIMIT_RESOURCE_TYPES``), and adds the wind and solar land use limits
    for ``scenario``. Land use rows never get a penalty.

    Args:
        iasr_tables: IASR tables; uses ``initial_resource_limits``.
        scenario: ISP scenario, which picks the land use columns (see
            ``_LAND_USE_COLUMNS_BY_SCENARIO``), e.g. "Accelerated Transition".
        generators_new_entrant: templated new entrant generators, used to drop
            limit rows that match no generator.
        storage_new_entrant: templated new entrant storage, used likewise.
    """
    logging.info("Creating a template for resource and land-use limits")

    initial_resource_limits = iasr_tables["initial_resource_limits"]
    resource_limits = _melt_resource_limits(initial_resource_limits)
    resource_limits = _add_relaxation_penalty(initial_resource_limits, resource_limits)
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
    return pd.DataFrame()


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
    limits: pd.DataFrame,
    resource_limits: pd.DataFrame,
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
    return resource_limits.merge(penalty, how="outer", on="resource_zone")


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
