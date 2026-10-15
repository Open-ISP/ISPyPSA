# TODO: also add emissions_reduction templating to this file? It's related?

import pandas as pd

# notes to self - helper lists to have in one place
# TODO delete before pushing
_EXPECTED_COLUMNS = [
    "fuel_type",
    "technology",
    "geo_id",
    # "fuel_price_mapping",     # maybe don't need this in the end?
    "year",
    "price",
]

_SOURCE_TABLES = [
    "coal_fuel_price",
    "biomass_fuel_price",
    "gas_prices_existing_generators",
    "gas_prices_new_entrants",
    "liquid_fuel_prices",
    "gpg_secondary_fuel_prices",  # maybe
    "hydrogen_prices",
    "biomethane_prices",
]
# don't need consultant's scenario mapping table in the end - scenarios listed in
# fuel price tables match allowed scenario strings already


def _template_fuel_prices(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
    generators_new_entrant: pd.DataFrame,
    generators_existing_planned: pd.DataFrame,
) -> pd.DataFrame:

    coal_prices = _template_coal_prices(
        iasr_tables["coal_fuel_price"], scenario, generators_existing_planned
    )

    gas_prices = _template_gas_prices(
        iasr_tables["gas_prices_existing_generators"],
        iasr_tables["gas_prices_new_entrants"],
        scenario,
        generators_new_entrant,
        sub_regional_geography,
        regional_granularity,
    )

    liquid_fuel_prices = _template_liquid_fuel_prices(
        iasr_tables["liquid_fuel_prices"],
        iasr_tables["gpg_secondary_fuel_prices"],
        scenario,
    )

    hydrogen_prices = _template_hydrogen_prices(
        iasr_tables["hydrogen_prices"], scenario
    )

    biomethane_prices = _template_biomethane_prices(
        iasr_tables["biomethane_prices"], scenario
    )

    # add Wind/Solar/Hydro fuel_type w/ price = 0.0

    return pd.DataFrame()


def _template_coal_prices(
    coal: pd.DataFrame,
    scenario: str,
    generators_existing_planned: pd.DataFrame,  # this could be from IASR table, not templated?
) -> pd.DataFrame:

    # filter df by scenario
    # melt to long-form
    # convert 'year' column to int (from FY format)
    # enforce numeric (float) on price column
    # set fuel_type and geo_id: by merge with generators_existing_planned on
    # power_station (name match first w/ fuzzy - canonicalise-vibe)

    # NOTE: don't add 'technology' column (revisit?) because Condamine has 2 options
    # for the same power_station. Not sure yet whether this works with wild card
    # handling...

    return pd.DataFrame()


def _template_gas_prices(
    gas_existing: pd.DataFrame,
    gas_new_entrant: pd.DataFrame,
    scenario: str,
    generators_new_entrant: pd.DataFrame,  # this could be from IASR table, not templated?
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
) -> pd.DataFrame:

    # 1. Existing/planned generators
    # filter df by scenario
    # melt to long-form
    # convert 'year' column to int (from FY format)
    # enforce numeric (float) on price column
    # set fuel_type == 'Gas'

    # 2. New entrant generators
    # filter by scenario
    # (
    # pull apart nem_region and technology(ish) from name column (- potentially could
    # map this from raw iasr new_entrants_summary table? for Gas, match on fuel_cost_mapping?)
    # explode to all gas technologies in new entrants (hard code or infer?) (?)
    # )
    # handle regional granularity variations: either explode to subregion or avg to NEM
    # melt to long form
    # convert 'year' column to int
    # enforce numeric (float) on price column
    # set fuel_type = 'Gas'

    # concat([1,2])

    return pd.DataFrame()


def _template_liquid_fuel_prices(
    liquid_fuel: pd.DataFrame,
    secondary_fuel: pd.DataFrame,
    scenario: str,
) -> pd.DataFrame:

    # 1. Liquid fuel
    # filter df by scenario
    # melt to long-form
    # convert 'year' column to int (from FY format)
    # enforce numeric (float) on price column
    # set fuel_type = 'liquid fuel' (whatever the match in generators is)

    # 2. Secondary fuel - liquid fuel for gas generators
    # filter df by scenario
    # clean names (remove 'Secondary')
    # melt to long-form
    # convert 'year' column to int (from FY format)
    # enforce numeric (float) on price column
    # set fuel_type = 'liquid fuel' (whatever the match in generators is)

    return pd.DataFrame()


def _template_hydrogen_prices(
    hydrogen: pd.DataFrame,
    scenario: str,
) -> pd.DataFrame:

    # filter df by scenario
    # melt to long-form
    # convert 'year' column to int (from FY format)
    # enforce numeric (float) on price column
    # set fuel_type = 'hydrogen' (whatever the match in generators is)

    return pd.DataFrame()


def _template_biomethane_prices(
    biomethane: pd.DataFrame,
    scenario: str,
) -> pd.DataFrame:

    # filter df by scenario
    # melt to long-form
    # convert 'year' column to int (from FY format)
    # enforce numeric (float) on price column
    # set fuel_type = 'biomethane' (whatever the match in generators is)

    return pd.DataFrame()
