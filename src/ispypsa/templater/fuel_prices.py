# TODO: also add emissions_reduction templating to this file? It's related?

import pandas as pd

from ispypsa.templater.helpers import (
    _financial_year_string_to_end_year_int,
    _fuzzy_match_names,
    _looks_like_financial_year,
)
from ispypsa.templater.mappings import _SINGLE_REGION_ID

# notes to self - helper lists to have in one place
# TODO delete before pushing
_EXPECTED_COLUMNS = [
    "fuel_type",
    "power_station",
    "technology",
    "geo_id",
    # "fuel_price_mapping",     # maybe don't need this in the end?
    "year",
    "price",
]

_FUEL_PRICE_ID_COLS = ["fuel_type", "power_station", "technology", "geo_id"]

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

# this orchestrator is WIRED IN to create_template for DEBUGGING purposes


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
        generators_existing_planned,
        generators_new_entrant,
        sub_regional_geography,
        regional_granularity,
    )

    liquid_fuel_prices = _template_liquid_fuel_prices(
        iasr_tables["liquid_fuel_prices"],
        iasr_tables["gpg_secondary_fuel_prices"],
        scenario,
    )

    biomass_prices = _template_biomass_prices(
        iasr_tables["biomass_fuel_price"], scenario
    )

    hydrogen_prices = _template_hydrogen_prices(
        iasr_tables["hydrogen_prices"], scenario
    )

    biomethane_prices = _template_biomethane_prices(
        iasr_tables["biomethane_prices"], scenario
    )

    # add Wind/Solar/Hydro fuel_type w/ price = 0.0
    result = pd.concat([coal_prices, gas_prices], axis=0, ignore_index=True)

    return result[_EXPECTED_COLUMNS]


def _template_standardised_fuel_price_table(
    table: pd.DataFrame, scenario: str
) -> pd.DataFrame:
    # filter
    filtered = table[table["scenario"] == scenario]
    # select id and year columns
    id_cols = [c for c in table.columns if c in _FUEL_PRICE_ID_COLS]
    year_cols = [c for c in table.columns if _looks_like_financial_year(c)]
    # reshape to long
    long = filtered.melt(
        id_vars=id_cols,
        value_vars=year_cols,
        var_name="year",
        value_name="price",
    )
    # apply FY -> int processing
    long["year"] = long["year"].apply(_financial_year_string_to_end_year_int)
    # enforce numeric price
    long["price"] = pd.to_numeric(long["price"], errors="raise")
    return long


def _template_coal_prices(
    coal_prices: pd.DataFrame,
    scenario: str,
    generators_existing_planned: pd.DataFrame,
) -> pd.DataFrame:

    coal_generators = generators_existing_planned[
        generators_existing_planned["fuel_type"].str.contains("coal", case=False)
    ].drop_duplicates(subset=["power_station", "fuel_type"])
    power_station_to_fuel_type = dict(
        zip(coal_generators["power_station"], coal_generators["fuel_type"])
    )

    # clean up table: rename key columns and align power_station names with canonical
    _COAL_PRICE_RENAMES = {"Generator": "power_station", "Scenario": "scenario"}
    coal_prices = coal_prices.rename(columns=_COAL_PRICE_RENAMES)
    coal_prices["power_station"] = _fuzzy_match_names(
        coal_prices["power_station"],
        coal_generators["power_station"],
        "templating coal fuel prices for existing power stations",
    )
    coal_prices["fuel_type"] = coal_prices["power_station"].map(
        power_station_to_fuel_type
    )

    return _template_standardised_fuel_price_table(coal_prices, scenario)


def _template_gas_prices(
    gas_existing: pd.DataFrame,
    gas_new_entrant: pd.DataFrame,
    scenario: str,
    generators_existing_planned: pd.DataFrame,
    generators_new_entrant: pd.DataFrame,  # this could be from IASR table, not templated?
    sub_regional_geography: pd.DataFrame,
    regional_granularity: str,
) -> pd.DataFrame:

    # 1. Existing/planned generators
    existing_gas_gens = generators_existing_planned[
        generators_existing_planned["fuel_type"] == "Gas"
    ].drop_duplicates(subset=["power_station"])

    _GAS_EXISTING_RENAMES = {
        "Generator": "power_station",
        "Gas price scenario": "scenario",
    }
    gas_existing = gas_existing.rename(columns=_GAS_EXISTING_RENAMES)
    gas_existing["power_station"] = _fuzzy_match_names(
        gas_existing["power_station"],
        existing_gas_gens["power_station"],
        "templating gas fuel prices for existing power stations",
    )
    # STILL NEED TO MANUALLY HANDLE THE KOGAN GAS/BRIGALOW GT THING!!
    gas_existing = _template_standardised_fuel_price_table(gas_existing, scenario)

    # 2. New entrant generators
    new_entrant_gas_technologies = generators_new_entrant[
        generators_new_entrant["fuel_type"] == "Gas"
    ].drop_duplicates(subset="technology")
    new_entrant_gas_technologies["gt_type"] = new_entrant_gas_technologies[
        "technology"
    ].str.extract(r"^([OC]CGT).*")

    _GAS_NEW_ENTRANT_RENAMES = {
        "Gas price scenario": "scenario",
    }
    gas_new_entrant = gas_new_entrant.rename(columns=_GAS_NEW_ENTRANT_RENAMES)
    key_col_pattern = r"^(?P<geo_id>[A-Z]+) new (?P<gt_type>[OC]CGT)$"
    gas_new_entrant = gas_new_entrant.assign(
        **gas_new_entrant["New generating stations"].str.extract(
            key_col_pattern, expand=True
        )
    )
    gas_new_entrant = gas_new_entrant.merge(
        new_entrant_gas_technologies[["gt_type", "technology"]],
        how="left",
        on="gt_type",
    )
    gas_new_entrant = _template_standardised_fuel_price_table(gas_new_entrant, scenario)
    if regional_granularity == "nem_regions":
        pass
    elif regional_granularity == "single_region":
        gas_new_entrant = gas_new_entrant.groupby(["technology", "year"])["price"].mean(
            skipna=True
        )
        gas_new_entrant["geo_id"] = _SINGLE_REGION_ID
    elif regional_granularity == "sub_regions":
        gas_new_entrant = gas_new_entrant.rename(columns={"geo_id": "region_id"})
        regions_to_subregions = sub_regional_geography[
            sub_regional_geography["geo_type"] == "subregion"
        ][["region_id", "geo_id"]]
        gas_new_entrant = gas_new_entrant.merge(
            regions_to_subregions, how="left", on="region_id"
        )
    else:
        raise ValueError("invalid regional_granularity")

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
    gas_prices = pd.concat([gas_existing, gas_new_entrant], axis=0, ignore_index=True)
    gas_prices["fuel_type"] = "Gas"

    expected_cols = [c for c in _EXPECTED_COLUMNS if c in gas_prices.columns]
    return gas_prices[expected_cols]


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


def _template_biomass_prices(
    biomass: pd.DataFrame,
    scenario: str,
) -> pd.DataFrame:

    # filter df by scenario
    # melt to long-form
    # convert 'year' column to int (from FY format)
    # enforce numeric (float) on price column
    # set fuel_type = 'biomass' (whatever the match in generators is)

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
