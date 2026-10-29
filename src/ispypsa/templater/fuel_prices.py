import logging

import pandas as pd

from ispypsa.templater.helpers import (
    _filter_table_by_isp_scenario,
    _financial_year_string_to_end_year_int,
    _looks_like_financial_year,
    _map_geo_id_to_granularity,
    _set_geo_id,
)
from ispypsa.templater.mappings import _SUMMARY_COLUMN_RENAMES

_FUEL_PRICE_COLUMNS = [
    "fuel_type",
    "power_station",
    "technology",
    "geo_id",
    "year",
    "price",
]

_NEW_ENTRANT_KEY_COLS = ["fuel_type", "technology", "geo_id"]
_STATION_KEY_COLS = _NEW_ENTRANT_KEY_COLS + ["power_station"]

# IASR fuel price tables read by this module
_FUEL_PRICE_TABLES = [
    "coal_fuel_price",
    "gas_prices_existing_generators",
    "gas_prices_new_entrants",
    "liquid_fuel_prices",
    "gpg_secondary_fuel_prices",
    "biomass_fuel_price",
    "hydrogen_prices",
    "biomethane_prices",
]

# Raw IASR fuel price table headers → names used in this module. Each table has one
# scenario column, plus one key column holding either a generator's fuel cost mapping
# (station and new entrant tables) or the fuel type itself (biomass, hydrogen,
# biomethane).
_PRICE_TABLE_COLUMN_RENAMES = {
    "Scenario": "scenario",
    "Gas price scenario": "scenario",
    "Hydrogen price scenario": "scenario",
    "Biomethane price scenario": "scenario",
    "Generator": "fuel_cost_mapping",
    "New generating stations": "fuel_cost_mapping",
    "Biomass price": "fuel_type",
    "Hydrogen price": "fuel_type",
    "Biomethane price": "fuel_type",
}

# Brigalow GT primary fuel (gas) is priced with the 'QLD new OCGT' new-entrant key,
# and has no secondary fuel price row in the gpg_secondary_fuel_prices table. It
# gets excluded and logged (see `_exclude_gpg_with_no_2nd_fuel_price`) when templating
# secondary liquid fuels.
_GPG_WITH_NO_2ND_FUEL_PRICE = ["Brigalow GT"]

# Three options are given in v7.5 workbook for biomethane prices: 'Biomethane (Waste)',
# 'Biomethane (Landfill gas)' and 'Biomethane (Crop residue)'. The corresponding
# note in the workbook says: "Note: Fuel price calculation for gas powered generators
# uses Biomethane (Waste) price".
_BIOMETHANE_PRICE_TYPE = "Biomethane (Waste)"

_ZERO_COST_FUEL_TYPES = ["Solar", "Wind", "Water"]


def _template_fuel_prices(
    iasr_tables: dict[str, pd.DataFrame],
    scenario: str,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    logging.info(
        "Creating a template for fuel prices for existing and new entrant generators"
    )

    summary_existing = _prepare_existing_summary(
        iasr_tables["existing_committed_anticipated_additional_generator_summary"],
        regional_granularity,
        sub_regional_geography,
    )

    # prep price tables all together:
    price_tables = _prepare_price_tables(iasr_tables, scenario)

    # existing stations
    coal_prices = _template_station_keyed_prices(
        price_tables["coal_fuel_price"],
        summary_existing,
        ["Black Coal", "Brown Coal"],
        "coal_fuel_price",
    )
    gas_prices = _template_station_keyed_prices(
        _combine_gas_price_tables(price_tables),
        summary_existing,
        ["Gas"],
        "gas_prices_existing_generators / gas_prices_new_entrants",
    )
    liquid_fuel_prices = _template_station_keyed_prices(
        price_tables["liquid_fuel_prices"],
        summary_existing,
        ["Liquid Fuel"],
        "liquid_fuel_prices",
    )
    secondary_liquid_fuel_prices = _template_secondary_liquid_fuel_prices(
        price_tables["gpg_secondary_fuel_prices"], summary_existing
    )

    # new entrants
    new_entrant_gas_prices = _template_new_entrant_gas_prices(
        price_tables["gas_prices_new_entrants"],
        iasr_tables["new_entrants_summary"],
        regional_granularity,
        sub_regional_geography,
    )

    # fuel-wide prices
    biomass_prices = _melt_fuel_prices_to_long(
        price_tables["biomass_fuel_price"],
        ["fuel_type"],
    )
    hydrogen_prices = _melt_fuel_prices_to_long(
        price_tables["hydrogen_prices"],
        ["fuel_type"],
    )
    biomethane_prices = _template_biomethane_prices(price_tables["biomethane_prices"])

    result = pd.concat(
        [
            coal_prices,
            gas_prices,
            liquid_fuel_prices,
            secondary_liquid_fuel_prices,
            new_entrant_gas_prices,
            biomass_prices,
            hydrogen_prices,
            biomethane_prices,
            _zero_cost_rows(),
        ],
        axis=0,
        ignore_index=True,
    )
    return result[_FUEL_PRICE_COLUMNS]


# --- input prep ---


def _prepare_existing_summary(
    summary_existing: pd.DataFrame,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    # existing summary is shared by four sub-templates, so running geo_id prep once.
    # NOTE: relies on fuel prices only existing for thermal gens that aren't defined
    # in REZs, atm, so that IASR table replacements don't need to be applied first.
    # See Open-ISP/ISPyPSA#152 for planned refactor.
    summary_existing = _set_geo_id(summary_existing)
    summary_existing["geo_id"] = _map_geo_id_to_granularity(
        summary_existing["geo_id"], regional_granularity, sub_regional_geography
    )
    return summary_existing


def _prepare_price_tables(
    iasr_tables: dict[str, pd.DataFrame], scenario: str
) -> dict[str, pd.DataFrame]:
    return {
        name: _filter_table_by_isp_scenario(
            iasr_tables[name].rename(columns=_PRICE_TABLE_COLUMN_RENAMES),
            scenario,
            "scenario",
            name,
        )
        for name in _FUEL_PRICE_TABLES
    }


# --- existing station prices ---


def _template_station_keyed_prices(
    prices: pd.DataFrame,
    summary_existing: pd.DataFrame,
    fuel_types: list[str],
    price_table_name: str,
) -> pd.DataFrame:
    generators = _select_generator_keys(summary_existing, fuel_types)
    merged = _merge_prices_onto_generators(prices, generators, price_table_name)
    return _melt_fuel_prices_to_long(merged, _STATION_KEY_COLS)


def _combine_gas_price_tables(price_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Brigalow GT (in summary_existing) is priced off the new-entrant 'QLD new OCGT' key
    return pd.concat(
        [
            price_tables["gas_prices_existing_generators"],
            price_tables["gas_prices_new_entrants"],
        ],
    )


def _template_secondary_liquid_fuel_prices(
    prices: pd.DataFrame,
    summary_existing: pd.DataFrame,
) -> pd.DataFrame:
    # NOTE: this isn't actually used yet in the demo setup, but it's easy to do here
    # and could be easy to use to tweak fuel prices in input tables so leaving for now.
    generators = _select_generator_keys(summary_existing, ["Gas"])
    generators = _exclude_gpg_with_no_2nd_fuel_price(generators)

    # strip suffix so keys match primary mapping ('Bairnsdale_2nd_Fuel'->'Bairnsdale')
    prices = prices.assign(
        fuel_cost_mapping=prices["fuel_cost_mapping"].str.removesuffix("_2nd_Fuel")
    )
    merged = _merge_prices_onto_generators(
        prices, generators, "gpg_secondary_fuel_prices"
    )
    # manually set fuel_type -> otherwise would be 'Gas' from the merge still.
    merged = merged.assign(fuel_type="Liquid Fuel")
    return _melt_fuel_prices_to_long(
        merged,
        _STATION_KEY_COLS,
    )


def _exclude_gpg_with_no_2nd_fuel_price(generators: pd.DataFrame) -> pd.DataFrame:
    generators = generators[
        ~generators["power_station"].isin(_GPG_WITH_NO_2ND_FUEL_PRICE)
    ]
    # log for full record - currently only Brigalow GT
    logging.info(
        f"KNOWN EXCLUSION: power station(s) {sorted(_GPG_WITH_NO_2ND_FUEL_PRICE)} "
        "excluded while templating secondary fuel prices for named GPG."
    )
    return generators


# --- new entrant prices ---


def _template_new_entrant_gas_prices(
    prices: pd.DataFrame,
    summary_new_entrant: pd.DataFrame,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    generators = (
        # set geo_id before merge+melt, then apply granularity collapse
        _select_generator_keys(_set_geo_id(summary_new_entrant), ["Gas"])
        .drop(columns="power_station")
        .drop_duplicates()
    )
    merged = _merge_prices_onto_generators(
        prices,
        generators,
        "gas_prices_new_entrants",
    )
    long = _melt_fuel_prices_to_long(
        merged,
        _NEW_ENTRANT_KEY_COLS,
    )
    return _collapse_new_entrant_gas_prices_to_granularity(
        long, regional_granularity, sub_regional_geography
    )


def _collapse_new_entrant_gas_prices_to_granularity(
    prices: pd.DataFrame,
    regional_granularity: str,
    sub_regional_geography: pd.DataFrame,
) -> pd.DataFrame:
    prices = prices.copy()
    prices["geo_id"] = _map_geo_id_to_granularity(
        prices["geo_id"], regional_granularity, sub_regional_geography
    )
    return (
        prices.groupby(_NEW_ENTRANT_KEY_COLS + ["year"])["price"].mean().reset_index()
    )


# --- fuel-wide prices ---


def _template_biomethane_prices(
    prices: pd.DataFrame,
) -> pd.DataFrame:
    # Filter for the specific 'Biomethane price'-type specified in IASR note:
    prices = prices[prices["fuel_type"] == _BIOMETHANE_PRICE_TYPE]
    prices = prices.assign(fuel_type="Biomethane")
    return _melt_fuel_prices_to_long(prices, ["fuel_type"])


def _zero_cost_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "fuel_type": _ZERO_COST_FUEL_TYPES,
            "price": 0.0,
        },
    )


# --- shared steps ---


def _select_generator_keys(
    summary: pd.DataFrame, fuel_types: list[str]
) -> pd.DataFrame:
    renamed = summary.rename(columns=_SUMMARY_COLUMN_RENAMES)
    filtered = renamed[renamed["fuel_type"].isin(fuel_types)]
    # Gas stations listed under two technologies keep a row for each, so every
    # key column stays filled for wildcard matching.
    return filtered[_STATION_KEY_COLS + ["fuel_cost_mapping"]].drop_duplicates()


def _merge_prices_onto_generators(
    prices: pd.DataFrame,
    generators: pd.DataFrame,
    price_table_name: str,
) -> pd.DataFrame:
    # raises if any generator has no price; unused price rows are ignored
    merged = generators.merge(
        prices,
        how="left",
        on="fuel_cost_mapping",
        indicator=True,
    )
    # TODO revisit logging unused rows; sticking point is gas w/ combined ecaa/new entrants
    _raise_on_unpriced_generators(
        merged,
        f"Generator fuel cost mappings with no price in {price_table_name}",
    )
    return merged.drop(columns="_merge")


def _raise_on_unpriced_generators(merged: pd.DataFrame, message: str) -> None:
    unmatched = sorted(
        merged.loc[merged["_merge"] == "left_only", "fuel_cost_mapping"].unique()
    )
    if unmatched:
        raise ValueError(f"{message}: {unmatched}")


def _melt_fuel_prices_to_long(table: pd.DataFrame, id_cols: list[str]) -> pd.DataFrame:
    year_cols = [c for c in table.columns if _looks_like_financial_year(c)]
    long = table.melt(
        id_vars=id_cols,
        value_vars=year_cols,
        var_name="year",
        value_name="price",
    )
    long["year"] = long["year"].map(_financial_year_string_to_end_year_int)
    long["price"] = pd.to_numeric(long["price"])
    return long
