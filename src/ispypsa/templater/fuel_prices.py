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
    """Templates costs_fuel_prices from the IASR fuel price tables and generator summaries.

    Turns the eight IASR fuel price tables, together with the existing (ECAA) and
    new entrant generator summaries, into a single long table of fuel prices by year.

    There are two input price table shapes, broadly:
    - Those keyed by a generator's fuel cost mapping, where each row of a generator
      summary carries a 'Fuel cost mapping' naming the price row it is matched to.
      Several generators can share one mapping, and a mapping can be a station name
      or a regional new entrant key.
    - Others (biomass, hydrogen and biomethane tables) key each price on the fuel
      itself, and aren't tied to specific generators.

    The output has one row per fuel type, generator key and year. A blank key cell is a
    wildcard that matches every generator (or every year) that no more specific row
    covers, so each kind of price fills in only the keys it is specific to.

    Pipeline:
    1. Prepare the inputs: price tables get a flat header rename and filtered to
     scenario; ECAA summary gets geo_id mapped to regional granularity.
    2. Template station-keyed fuel prices - coal, gas and liquid fuel - merge
     price rows onto corresponding fuel_cost_mapping values in ECAA summary. The
     merge raises if any station has no price.
    3. Template secondary (liquid) fuel for the existing gas stations.
    4. Template the new entrant gas options: merge price rows onto fuel_cost_mapping
     values in new entrants summary, and take the mean across sub-regions to collapse to
     the specified regional granularity.
    5. Template fuel-wide prices (biomass, hydrogen and biomethane) directly, keyed on
     fuel_type.
    6. Add the zero-cost fuels: one row per fuel with a price of 0.0 and every key
     column blank.

    Args:
        iasr_tables: IASR tables; uses the eight in _FUEL_PRICE_TABLES, plus
            existing_committed_anticipated_additional_generator_summary and
            new_entrants_summary.
        scenario: ISP scenario that every price table is filtered to, e.g.
            "Step Change".
        regional_granularity: "sub_regions", "nem_regions", or "single_region".
        sub_regional_geography: network_geography templated at "sub_regions"
            granularity; columns used: 'geo_id', 'geo_type', 'region_id'.

    I/O Example:
        iasr_tables (abbreviated):
            existing_committed_anticipated_additional_generator_summary:
                IASR ID  Power Station  Technology Type     Sub-region  Fuel type   Fuel cost mapping   ...
                BW01     Bayswater      Steam Sub Critical  CNSW        Black Coal  Bayswater
                BW02     Bayswater      Steam Sub Critical  CNSW        Black Coal  Bayswater
                TALWA1   Tallawarra     CCGT                SNW         Gas         Tallawarra

            new_entrants_summary:
                IASR ID         Power Station  Technology Type     Sub-region  Fuel type   Fuel cost mapping   ...
                QLD new OCGT    QLD new OCGT   OCGT (small GT)     NQ          Gas         QLD new OCGT
                QLD new OCGT    QLD new OCGT   OCGT (small GT)     SQ          Gas         QLD new OCGT
                NSW new OCGT    NSW new OCGT   OCGT (small GT)     CNSW        Gas         NSW new OCGT

            coal_fuel_price:
                Generator  Scenario     2025-26  2026-27
                Bayswater  Step Change  4.12     3.95

            gas_prices_existing_generators:
                Generator       Gas price scenario  2025-26
                Tallawarra      Step Change         14.50

            gas_prices_new_entrants:
                New generating stations  Gas price scenario  2025-26
                QLD new OCGT             Step Change         15.50
                NSW new OCGT             Step Change         14.00

            biomethane_prices:
                Biomethane price           Biomethane price scenario    2025-26
                Biomethane (Landfill gas)  Step Change                  13.31
                Biomethane (Waste)         Step Change                  25.57

        scenario = "Step Change"

        sub_regional_geography:
            geo_id  geo_type   region_id
            CNSW    subregion  NSW
            SNW     subregion  NSW
            NQ      subregion  QLD

        regional_granularity = "sub_regions" returns:
            fuel_type   power_station  technology          geo_id  year  price
            Black Coal  Bayswater      Steam Sub Critical  CNSW    2026  4.12   # existing stations
            Gas         Tallawarra     CCGT                SNW     2026  14.50
            Gas                        OCGT (small GT)     NQ      2026  15.50  # new entrants
            Gas                        OCGT (small GT)     SQ      2026  15.50
            Gas                        OCGT (small GT)     CNSW    2026  14.00
            Biomethane                                             2026  25.57  # fuel-wide
            Wind                                                         0.0    # zero-cost:
            Solar                                                        0.0    # - applies all years
            Water                                                        0.0    #   and all geo_id/tech

        regional_granularity = "nem_regions" returns:
            fuel_type   power_station  technology          geo_id  year  price
            Black Coal  Bayswater      Steam Sub Critical  NSW     2026  4.12   # existing: no price change,
            Gas         Tallawarra     CCGT                NSW     2026  14.50  # geo_id CNSW/SNW -> NSW
            Gas                        OCGT (small GT)     QLD     2026  15.50  # new entrants: same
            Gas                        OCGT (small GT)     NSW     2026  14.00  # geo_id NQ/SQ -> QLD, CNSW -> NSW
            ...         ...            ...                 ...     ...   ...
            # fuel-wide and zero-cost rows are the same as sub_regions output

        regional_granularity = "single_region" returns:
            fuel_type   power_station  technology          geo_id  year  price
            Black Coal  Bayswater      Steam Sub Critical  NEM     2026  4.12   # existing: no price change
            Gas         Tallawarra     CCGT                NEM     2026  14.50  # geo_id -> NEM
            Gas                        OCGT (small GT)     NEM     2026  14.75  # mean(14.00, 15.50), geo_id -> NEM
            ...         ...            ...                 ...     ...   ...
            # fuel-wide and zero-cost rows are the same as sub_regions output

    """
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
    """Adds a geo_id to the ECAA summary, mapped to ``regional_granularity``.

    The existing generator summary table is shared by four sub-templates for fuel
    prices that are keyed by named stations, all requiring geo_id column, so it
    gets added once. Subsequent templating steps then share the one summary_existing
    table.

    I/O Example (regional_granularity="nem_regions"; other columns pass through):
        summary_existing:
            Power Station  REZ ID          Sub-region   ...
            Bayswater      Not Applicable  CNSW

        returns:
            Power Station  REZ ID          Sub-region  geo_id   ...
            Bayswater      Not Applicable  CNSW        NSW
    """
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
    """Renames the headers of each IASR fuel price table and filters it to ``scenario``.

    The eight tables name the same column differently: one flat rename
    (_PRICE_TABLE_COLUMN_RENAMES) covers every variant. Then each table is filtered
    to the specified scenario here (see helpers._filter_table_by_isp_scenario).
    Every table is filtered to the scenario here, once, so a table that two steps
    read (gas_prices_new_entrants feeds both existing and new entrant gas) isn't
    filtered twice.

    I/O Example (scenario="Step Change"; two of the eight tables shown):
        iasr_tables:
            coal_fuel_price:
                Generator  Scenario       2025-26
                Bayswater  Step Change    4.12
                Bayswater  Slower Growth  4.30

            gas_prices_new_entrants:
                New generating stations  Gas price scenario  2025-26
                QLD new OCGT             Step Change         14.50
                QLD new OCGT             Slower Growth       15.20

        returns:
            coal_fuel_price:
                fuel_cost_mapping  2025-26
                Bayswater          4.12

            gas_prices_new_entrants:
                fuel_cost_mapping  2025-26
                QLD new OCGT       14.50
    """
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
    """Prices each ``fuel_types`` station in ``summary_existing`` from ``prices``, as long rows.

    Raises if any station has no price row (see _merge_prices_onto_generators).

    I/O Example (fuel_types=["Black Coal"]):
        prices:
            fuel_cost_mapping  2025-26  2026-27
            Bayswater          4.12     3.95

        summary_existing:
            Power Station  Technology Type     geo_id  Fuel type   Fuel cost mapping
            Bayswater      Steam Sub Critical  CNSW    Black Coal  Bayswater

        returns:
            fuel_type   technology          geo_id  power_station  year  price
            Black Coal  Steam Sub Critical  CNSW    Bayswater      2026  4.12
            Black Coal  Steam Sub Critical  CNSW    Bayswater      2027  3.95
    """
    generators = _select_generator_keys(summary_existing, fuel_types)
    merged = _merge_prices_onto_generators(prices, generators, price_table_name)
    return _melt_fuel_prices_to_long(merged, _STATION_KEY_COLS)


def _combine_gas_price_tables(price_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Stacks the existing and new entrant gas price tables into one."""
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
    """Prices the secondary liquid fuel of each existing gas station, as long rows.

    The secondary fuel table keys each station as '<station>_2nd_Fuel'. The suffix
    is stripped so the keys match the stations' primary fuel cost mapping, and the
    merged rows are relabelled fuel_type "Liquid Fuel". Stations known to have no
    secondary fuel price are excluded first (see _exclude_gpg_with_no_2nd_fuel_price).

    I/O Example:
        prices:
            fuel_cost_mapping    2025-26    2026-27
            Tallawarra_2nd_Fuel  31.00      32.50

        summary_existing:
            Power Station  Technology Type  geo_id  Fuel type  Fuel cost mapping
            Tallawarra     CCGT             SNW     Gas        Tallawarra
            Brigalow GT    OCGT (large GT)  SQ      Gas        QLD new OCGT  # excluded

        returns:
            fuel_type    technology  geo_id  power_station  year  price
            Liquid Fuel  CCGT        SNW     Tallawarra     2026  31.00
            Liquid Fuel  CCGT        SNW     Tallawarra     2027  32.50
    """
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
    """Drops the stations in _GPG_WITH_NO_2ND_FUEL_PRICE and logs the exclusion."""
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
    """Prices each new entrant gas option from ``prices``, collapsed to ``regional_granularity``.

    IASR prices new entrant gas per region and technology class ('NSW new OCGT'
    covers both OCGT sizes), not per option. The summary's power_station holds
    option names, so it's dropped, and options left sharing a technology, geo_id
    and fuel_cost_mapping (the WOO options, which sit in CNSW) are de-duplicated.
    The rest are priced per sub-region, then collapsed to the regional granularity
    (see _collapse_new_entrant_gas_prices_to_granularity).

    I/O Example:
        Inputs:
            prices:
                fuel_cost_mapping  2025-26
                NSW new OCGT       19.00
                QLD new OCGT       16.00
                QLD new CCGT       13.00

            summary_new_entrant (abbr.):
                Power Station       Technology Type  Sub-region  Fuel type  Fuel cost mapping
                CNSW OCGT Small     OCGT (small GT)  CNSW        Gas        NSW new OCGT
                WOO OCGT Small      OCGT (small GT)  CNSW        Gas        NSW new OCGT  # duplicate - dropped
                SNW OCGT Small      OCGT (small GT)  SNW         Gas        NSW new OCGT
                NQ OCGT Small       OCGT (small GT)  NQ          Gas        QLD new OCGT
                NQ CCGT             CCGT             NQ          Gas        QLD new CCGT
                NQ CCGT with CCS    CCGT with CCS    NQ          Gas        QLD new CCGT  # different tech - same mapping

            sub_regional_geography:
                geo_id  geo_type   region_id
                CNSW    subregion  NSW
                SNW     subregion  NSW
                NQ      subregion  QLD

        regional_granularity="sub_regions" returns:
            fuel_type  technology       geo_id  year  price
            Gas        OCGT (small GT)  CNSW    2026  19.00
            Gas        OCGT (small GT)  SNW     2026  19.00
            Gas        OCGT (small GT)  NQ      2026  16.00
            Gas        CCGT             NQ      2026  13.00     # different price for OCGT vs CCGT
            Gas        CCGT with CCS    NQ      2026  13.00     # same price for all CCGT-types

        regional_granularity="nem_regions" returns:
            fuel_type  technology       geo_id  year  price
            Gas        OCGT (small GT)  NSW     2026  19.00     # mean(19.00, 19.00); (CNSW, SNW) -> NSW
            Gas        OCGT (small GT)  QLD     2026  16.00     # mean(16.00); NQ -> QLD
            Gas        CCGT             QLD     2026  13.00     # mean(13.00); NQ -> QLD
            Gas        CCGT with CCS    QLD     2026  13.00     # mean(13.00); NQ -> QLD

        regional_granularity="single_region" returns:
            fuel_type  technology       geo_id  year  price
            Gas        OCGT (small GT)  NEM     2026  18.00     # mean(19.00, 19.00, 16.00)
            Gas        CCGT             NEM     2026  13.00     # mean(13.00)
            Gas        CCGT with CCS    NEM     2026  13.00     # mean(13.00)

    """
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
    """Maps each price's geo_id to ``regional_granularity`` and averages rows that
    share a fuel_type, technology, geo_id and year.

    I/O Example:
        Inputs:
            prices:
                fuel_type  technology       geo_id  year  price
                Gas        OCGT (small GT)  CNSW    2026  16.0
                Gas        OCGT (small GT)  SQ      2026  13.0

            sub_regional_geography:
                geo_id  geo_type   region_id
                CNSW    subregion  NSW
                SQ      subregion  QLD

            regional_granularity="single_region"

        returns:
            fuel_type  technology       geo_id  year  price
            Gas        OCGT (small GT)  NEM     2026  14.50  # mean(16, 13); geo_id -> NEM
    """
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
    """Keeps only the _BIOMETHANE_PRICE_TYPE price, relabelled 'Biomethane', as long rows.

    I/O Example:
        prices:
            fuel_type                  2025-26
            Biomethane (Landfill gas)  13.31
            Biomethane (Waste)         25.57
            Biomethane (Crop residue)  36.54

        returns:
            fuel_type   year  price
            Biomethane  2026  25.57
    """
    # Filter for the specific 'Biomethane price'-type specified in IASR note:
    prices = prices[prices["fuel_type"] == _BIOMETHANE_PRICE_TYPE]
    prices = prices.assign(fuel_type="Biomethane")
    return _melt_fuel_prices_to_long(prices, ["fuel_type"])


def _zero_cost_rows() -> pd.DataFrame:
    """One row per fuel in _ZERO_COST_FUEL_TYPES, priced at 0.0.

    The key columns are left off, so they come out blank (wildcards) once these rows
    are concatenated into the full table.

    I/O Example:
        returns:
            fuel_type  price
            Solar      0.0
            Wind       0.0
            Water      0.0
    """
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
    """Reduces ``summary`` to one row of price keys per station, for ``fuel_types`` only.

    The summary has a row per generating unit, but fuel prices apply per station, so
    the unit-level name is left out and the units collapse into one row per station.
    A gas station listed under two technologies (Condamine A has gas and steam
    turbines) keeps a row for each, so that every key column stays filled for
    wildcard matching.

    I/O Example (fuel_types=["Gas"]):
        summary:
            IASR ID / DLT names  Power Station  Technology Type       geo_id  Fuel type   Fuel cost mapping
            CPSA_GT1             Condamine A    CCGT - Gas Turbine    SQ      Gas         Condamine A
            CPSA_GT2             Condamine A    CCGT - Gas Turbine    SQ      Gas         Condamine A
            CPSA_ST              Condamine A    CCGT - Steam Turbine  SQ      Gas         Condamine A
            BW01                 Bayswater      Steam Sub Critical    CNSW    Black Coal  Bayswater  # other fuel - dropped

        returns:
            fuel_type  technology            geo_id  power_station  fuel_cost_mapping
            Gas        CCGT - Gas Turbine    SQ      Condamine A    Condamine A
            Gas        CCGT - Steam Turbine  SQ      Condamine A    Condamine A
    """
    renamed = summary.rename(columns=_SUMMARY_COLUMN_RENAMES)
    filtered = renamed[renamed["fuel_type"].isin(fuel_types)]
    return filtered[_STATION_KEY_COLS + ["fuel_cost_mapping"]].drop_duplicates()


def _merge_prices_onto_generators(
    prices: pd.DataFrame,
    generators: pd.DataFrame,
    price_table_name: str,
) -> pd.DataFrame:
    """Merges each generator in ``generators`` onto its row of ``prices`` by fuel_cost_mapping.

    The merge check runs from the generators' side only. A generator whose mapping has no
    row in ``prices`` (e.g. 'Eraring' if the table only prices Bayswater) raises a
    ValueError naming the mapping and ``price_table_name``, since the model would
    otherwise have no fuel cost for it. Price rows that no generator uses are
    dropped without a log, as a price table can cover more keys than the summary
    needs (existing gas is merged against both gas tables combined).

    I/O Example:
        generators:
            power_station       fuel_cost_mapping
            Snuggery            Snuggery
            Port Lincoln GT     Port Lincoln

        prices:
            fuel_cost_mapping  2025-26
            Snuggery           4.12
            Port Lincoln       5.43
            Eraring            3.80  # unused: dropped

        returns:
            power_station       fuel_cost_mapping  2025-26
            Snuggery            Snuggery           4.12
            Port Lincoln GT     Port Lincoln       5.43
    """
    merged = generators.merge(
        prices,
        how="left",
        on="fuel_cost_mapping",
        indicator=True,
    )
    # TODO revisit logging unused rows; sticking point is gas w/ combined ecaa/new entrants
    _raise_on_unpriced_generators(
        merged,
        f"Generator fuel cost mappings with no price in '{price_table_name}'",
    )
    return merged.drop(columns="_merge")


def _raise_on_unpriced_generators(merged: pd.DataFrame, message: str) -> None:
    """Raises a ValueError listing the fuel cost mappings of ``merged``'s unpriced rows."""
    unmatched = sorted(
        merged.loc[merged["_merge"] == "left_only", "fuel_cost_mapping"].unique()
    )
    if unmatched:
        raise ValueError(f"{message}: {unmatched}")


def _melt_fuel_prices_to_long(table: pd.DataFrame, id_cols: list[str]) -> pd.DataFrame:
    """Melts the financial-year columns of ``table`` into one row per ``id_cols`` row and year.

    Each year is the one its financial year ends in, and a price that isn't numeric
    raises. Columns that are neither in ``id_cols`` nor a financial year (e.g.
    fuel_cost_mapping after a merge) are dropped.

    I/O Example (id_cols=["power_station"]):
        table:
            power_station  fuel_cost_mapping  2024-25  2025-26
            Bayswater      Bayswater          4.12     3.95

        returns:
            power_station  year  price
            Bayswater      2025  4.12
            Bayswater      2026  3.95
    """
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
