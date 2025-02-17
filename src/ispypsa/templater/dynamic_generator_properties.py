import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.templater.helpers import (
    _add_units_to_financial_year_columns,
    _convert_financial_year_columns_to_float,
)

from .helpers import _snakecase_string
from .lists import _ECAA_GENERATOR_TYPES


def _template_generator_dynamic_properties(
    iasr_tables: dict[str : pd.DataFrame], scenario: str
) -> dict[str, pd.DataFrame]:
    """Creates ISPyPSA templates for dynamic generator properties (i.e. those that vary
    with calendar/financial year).

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `dict[pd.DataFrame]`: Templates for dynamic generator properties including coal
            prices, gas prices, full outage rates for existing generators, partial outage
            rates for existing generators and ECAA generator seasonal ratings.
    """
    logging.info("Creating a template for dynamic generator properties")
    snakecase_scenario = _snakecase_string(scenario)

    coal_prices = iasr_tables[f"coal_prices_{snakecase_scenario}"]
    coal_prices = _template_coal_prices(coal_prices)

    gas_prices = iasr_tables[f"gas_prices_{snakecase_scenario}"]
    gas_prices = _template_gas_prices(gas_prices)

    liquid_fuel_prices = iasr_tables["liquid_fuel_prices"]
    liquid_fuel_prices = _template_liquid_fuel_prices(liquid_fuel_prices, scenario)

    full_outage_forecasts = _template_existing_generators_full_outage_forecasts(
        iasr_tables["full_outages_forecast_existing_generators"]
    )

    partial_outage_forecasts = _template_existing_generators_partial_outage_forecasts(
        iasr_tables["partial_outages_forecast_existing_generators"]
    )

    seasonal_ratings = [
        iasr_tables[f"seasonal_ratings_{gen_type}"]
        for gen_type in _ECAA_GENERATOR_TYPES
    ]
    seasonal_ratings = _template_seasonal_ratings(seasonal_ratings)

    closure_years = iasr_tables["expected_closure_years"]
    closure_years = _template_closure_years(closure_years)

    build_costs = _template_new_entrant_build_costs(iasr_tables, scenario)
    wind_and_solar_connection_costs = (
        _template_new_entrant_wind_and_solar_connection_costs(iasr_tables, scenario)
    )

    connection_costs_other = iasr_tables["connection_costs_other"]
    non_vre_connection_costs = _template_new_entrant_non_vre_connection_costs(
        connection_costs_other
    )
    return {
        "coal_prices": coal_prices,
        "gas_prices": gas_prices,
        "liquid_fuel_prices": liquid_fuel_prices,
        "full_outage_forecasts": full_outage_forecasts,
        "partial_outage_forecasts": partial_outage_forecasts,
        "seasonal_ratings": seasonal_ratings,
        "closure_years": closure_years,
        "build_costs": build_costs,
        "new_entrant_build_costs": build_costs,
        "new_entrant_wind_and_solar_connection_costs": wind_and_solar_connection_costs,
        "new_entrant_non_vre_connection_costs": non_vre_connection_costs,
    }


def _template_coal_prices(coal_prices: pd.DataFrame) -> pd.DataFrame:
    """Creates a coal price template

    Args:
        coal_prices: pd.DataFrame table from IASR workbook specifying coal prices
            forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for coal prices
    """
    coal_prices.columns = _add_units_to_financial_year_columns(
        coal_prices.columns, "$/GJ"
    )
    coal_prices = coal_prices.drop(columns="coal_price_scenario")
    coal_prices = _convert_financial_year_columns_to_float(coal_prices)
    return coal_prices


def _template_gas_prices(gas_prices: pd.DataFrame) -> pd.DataFrame:
    """Creates a gas price template

    Args:
        gas_prices: pd.DataFrame table from IASR workbook specifying gas prices
            forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for gas prices
    """
    cols = _add_units_to_financial_year_columns(gas_prices.columns, "$/GJ")
    cols[0] = "generator"
    gas_prices.columns = cols
    gas_prices = gas_prices.drop(columns="gas_price_scenario")
    gas_prices = _convert_financial_year_columns_to_float(gas_prices)
    return gas_prices


def _template_liquid_fuel_prices(
    liquid_fuel_prices: pd.DataFrame, scenario: str
) -> pd.Series:
    """Creates a liquid fuel prices template

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        liquid_fuel_prices: pd.DataFrame table from IASR workbook specifying liquid fuel
            price forecasts.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for liquid fuel prices
    """
    liquid_fuel_prices.columns = _add_units_to_financial_year_columns(
        liquid_fuel_prices.columns, "$/GJ"
    )
    liquid_fuel_prices = liquid_fuel_prices.drop(columns="liquid_fuel_price").set_index(
        "liquid_fuel_price_scenario"
    )
    liquid_fuel_prices = _convert_financial_year_columns_to_float(liquid_fuel_prices)
    liquid_fuel_prices_scenario = liquid_fuel_prices.loc[scenario, :]
    liquid_fuel_prices_scenario.index.set_names("FY", inplace=True)
    liquid_fuel_prices_scenario.name = "fuel_price"
    return liquid_fuel_prices_scenario


def _template_existing_generators_full_outage_forecasts(
    full_outages_forecast: pd.DataFrame,
) -> pd.DataFrame:
    """Creates a full outage forecast template for existing generators

    Args:
        full_outages_forecast: pd.DataFrame table from IASR workbook specifying full
            outage forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for full outage forecasts
    """
    full_outages_forecast.columns = [
        _snakecase_string(col) for col in full_outages_forecast.columns
    ]
    full_outages_forecast = full_outages_forecast.set_index("fuel_type")
    full_outages_forecast = _apply_all_coal_averages(full_outages_forecast)
    full_outages_forecast = _convert_financial_year_columns_to_float(
        full_outages_forecast.drop(index="All Coal Average")
    )
    return full_outages_forecast


def _template_existing_generators_partial_outage_forecasts(
    partial_outages_forecast: pd.DataFrame,
) -> pd.DataFrame:
    """Creates a partial outage forecast template for existing generators

    Args:
        partial_outages_forecast: pd.DataFrame table from IASR workbook specifying
            partial outage forecasts.

    Returns:
        `pd.DataFrame`: ISPyPSA template for partial outage forecasts
    """
    partial_outages_forecast.columns = [
        _snakecase_string(col) for col in partial_outages_forecast.columns
    ]
    partial_outages_forecast = partial_outages_forecast.set_index("fuel_type")
    partial_outages_forecast = _apply_all_coal_averages(partial_outages_forecast)
    partial_outages_forecast = _convert_financial_year_columns_to_float(
        partial_outages_forecast.drop(index="All Coal Average")
    )
    return partial_outages_forecast


def _template_closure_years(closure_years: pd.DataFrame) -> pd.DataFrame:
    """Creates a closure years template for existing generators

    Args:
        closure_years: pd.DataFrame table from IASR workbook specifying full
            generator closure years.

    Returns:
        `pd.DataFrame`: ISPyPSA template for full outage forecasts
    """
    closure_years.columns = [_snakecase_string(col) for col in closure_years.columns]
    closure_years = closure_years.rename(columns={"generator_name": "generator"})
    closure_years = closure_years.loc[
        :, ["generator", "duid", "expected_closure_year_calendar_year"]
    ]
    return closure_years


def _template_seasonal_ratings(
    seasonal_ratings: list[pd.DataFrame],
) -> pd.DataFrame:
    """Creates a seasonal generator ratings template

    Args:
        seasonal_ratings: list of pd.DataFrame tables from IASR workbook specifying
            the seasonal ratings of the different generator types.

    Returns:
        `pd.DataFrame`: ISPyPSA template for seasonal generator ratings
    """

    seasonal_rating = pd.concat(seasonal_ratings, axis=0)
    seasonal_rating.columns = [
        _snakecase_string(col) for col in seasonal_rating.columns
    ]
    seasonal_rating = _convert_seasonal_columns_to_float(seasonal_rating)
    return seasonal_rating


def _template_new_entrant_build_costs(
    iasr_tables: dict[str : pd.DataFrame], scenario: str
) -> pd.DataFrame:
    """Creates a new entrants build cost template

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for new entrant build costs
    """
    scenario_mapping = iasr_tables["build_costs_scenario_mapping"]
    scenario_mapping = scenario_mapping.set_index(scenario_mapping.columns[0])
    scenario_mapping = scenario_mapping.transpose().squeeze()
    gencost_scenario_desc = re.match(
        r"GenCost\s(.*)", scenario_mapping[scenario]
    ).group(1)

    build_costs_scenario = iasr_tables[
        f"build_costs_{_snakecase_string(gencost_scenario_desc)}"
    ]
    build_costs_phes = iasr_tables["build_costs_pumped_hydro"]

    build_costs = pd.concat([build_costs_scenario, build_costs_phes], axis=0)
    build_costs = _convert_financial_year_columns_to_float(build_costs)
    build_costs = build_costs.drop(columns=["Source"])
    # convert data in $/kW to $/MW
    build_costs.columns = _add_units_to_financial_year_columns(
        build_costs.columns, "$/MW"
    )
    build_costs = build_costs.set_index("technology")
    build_costs *= 1000.0
    return build_costs


def _template_new_entrant_wind_and_solar_connection_costs(
    iasr_tables: dict[str : pd.DataFrame], scenario: str
) -> pd.DataFrame:
    """Creates a new entrant wind and solar connection cost template

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        iasr_tables: Dict of tables from the IASR workbook that have been parsed using
            `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration


    Returns:
        `pd.DataFrame`: ISPyPSA template for new entrant wind and solar connection costs
    """
    scenario = _snakecase_string(scenario)
    if scenario == "step_change" or scenario == "green_energy_exports":
        file_scenario = "step_change&green_energy_exports"
    else:
        file_scenario = scenario
    # get rez cost forecasts and concatenate non-rez cost forecasts
    wind_solar_connection_costs_forecasts = iasr_tables[
        f"connection_cost_forecast_wind_and_solar_{file_scenario}"
    ]
    wind_solar_connection_costs_forecasts = (
        wind_solar_connection_costs_forecasts.set_index("REZ names")
    )
    wind_solar_connection_costs_forecasts = (
        wind_solar_connection_costs_forecasts.rename(
            columns={"REZ network voltage (kV)": "Network voltage (kV)"}
        )
    )

    non_rez_connection_costs_forecasts = iasr_tables[
        f"connection_cost_forecast_non_rez_{file_scenario}"
    ]
    non_rez_connection_costs_forecasts = non_rez_connection_costs_forecasts.set_index(
        "Non-REZ name"
    )

    wind_solar_connection_cost_forecasts = pd.concat(
        [non_rez_connection_costs_forecasts, wind_solar_connection_costs_forecasts],
        axis=0,
    )
    # get system strength connection cost from the initial connection cost table
    initial_wind_solar_connection_costs = iasr_tables[
        f"connection_costs_for_wind_and_solar"
    ].set_index("REZ names")

    system_strength_cost = (
        initial_wind_solar_connection_costs["System Strength connection cost ($/kW)"]
        * 1000
    ).rename("System strength connection cost ($/MW)")
    wind_solar_connection_cost_forecasts = pd.concat(
        [wind_solar_connection_cost_forecasts, system_strength_cost], axis=1
    )
    # remove notes
    wind_solar_connection_cost_forecasts = wind_solar_connection_cost_forecasts.replace(
        "Note 1", np.nan
    )
    # calculate $/MW by dividing total cost by connection capacity in MVA
    wind_solar_connection_cost_forecasts = _convert_financial_year_columns_to_float(
        wind_solar_connection_cost_forecasts
    )
    fy_cols = [
        col
        for col in wind_solar_connection_cost_forecasts.columns
        if re.match(r"[0-9]{4}-[0-9]{2}", col)
    ]
    for col in fy_cols:
        wind_solar_connection_cost_forecasts[col] /= (
            wind_solar_connection_cost_forecasts["Connection capacity (MVA)"]
        )
    wind_solar_connection_cost_forecasts.columns = _add_units_to_financial_year_columns(
        wind_solar_connection_cost_forecasts.columns, "$/MW"
    )
    return wind_solar_connection_cost_forecasts.reset_index()


def _template_new_entrant_non_vre_connection_costs(
    connection_costs: pd.DataFrame,
) -> pd.DataFrame:
    """Creates a new entrant non-VRE connection cost template

    Args:
        connection_costs: list of pd.DataFrame tables from IASR workbook specifying
            the seasonal ratings of the different generator types.

    Returns:
        `pd.DataFrame`: ISPyPSA template for new entrant non-VRE connection costs
    """
    connection_costs = connection_costs.set_index("Region")
    # convert to $/MW and add units to columns
    col_rename_map = {}
    for col in connection_costs.columns:
        connection_costs[col] *= 1000
        col_rename_map[col] = _snakecase_string(col) + "_$/mw"
    connection_costs = connection_costs.rename(columns=col_rename_map)
    return connection_costs.reset_index()


def _convert_seasonal_columns_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """Forcefully converts seasonal columns to float columns"""
    cols = [
        df[col].astype(float)
        if re.match(r"summer", col) or re.match(r"winter", col)
        else df[col]
        for col in df.columns
    ]
    return pd.concat(cols, axis=1)


def _apply_all_coal_averages(outages_df: pd.DataFrame) -> pd.DataFrame:
    """Applies the All Coal Average to each coal fuel type"""
    where_coal_average = outages_df.loc["All Coal Average", :].notna()
    for coal_row in outages_df.index[outages_df.index.str.contains("Coal")]:
        outages_df.loc[coal_row, where_coal_average] = outages_df.loc[
            "All Coal Average", where_coal_average
        ]
    return outages_df
