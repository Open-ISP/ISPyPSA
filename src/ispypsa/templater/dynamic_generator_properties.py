import logging
import re
from pathlib import Path

import pandas as pd

from .helpers import _snakecase_string
from .lists import _ECAA_GENERATOR_TYPES


def template_generator_dynamic_properties(
    parsed_workbook_path: Path | str, scenario: str
) -> dict[str, pd.DataFrame]:
    """Creates ISPyPSA templates for dynamic generator properties (i.e. those that vary
    with calendar/financial year).

    Args:
        parsed_workbook_path: Path to directory containing CSVs that are the output
            of parsing an ISP Inputs and Assumptions workbook using `isp-workbook-parser`
        scenario: Scenario obtained from the model configuration

    Returns:
        `dict[pd.DataFrame]`: Templates for dynamic generator properties including coal
            prices, gas prices, full outage rates for existing generators, partial outage
            rates for existing generators and ECAA generator seasonal ratings.
    """
    logging.info("Creating a template for dynamic generator properties")
    coal_prices = _template_coal_prices(parsed_workbook_path, scenario)
    gas_prices = _template_gas_prices(parsed_workbook_path, scenario)
    liquid_fuel_prices = _template_liquid_fuel_prices(parsed_workbook_path)
    full_outage_forecasts = _template_full_outage_forecasts(parsed_workbook_path)
    partial_outage_forecasts = _template_partial_outage_forecasts(parsed_workbook_path)
    seasonal_ratings = _template_seasonal_ratings(parsed_workbook_path)
    return {
        "coal_prices": coal_prices,
        "gas_prices": gas_prices,
        "liquid_fuel_prices": liquid_fuel_prices,
        "full_outage_forecasts": full_outage_forecasts,
        "partial_outage_forecasts": partial_outage_forecasts,
        "seasonal_ratings": seasonal_ratings,
    }


def _template_coal_prices(
    parsed_workbook_path: Path | str, scenario: str
) -> pd.DataFrame:
    """Creates a coal price template

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for coal prices
    """
    snakecase_scenario = _snakecase_string(scenario)
    coal_prices = pd.read_csv(
        Path(parsed_workbook_path, f"coal_prices_{snakecase_scenario}.csv")
    )
    coal_prices.columns = [
        _snakecase_string(col + "_$/GJ")
        if re.match(r"[0-9]{4}-[0-9]{2}", col)
        else _snakecase_string(col)
        for col in coal_prices.columns
    ]
    coal_prices = coal_prices.drop(columns="coal_price_scenario")
    return coal_prices.set_index("generator")


def _template_gas_prices(
    parsed_workbook_path: Path | str, scenario: str
) -> pd.DataFrame:
    """Creates a gas price template

    The function behaviour depends on the `scenario` specified in the model
    configuration.

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for gas prices
    """
    snakecase_scenario = _snakecase_string(scenario)
    gas_prices = pd.read_csv(
        Path(parsed_workbook_path, f"gas_prices_{snakecase_scenario}.csv")
    )
    cols = [
        _snakecase_string(col + "_$/GJ")
        if re.match(r"[0-9]{4}-[0-9]{2}", col)
        else _snakecase_string(col)
        for col in gas_prices.columns
    ]
    cols[0] = "generator"
    gas_prices.columns = cols
    gas_prices = gas_prices.drop(columns="gas_price_scenario")
    return gas_prices.set_index("generator")


def _template_liquid_fuel_prices(parsed_workbook_path: Path | str) -> pd.DataFrame:
    """Creates a liquid fuel prices template

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA template for liquid fuel prices
    """
    liquid_fuel_prices = pd.read_csv(
        Path(parsed_workbook_path, "liquid_fuel_prices.csv")
    )
    cols = [
        _snakecase_string(col + "_$/GJ")
        if re.match(r"[0-9]{4}-[0-9]{2}", col)
        else _snakecase_string(col)
        for col in liquid_fuel_prices.columns
    ]
    liquid_fuel_prices.columns = cols
    liquid_fuel_prices = liquid_fuel_prices.drop(columns="liquid_fuel_price")
    return liquid_fuel_prices.set_index("liquid_fuel_price_scenario")


def _template_full_outage_forecasts(parsed_workbook_path: Path | str) -> pd.DataFrame:
    """Creates a full outage forecast template for existing generators

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA template for full outage forecasts
    """
    full_outages_forecast = pd.read_csv(
        Path(parsed_workbook_path, "full_outages_forecast_existing_generators.csv")
    )
    full_outages_forecast.columns = [
        _snakecase_string(col) for col in full_outages_forecast.columns
    ]
    full_outages_forecast = full_outages_forecast.set_index("fuel_type")
    where_coal_average = full_outages_forecast.loc["All Coal Average", :].notna()
    for coal_row in full_outages_forecast.index[
        full_outages_forecast.index.str.contains("Coal")
    ]:
        full_outages_forecast.loc[coal_row, where_coal_average] = (
            full_outages_forecast.loc["All Coal Average", where_coal_average]
        )
    return full_outages_forecast.drop(index="All Coal Average")


def _template_partial_outage_forecasts(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Creates a partial outage forecast template for existing generators

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA template for partial outage forecasts
    """
    partial_outages_forecast = pd.read_csv(
        Path(parsed_workbook_path, "partial_outages_forecast_existing_generators.csv")
    )
    partial_outages_forecast.columns = [
        _snakecase_string(col) for col in partial_outages_forecast.columns
    ]
    partial_outages_forecast = partial_outages_forecast.set_index("fuel_type")
    where_coal_average = partial_outages_forecast.loc["All Coal Average", :].notna()
    for coal_row in partial_outages_forecast.index[
        partial_outages_forecast.index.str.contains("Coal")
    ]:
        partial_outages_forecast.loc[coal_row, where_coal_average] = (
            partial_outages_forecast.loc["All Coal Average", where_coal_average]
        )
    return partial_outages_forecast.drop(index="All Coal Average")


def _template_seasonal_ratings(
    parsed_workbook_path: Path | str,
) -> pd.DataFrame:
    """Creates a seasonal generator ratings template

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.

    Returns:
        `pd.DataFrame`: ISPyPSA template for seasonal generator ratings
    """
    seasonal_ratings = [
        pd.read_csv(Path(parsed_workbook_path, f"seasonal_ratings_{gen_type}.csv"))
        for gen_type in _ECAA_GENERATOR_TYPES
    ]
    seasonal_rating = pd.concat(seasonal_ratings, axis=0)
    seasonal_rating.columns = [
        _snakecase_string(col) for col in seasonal_rating.columns
    ]
    return seasonal_rating.set_index("generator")
