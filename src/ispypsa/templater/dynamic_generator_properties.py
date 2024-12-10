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
    liquid_fuel_prices = _template_liquid_fuel_prices(parsed_workbook_path, scenario)
    full_outage_forecasts = _template_full_outage_forecasts(parsed_workbook_path)
    partial_outage_forecasts = _template_partial_outage_forecasts(parsed_workbook_path)
    seasonal_ratings = _template_seasonal_ratings(parsed_workbook_path)
    closure_years = _template_closure_years(parsed_workbook_path)
    return {
        "coal_prices": coal_prices,
        "gas_prices": gas_prices,
        "liquid_fuel_prices": liquid_fuel_prices,
        "full_outage_forecasts": full_outage_forecasts,
        "partial_outage_forecasts": partial_outage_forecasts,
        "seasonal_ratings": seasonal_ratings,
        "closure_years": closure_years,
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
    coal_prices.columns = _add_units_to_financial_year_columns(coal_prices.columns)
    coal_prices = coal_prices.drop(columns="coal_price_scenario")
    coal_prices = coal_prices.set_index("generator")
    coal_prices = _convert_financial_year_columns_to_float(coal_prices)
    return coal_prices


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
    cols = _add_units_to_financial_year_columns(gas_prices.columns)
    cols[0] = "generator"
    gas_prices.columns = cols
    gas_prices = gas_prices.drop(columns="gas_price_scenario").set_index("generator")
    gas_prices = _convert_financial_year_columns_to_float(gas_prices)
    return gas_prices


def _template_liquid_fuel_prices(
    parsed_workbook_path: Path | str, scenario: str
) -> pd.Series:
    """Creates a liquid fuel prices template

    Args:
        parsed_workbook_path: Path to directory with table CSVs that are the
            outputs from the `isp-workbook-parser`.
        scenario: Scenario obtained from the model configuration

    Returns:
        `pd.DataFrame`: ISPyPSA template for liquid fuel prices
    """
    liquid_fuel_prices = pd.read_csv(
        Path(parsed_workbook_path, "liquid_fuel_prices.csv")
    )
    liquid_fuel_prices.columns = _add_units_to_financial_year_columns(
        liquid_fuel_prices.columns
    )
    liquid_fuel_prices = liquid_fuel_prices.drop(columns="liquid_fuel_price").set_index(
        "liquid_fuel_price_scenario"
    )
    liquid_fuel_prices = _convert_financial_year_columns_to_float(liquid_fuel_prices)
    liquid_fuel_prices_scenario = liquid_fuel_prices.loc[scenario, :]
    liquid_fuel_prices_scenario.index.set_names("FY", inplace=True)
    return liquid_fuel_prices_scenario


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
    full_outages_forecast = _apply_all_coal_averages(full_outages_forecast)
    full_outages_forecast = _convert_financial_year_columns_to_float(
        full_outages_forecast.drop(index="All Coal Average")
    )
    return full_outages_forecast


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
    partial_outages_forecast = _apply_all_coal_averages(partial_outages_forecast)
    partial_outages_forecast = _convert_financial_year_columns_to_float(
        partial_outages_forecast.drop(index="All Coal Average")
    )
    return partial_outages_forecast


def _template_closure_years(parsed_workbook_path: Path | str) -> pd.DataFrame:
    closure_years = pd.read_csv(
        Path(parsed_workbook_path, "expected_closure_years.csv")
    )
    closure_years.columns = [_snakecase_string(col) for col in closure_years.columns]
    closure_years = closure_years.rename(columns={"generator_name": "generator"})
    closure_years = closure_years.loc[
        :, ["generator", "duid", "expected_closure_year_calendar_year"]
    ]
    closure_years = closure_years.set_index("generator")
    return closure_years


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
    seasonal_rating = _convert_seasonal_columns_to_float(seasonal_rating)
    seasonal_rating = seasonal_rating.set_index("generator")
    return seasonal_rating


def _add_units_to_financial_year_columns(columns: pd.Index) -> list[str]:
    """Adds _$/GJ to the financial year columns"""
    cols = [
        _snakecase_string(col + "_$/GJ")
        if re.match(r"[0-9]{4}-[0-9]{2}", col)
        else _snakecase_string(col)
        for col in columns
    ]
    return cols


def _convert_financial_year_columns_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """Forcefully converts FY columns to float columns"""
    cols = [
        df[col].astype(float) if re.match(r"[0-9]{4}_[0-9]{2}", col) else df[col]
        for col in df.columns
    ]
    return pd.concat(cols, axis=1)


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
