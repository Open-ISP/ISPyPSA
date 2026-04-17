# File holds functions for templating tables that hold data with time-varying
# (spanning multiple years) values. These tables contain data that applies to
# both existing/planned and new entrant technologies, both generation and storage.

import pandas as pd

from ispypsa.templater.helpers import _snakecase_string


def _template_connection_costs(
    connection_cost_tables: dict[str, pd.DataFrame],
    scenario: str,
) -> pd.DataFrame:
    vre_connection_costs = _template_vre_connection_costs(
        connection_cost_tables["connection_cost_forecast_wind_and_solar"],
        connection_cost_tables["connection_costs_for_wind_and_solar"],
        scenario,
    )

    non_vre_connection_costs = _template_non_vre_connection_costs(
        connection_cost_tables["connection_cost_forecast_other"],
        connection_cost_tables["connection_costs_other"],
        scenario,
    )

    return pd.DataFrame()


def _template_vre_connection_costs(
    connection_cost_forecast_vre: pd.DataFrame,
    connection_costs_for_vre: pd.DataFrame,
    scenario: str,
) -> pd.DataFrame:
    return pd.DataFrame()


def _template_non_vre_connection_costs(
    connection_cost_forecast_other: pd.DataFrame,
    connection_costs_other: pd.DataFrame,
    scenario: str,
) -> pd.DataFrame:
    return pd.DataFrame()
