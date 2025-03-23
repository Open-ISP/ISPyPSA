import pandas as pd
import pypsa


def _add_investment_period_weights(
    network: pypsa.Network, investment_period_weights: pd.DataFrame
) -> None:
    """Adds investment period weights defined in a pypsa-friendly `pd.DataFrame` to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        investment_period_weights: `pd.DataFrame` specifying the
            investment period weights with columns 'period', "years" and 'objective'.
            Where "period" is the start years of the investment periods, "years" is the
            length of each investment period, and "objective" is the relative weight of
            the objective function in each investment period.

    Returns: None
    """
    investment_period_weights = investment_period_weights.set_index("period")
    network.investment_period_weightings = investment_period_weights
