from datetime import timedelta

import pandas as pd


def _calculate_week_starting(timesteps: pd.Series) -> pd.Series:
    """Calculate the week starting date (Monday) for each timestep.

    Args:
        timesteps: Series of datetime objects

    Returns:
        Series of dates representing the Monday of the week for each timestep.
    """
    return (timesteps - timedelta(seconds=1)).dt.to_period("W").dt.start_time.dt.date
