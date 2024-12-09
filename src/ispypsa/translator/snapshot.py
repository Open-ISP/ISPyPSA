from datetime import datetime

import pandas as pd


def create_snapshot_index(
    start_year: int,
    end_year: int,
    operational_temporal_resolution_min: int,
    year_type: str,
) -> pd.DataFrame:
    """Creates a DatetimeIndex, stored in DataFrame, defining the snapshots for the model before temporal aggregation.

    The index will start at the beginning of `start_year` and finish at the end of
    `end_year` with the specified temporal resolution.

    Args:
        ispypsa_inputs_path: Path to directory containing modelling input template CSVs.

    Returns:
        pd.DataFrame
    """
    if year_type == "fy":
        start_year = start_year - 1
        end_year = end_year
        month = 7
    else:
        start_year = start_year
        end_year = end_year + 1
        month = 1

    if operational_temporal_resolution_min < 60:
        hour = 0
        minute = operational_temporal_resolution_min
    else:
        hour = operational_temporal_resolution_min // 60
        minute = operational_temporal_resolution_min % 60

    start_date = datetime(year=start_year, month=month, day=1, hour=hour, minute=minute)
    end_date = datetime(year=end_year, month=month, day=1, hour=0, minute=0)

    time_index = pd.date_range(
        start=start_date,
        end=end_date,
        freq=str(operational_temporal_resolution_min) + "min",
    )
    time_index = pd.DataFrame(index=time_index)
    return time_index
