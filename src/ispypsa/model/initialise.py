from datetime import datetime

import pandas as pd
import pypsa


def prepare_snapshot_index(
    start_year: int,
    end_year: int,
    operational_temporal_resolution_min: int,
    year_type: str,
) -> pd.DatetimeIndex:
    """Creates a DatetimeIndex defining the snapshots for the model.

    The index will start at the beginning of `start_year` and finish at the end of
    `end_year` with the specified temporal resolution.

    Args:
        start_year: the first year the model covers (i.e. inclusive)
        end_year: the last year the model covers (i.e. inclusive)
        year_type: The year type. 'fy' for financial years, and 'calendar' for calendar
            years. For 'fy', `start_year` and `end_year` refer to the year  in which
            the financial year ends.
        operational_temporal_resolution_min: int defining the temporal resolution in minutes.
    """
    if year_type == "fy":
        start_date = datetime(year=start_year - 1, month=7, day=1, hour=0, minute=30)
        end_date = datetime(year=end_year, month=7, day=1, hour=0, minute=0)
    else:
        start_date = datetime(year=start_year, month=1, day=1, hour=0, minute=30)
        end_date = datetime(year=end_year + 1, month=1, day=1, hour=0, minute=0)
    time_index = pd.date_range(
        start=start_date,
        end=end_date,
        freq=str(operational_temporal_resolution_min) + "min",
    )
    time_index.strftime("'%Y-%m-%d %H:%M:%S")
    return time_index


def initialise_network(
    start_year: int,
    end_year: int,
    year_type: str,
    operational_temporal_resolution_min: int,
) -> pypsa.Network:
    """Creates a `pypsa.Network object` with snapshots defined.

    Args:
        start_year: the first year the model covers (i.e. inclusive)
        end_year: the last year the model covers (i.e. inclusive)
        year_type: The year type. 'fy' for financial years, and 'calendar' for calendar
            years. For 'fy', `start_year` and `end_year` refer to the year  in which
            the financial year ends.
        operational_temporal_resolution_min: int defining the temporal resolution in minutes.

    Returns:
        `pypsa.Network` object
    """
    time_index = prepare_snapshot_index(
        start_year, end_year, operational_temporal_resolution_min, year_type
    )
    network = pypsa.Network(snapshots=time_index)
    return network
