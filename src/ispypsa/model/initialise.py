from datetime import datetime

import pandas as pd
import pypsa


def prepare_snapshot_index(start_year, end_year, temporal_resolution, year_type):
    """Creates a pd datetime index defining the snapshots for the model.

    The index will start at the beginning of the start_year and to the end of the end_year with the given temporal
    resolution.

    Args:
        start_year: int defining the first year the model covers (i.e. inclusive)
        end_year: int defining the last year the model covers (i.e. inclusive)
        year_type: str defining the year type, 'fy' for financial years, and 'calendar' for calendar years. For 'fy' the
        int definition of a financial is the year the financial ends.
        temporal_resolution: the temporal resolution of the model defined by a number unit combo accepted by
            pd.date_range i.e. '30min', '1h', or '3h'.
    """
    if year_type == "fy":
        start_date = datetime(year=start_year - 1, month=7, day=1, hour=0, minute=30)
        end_date = datetime(year=end_year, month=7, day=1, hour=0, minute=0)
    else:
        start_date = datetime(year=start_year, month=1, day=1, hour=0, minute=30)
        end_date = datetime(year=end_year + 1, month=1, day=1, hour=0, minute=0)

    time_index = pd.date_range(start=start_date, end=end_date, freq=temporal_resolution)

    time_index.strftime("'%Y-%m-%d %H:%M:%S")

    return time_index


def initialise_network(
    start_year: int, end_year: int, year_type: str, temporal_resolution: str
):
    """Creates a pypsa.Network object with snapshots defined.

    Args:
        start_year: int defining the first year the model covers (i.e. inclusive)
        end_year: int defining the last year the model covers (i.e. inclusive)
        year_type: str defining the year type, 'fy' for financial years, and 'calendar' for calendar years. For 'fy' the
        int definition of a financial is the year the financial ends.
        temporal_resolution: the temporal resolution of the model defined by a number unit combo accepted by
            pd.date_range i.e. '30min', '1h', or '3h'.

    """
    time_index = prepare_snapshot_index(
        start_year, end_year, temporal_resolution, year_type
    )
    network = pypsa.Network(snapshots=time_index)
    return network
