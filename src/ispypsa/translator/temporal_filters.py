from datetime import datetime, timedelta

import pandas as pd

from ispypsa.config.validators import TemporalConfig
from ispypsa.translator.helpers import _get_iteration_start_and_end_time


def _time_series_filter(time_series_data: pd.DataFrame, snapshots: pd.DataFrame):
    """Filters a timeseries pandas DataFrame based using the datetime values in
     the snapshots index.

    Examples:

    >>> datetime_index = pd.date_range('2020-01-01', '2020-01-03', freq='h')
    >>> time_series_data = pd.DataFrame({'Datetime': datetime_index, 'Value': range(len(datetime_index))})
    >>> snapshot = pd.DataFrame(index=datetime_index[::12])  # Every 12 hours
    >>> _time_series_filter(time_series_data, snapshot)
                  Datetime  Value
    0  2020-01-01 00:00:00      0
    12 2020-01-01 12:00:00     12
    24 2020-01-02 00:00:00     24
    36 2020-01-02 12:00:00     36
    48 2020-01-03 00:00:00     48

    Args:
        time_series_data: pd.DataFrame with time series column called 'Datetime'
        snapshots: pd.DataFrame with datetime index

    """
    return time_series_data[time_series_data["Datetime"].isin(snapshots["snapshots"])]


def _filter_snapshots(config: TemporalConfig, snapshots: pd.DataFrame):
    """Appy filter to the snapshots based on the model config.

    - If config.representative_weeks is not None then filter the
      snapshots based on the supplied list of representative weeks.

    Examples:

    # Create dummy config class with just data need for example.

    >>> from dataclasses import dataclass

    >>> @dataclass
    ... class TemporalAggregationConfig:
    ...     representative_weeks: list[int]

    >>> @dataclass
    ... class TemporalConfig:
    ...     start_year: int
    ...     end_year: int
    ...     year_type: str
    ...     aggregation: TemporalAggregationConfig

    >>> config = TemporalConfig(
    ...     start_year=2024,
    ...     end_year=2024,
    ...     year_type='calendar',
    ...     aggregation=TemporalAggregationConfig(
    ...        representative_weeks=[1],
    ...     )
    ... )

    >>> snapshots = pd.DataFrame(index=pd.date_range('2024-01-01', '2024-12-31', freq='h'))

    >>> snapshots = _filter_snapshots(config, snapshots)

    >>> snapshots.index[0]
    Timestamp('2024-01-01 01:00:00')

    >>> snapshots.index[-1]
    Timestamp('2024-01-08 00:00:00')

    Args:
         config: TemporalConfig defining snapshot filtering.
         snapshots: pd.DataFrame with datetime index containing the snapshot
    """
    if config.aggregation.representative_weeks is not None:
        snapshots = _filter_snapshots_for_representative_weeks(
            representative_weeks=config.aggregation.representative_weeks,
            snapshots=snapshots,
            start_year=config.start_year,
            end_year=config.end_year,
            year_type=config.year_type,
        )
    return snapshots


def _filter_snapshots_for_representative_weeks(
    representative_weeks: list[int],
    snapshots: pd.DataFrame,
    start_year: int,
    end_year: int,
    year_type: str,
):
    """Filters a snapshot by a list of weeks.

    A snapshot is provided as a pandas DatFrame with a datetime index. The
    snapshot may be multiple years in length. The snapshot is filtered for
    date times that fall within the weeks defined in representative_weeks.
    The weeks are defined as full weeks within a financial or calendar year,
    depending on the year_type provided.

    Examples:
    >>> # Filter for first and last full weeks of each calendar year from 2020-2022
    >>> df = pd.DataFrame(index=pd.date_range('2020-01-01', '2022-12-31', freq='h'))
    >>> _filter_snapshots_for_representative_weeks(
    ...     representative_weeks=[1],
    ...     snapshots=df,
    ...     start_year=2020,
    ...     end_year=2022,
    ...     year_type='calendar'
    ... ).head(3)
    Empty DataFrame
    Columns: []
    Index: [2020-01-06 01:00:00, 2020-01-06 02:00:00, 2020-01-06 03:00:00]

    >>> # Filter for weeks 1, 26 of financial years 2021-2022 (July 2020 - June 2022)
    >>> df = pd.DataFrame(index=pd.date_range('2020-07-01', '2022-06-30', freq='h'))
    >>> _filter_snapshots_for_representative_weeks(
    ...     representative_weeks=[2],
    ...     snapshots=df,
    ...     start_year=2021,
    ...     end_year=2022,
    ...     year_type='fy'
    ... ).head(3)
    Empty DataFrame
    Columns: []
    Index: [2020-07-13 01:00:00, 2020-07-13 02:00:00, 2020-07-13 03:00:00]

    Args:
        representative_weeks: list[int] of full weeks to filter for. The
            week 1 refers to the first full week (Monday-Sunday) falling
            with in the year.
        snapshots: pd.DataFrame with datetime index containing the snapshot
        start_year: int defining the start year of the snapshot (inclusive)
        end_year: int defining the end year of the snapshot (inclusive)
        year_type: str defining year the 'fy' for financial year or 'calendar'

    Raises: ValueError if the end of week falls outside after the year end i.e.
        for all weeks 53 or greater and for some years the week 52.
    """
    start_year, end_year, month = _get_iteration_start_and_end_time(
        year_type, start_year, end_year
    )

    snapshots = snapshots["snapshots"]

    filtered_snapshots = []

    for year in range(start_year, end_year):
        start_of_year_date_time = datetime(
            year=year, month=month, day=1, hour=0, minute=0
        )
        end_of_year_date_time = datetime(
            year=year + 1, month=month, day=1, hour=0, minute=0
        )
        days_until_monday = (7 - start_of_year_date_time.weekday()) % 7
        first_monday = start_of_year_date_time + timedelta(days=days_until_monday)
        for week_number in representative_weeks:
            nth_week_start = first_monday + timedelta(weeks=week_number - 1)
            nth_week_end = nth_week_start + timedelta(days=7)

            if nth_week_end - timedelta(seconds=1) > end_of_year_date_time:
                raise ValueError(
                    f"Representative week {week_number} ends after end of model year {year}."
                    " Adjust config to use a smaller week_number for representative_weeks."
                )

            filtered_snapshots.append(
                snapshots[
                    (snapshots > nth_week_start) & (snapshots <= nth_week_end)
                ].copy()
            )

    filtered_snapshots = pd.concat(filtered_snapshots)

    filtered_snapshots = pd.DataFrame({"snapshots": filtered_snapshots})

    return filtered_snapshots
