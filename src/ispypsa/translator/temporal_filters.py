from datetime import datetime, timedelta

import pandas as pd

from ispypsa.config.validators import TemporalConfig


def time_series_filter(time_series_data, snapshot):
    """Filters a timeseries pandas DataFrame based using the datetime values in
     the snapshot index.

    Examples:

    >>> datetime_index = pd.date_range('2020-01-01', '2020-01-03', freq='h')
    >>> time_series_data = pd.DataFrame({'Datetime': datetime_index, 'Value': range(len(datetime_index))})
    >>> snapshot = pd.DataFrame(index=datetime_index[::12])  # Every 12 hours
    >>> time_series_filter(time_series_data, snapshot)
                  Datetime  Value
    0  2020-01-01 00:00:00      0
    12 2020-01-01 12:00:00     12
    24 2020-01-02 00:00:00     24
    36 2020-01-02 12:00:00     36
    48 2020-01-03 00:00:00     48

    """
    return time_series_data[time_series_data["Datetime"].isin(snapshot.index)]


def filter_snapshot(config: TemporalConfig, snapshot: pd.DataFrame):
    """Appy filter to the snapshot based on the model config.

    - If config.representative_weeks is not None then filter the
      snapshot based on the supplied list of representative weeks.

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

    >>> snapshot = pd.DataFrame(index=pd.date_range('2024-01-01', '2024-12-31', freq='h'))

    >>> snapshot = filter_snapshot(config, snapshot)

    >>> snapshot.index[0]
    Timestamp('2024-01-01 01:00:00')

    >>> snapshot.index[-1]
    Timestamp('2024-01-08 00:00:00')

    Args:
         config: TemporalConfig defining snapshot filtering.
         snapshot: pd.DataFrame with datetime index containing the snapshot
    """
    if config.aggregation.representative_weeks is not None:
        snapshot = filter_snapshot_for_representative_weeks(
            representative_weeks=config.aggregation.representative_weeks,
            snapshot=snapshot,
            start_year=config.start_year,
            end_year=config.end_year,
            year_type=config.year_type,
        )
    return snapshot


def filter_snapshot_for_representative_weeks(
    representative_weeks: list[int],
    snapshot: pd.DataFrame,
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
    >>> filter_snapshot_for_representative_weeks(
    ...     representative_weeks=[1],
    ...     snapshot=df,
    ...     start_year=2020,
    ...     end_year=2022,
    ...     year_type='calendar'
    ... ).head(3)
    Empty DataFrame
    Columns: []
    Index: [2020-01-06 01:00:00, 2020-01-06 02:00:00, 2020-01-06 03:00:00]

    >>> # Filter for weeks 1, 26 of financial years 2021-2022 (July 2020 - June 2022)
    >>> df = pd.DataFrame(index=pd.date_range('2020-07-01', '2022-06-30', freq='h'))
    >>> filter_snapshot_for_representative_weeks(
    ...     representative_weeks=[2],
    ...     snapshot=df,
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
        snapshot: pd.DataFrame with datetime index containing the snapshot
        start_year: int defining the start year of the snapshot (inclusive)
        end_year: int defining the end year of the snapshot (inclusive)
        year_type: str defining year the 'fy' for financial year or 'calendar'

    Raises: ValueError if the end of week falls outside after the year end i.e.
        for all weeks 53 or greater and for some years the week 52.
    """
    if year_type == "fy":
        start_year = start_year - 1
        end_year = end_year
        month = 7
    else:
        start_year = start_year
        end_year = end_year + 1
        month = 1

    snapshot = snapshot.index.to_series()

    filtered_snapshot = []

    for year in range(start_year, end_year + 1):
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

            filtered_snapshot.append(
                snapshot[
                    (snapshot > nth_week_start) & (snapshot <= nth_week_end)
                ].copy()
            )

    filtered_snapshot = pd.concat(filtered_snapshot)

    filtered_snapshot = pd.DataFrame(index=filtered_snapshot)

    return filtered_snapshot
