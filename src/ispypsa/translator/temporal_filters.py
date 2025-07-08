from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.config import (
    ModelConfig,
    TemporalAggregationConfig,
    TemporalRangeConfig,
)
from ispypsa.config.validators import TemporalConfig
from ispypsa.translator.helpers import _get_iteration_start_and_end_time
from ispypsa.translator.named_weeks_filter import _filter_snapshots_for_named_weeks


def _time_series_filter(time_series_data: pd.DataFrame, snapshots: pd.DataFrame):
    """Filters a timeseries pandas DataFrame based using the datetime values in
     the snapshots index.

    Examples:

    >>> datetime_index = pd.date_range('2020-01-01', '2020-01-03', freq='h')
    >>> time_series_data = pd.DataFrame({'snapshots': datetime_index, 'p_set': range(len(datetime_index))})
    >>> snapshots = pd.DataFrame(index=datetime_index[::12])  # Every 12 hours
    >>> _time_series_filter(time_series_data, snapshots)
                  snapshots  p_set
    0  2020-01-01 00:00:00      0
    12 2020-01-01 12:00:00     12
    24 2020-01-02 00:00:00     24
    36 2020-01-02 12:00:00     36
    48 2020-01-03 00:00:00     48

    Args:
        time_series_data: pd.DataFrame with time series column called 'Datetime'
        snapshots: pd.DataFrame with datetime index

    """
    return time_series_data[time_series_data["snapshots"].isin(snapshots["snapshots"])]


def _merge_snapshot_dataframes(snapshot_dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge multiple snapshot DataFrames, removing duplicates and sorting by datetime.

    Args:
        snapshot_dataframes: List of DataFrames with 'snapshots' column containing datetimes

    Returns:
        pd.DataFrame: Merged DataFrame with unique snapshots sorted chronologically

    Examples:
        >>> import pandas as pd
        >>> df1 = pd.DataFrame({'snapshots': pd.to_datetime(['2024-01-01', '2024-01-02'])})
        >>> df2 = pd.DataFrame({'snapshots': pd.to_datetime(['2024-01-02', '2024-01-03'])})
        >>> result = _merge_snapshot_dataframes([df1, df2])
        >>> len(result)
        3
        >>> result['snapshots'].iloc[0]
        Timestamp('2024-01-01 00:00:00')
    """
    if not snapshot_dataframes:
        return pd.DataFrame({"snapshots": []})

    if len(snapshot_dataframes) == 1:
        return snapshot_dataframes[0]

    # Concatenate all snapshots
    merged_snapshots = pd.concat(snapshot_dataframes, ignore_index=True)
    # Remove duplicates and sort by snapshot datetime
    merged_snapshots = (
        merged_snapshots.drop_duplicates(subset=["snapshots"])
        .sort_values("snapshots")
        .reset_index(drop=True)
    )

    return merged_snapshots


def _filter_snapshots(
    year_type: Literal["fy", "calendar"],
    temporal_range: TemporalRangeConfig,
    temporal_aggregation_config: TemporalAggregationConfig,
    snapshots: pd.DataFrame,
    isp_sub_regions: pd.DataFrame = None,
    trace_data_path: Path | str = None,
    scenario: str = None,
    regional_granularity: str = None,
    reference_year_mapping: dict[int, int] = None,
    existing_generators: pd.DataFrame = None,
) -> pd.DataFrame:
    """Appy filter to the snapshots based on the model config.

    - If config.representative_weeks is not None then filter the
      snapshots based on the supplied list of representative weeks.
    - If config.named_representative_weeks is not None then analyze demand/generation
      data to identify and filter snapshots for the named weeks.
    - If both are specified, the results are merged with duplicates removed.

    Examples:

    # Create dummy config class with just data need for example.

    >>> from dataclasses import dataclass

    >>> @dataclass
    ... class TemporalAggregationConfig:
    ...     representative_weeks: list[int]

    >>> @dataclass
    ... class TemporalOperationalConfig:
    ...     aggregation: TemporalAggregationConfig

    >>> temporal_agg = TemporalAggregationConfig(
    ...     representative_weeks=[1],
    ... )

    >>> @dataclass
    ... class TemporalRangeConfig:
    ...     start_year: int
    ...     end_year: int

    >>> temporal_range = TemporalRangeConfig(
    ...     start_year=2024,
    ...     end_year=2024,
    ... )

    >>> snapshots = pd.DataFrame(
    ... {"snapshots": pd.date_range('2024-01-01', '2024-12-31', freq='h')}
    ... )

    >>> snapshots = _filter_snapshots(
    ...     "calendar",
    ...     temporal_range,
    ...     temporal_agg,
    ...     snapshots
    ...  )

    >>> snapshots["snapshots"].iloc[0]
    Timestamp('2024-01-01 01:00:00')

    >>> snapshots["snapshots"].iloc[-1]
    Timestamp('2024-01-08 00:00:00')

    Args:
         year_type: 'fy' or 'calendar' year type
         temporal_range: TemporalRangeConfig with start and end years
         temporal_aggregation_config: TemporalAggregationConfig defining snapshot filtering
         snapshots: pd.DataFrame with datetime index containing the snapshot
         isp_sub_regions: Optional DataFrame needed for named weeks analysis
         trace_data_path: Optional path to trace data needed for named weeks
         scenario: Optional ISP scenario needed for named weeks
         regional_granularity: Optional regional granularity for named weeks
         reference_year_mapping: Optional year mapping for named weeks
         existing_generators: Optional generators DataFrame for residual demand calculations
    """
    filtered_snapshots_list = []

    if temporal_aggregation_config.representative_weeks is not None:
        representative_snapshots = _filter_snapshots_for_representative_weeks(
            representative_weeks=temporal_aggregation_config.representative_weeks,
            snapshots=snapshots,
            start_year=temporal_range.start_year,
            end_year=temporal_range.end_year,
            year_type=year_type,
        )
        filtered_snapshots_list.append(representative_snapshots)

    if temporal_aggregation_config.named_representative_weeks is not None:
        # Filter snapshots based on named week criteria
        named_snapshots = _filter_snapshots_for_named_weeks(
            named_weeks=temporal_aggregation_config.named_representative_weeks,
            snapshots=snapshots,
            isp_sub_regions=isp_sub_regions,
            trace_data_path=trace_data_path,
            scenario=scenario,
            regional_granularity=regional_granularity,
            reference_year_mapping=reference_year_mapping,
            year_type=year_type,
            start_year=temporal_range.start_year,
            end_year=temporal_range.end_year,
            existing_generators=existing_generators,
        )
        filtered_snapshots_list.append(named_snapshots)

    # If we have filtered snapshots, merge them and remove duplicates
    if filtered_snapshots_list:
        return _merge_snapshot_dataframes(filtered_snapshots_list)

    # If no filtering was applied, return original snapshots
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
