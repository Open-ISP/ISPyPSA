from datetime import datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd

from ispypsa.config import (
    TemporalAggregationConfig,
    TemporalRangeConfig,
)
from ispypsa.translator.helpers import _get_iteration_start_and_end_time


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


def _filter_snapshots(
    year_type: Literal["fy", "calendar"],
    temporal_range: TemporalRangeConfig,
    temporal_aggregation_config: TemporalAggregationConfig,
    snapshots: pd.DataFrame,
    existing_generators: pd.DataFrame | None = None,
    demand_traces: dict[str, pd.DataFrame] | None = None,
    generator_traces: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Appy filter to the snapshots based on the model config.

    - If config.representative_weeks is not None then filter the
      snapshots based on the supplied list of representative weeks.
    - If config.named_representative_weeks is not None then filter the
      snapshots based on the supplied list of named representative weeks.

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
         year_type: "fy" for financial year or "calendar" for calendar year
         temporal_range: TemporalRangeConfig with start and end years
         temporal_aggregation_config: TemporalAggregationConfig with filtering options
         snapshots: pd.DataFrame with datetime index containing the snapshot
         existing_generators: pd.DataFrame with generator data (optional, required for residual metrics)
         demand_traces: dict[str, pd.DataFrame] with demand traces, required for named_representative_weeks
         generator_traces: dict[str, pd.DataFrame] with generator traces, required for residual metrics
    """
    filtered_snapshots = []

    if temporal_aggregation_config.representative_weeks is not None:
        representative_snapshots = _filter_snapshots_for_representative_weeks(
            representative_weeks=temporal_aggregation_config.representative_weeks,
            snapshots=snapshots,
            start_year=temporal_range.start_year,
            end_year=temporal_range.end_year,
            year_type=year_type,
        )
        filtered_snapshots.append(representative_snapshots)

    if (
        hasattr(temporal_aggregation_config, "named_representative_weeks")
        and temporal_aggregation_config.named_representative_weeks is not None
    ):
        # Prepare time series data for named weeks filtering
        demand_data, renewable_data = _prepare_data_for_named_weeks(
            temporal_aggregation_config.named_representative_weeks,
            existing_generators,
            demand_traces,
            generator_traces,
        )

        named_snapshots = _filter_snapshots_for_named_representative_weeks(
            named_representative_weeks=temporal_aggregation_config.named_representative_weeks,
            snapshots=snapshots,
            start_year=temporal_range.start_year,
            end_year=temporal_range.end_year,
            year_type=year_type,
            demand_data=demand_data,
            renewable_data=renewable_data,
        )
        filtered_snapshots.append(named_snapshots)

    if filtered_snapshots:
        # Combine all filtered snapshots and drop duplicates
        combined_snapshots = pd.concat(filtered_snapshots, ignore_index=True)
        combined_snapshots = (
            combined_snapshots.drop_duplicates(subset=["snapshots"])
            .sort_values("snapshots")
            .reset_index(drop=True)
        )
        return combined_snapshots

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


def _filter_snapshots_for_named_representative_weeks(
    named_representative_weeks: list[str],
    snapshots: pd.DataFrame,
    start_year: int,
    end_year: int,
    year_type: str,
    demand_data: pd.DataFrame | None = None,
    renewable_data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Filters snapshots to include specific named representative weeks based on demand criteria.

    Named week types:
    - "peak-demand": Week with highest instantaneous demand
    - "residual-peak-demand": Week with highest residual demand (demand - renewable)
    - "minimum-demand": Week with lowest instantaneous demand
    - "residual-minimum-demand": Week with lowest residual demand
    - "peak-consumption": Week with highest total energy consumption
    - "residual-peak-consumption": Week with highest residual energy consumption

    Week Assignment Behavior:
    - Weeks are defined as Monday 00:00:00 to Monday 00:00:00 (7 days)
    - Monday 00:00:00 timestamps mark the END of a week, not the beginning
    - Weeks are assigned to the year/financial year in which they END
    - Weeks spanning year boundaries (e.g., Dec 28 2024 to Jan 4 2025)
      are EXCLUDED from the analysis

    Examples:

    >>> # Create sample data for 2024
    >>> snapshots = pd.DataFrame({
    ...     'snapshots': pd.date_range('2024-01-01', '2024-01-31', freq='D')
    ... })
    >>> demand_data = pd.DataFrame({
    ...     'Datetime': pd.date_range('2024-01-01', '2024-01-31', freq='D'),
    ...     'Value': [100, 110, 120, 130, 140, 150, 160,  # Week 1 (Jan 1-7)
    ...               170, 300, 250, 200, 180, 160, 140,  # Week 2 (Jan 8-14) - peak
    ...               120, 110, 100, 90, 80, 70, 60,      # Week 3 (Jan 15-21) - minimum
    ...               50, 60, 70, 80, 90, 100, 110,       # Week 4 (Jan 22-28)
    ...               120, 130, 140]                       # Partial week
    ... })

    >>> # Filter for peak demand week
    >>> result = _filter_snapshots_for_named_representative_weeks(
    ...     named_representative_weeks=["peak-demand"],
    ...     snapshots=snapshots,
    ...     start_year=2024,
    ...     end_year=2025,
    ...     year_type="calendar",
    ...     demand_data=demand_data
    ... )
    >>> # Returns snapshots from week containing Jan 9 (highest demand of 300)
    >>> # This is the week from Jan 9-15 (Tuesday to Monday)
    >>> len(result)
    7

    >>> # Filter for minimum demand week
    >>> result = _filter_snapshots_for_named_representative_weeks(
    ...     named_representative_weeks=["minimum-demand"],
    ...     snapshots=snapshots,
    ...     start_year=2024,
    ...     end_year=2025,
    ...     year_type="calendar",
    ...     demand_data=demand_data
    ... )
    >>> # Returns snapshots from week containing Jan 22 (lowest demand of 50)
    >>> # This is the week from Jan 16-22 (Tuesday to Monday)
    >>> len(result)
    7

    Args:
        named_representative_weeks: List of named week types to include
        snapshots: DataFrame with snapshots column containing datetime values
        start_year: First year to process
        end_year: Last year to process (inclusive)
        year_type: "fy" for financial year or "calendar" for calendar year
        demand_data: DataFrame with demand time series (optional)
        renewable_data: DataFrame with combined wind+solar generation time series (optional)

    Returns:
        DataFrame with filtered snapshots
    """
    start_year, end_year, month = _get_iteration_start_and_end_time(
        year_type, start_year, end_year
    )

    # Prepare demand data with residual if needed
    demand_df = _prepare_demand_with_residual(
        demand_data, renewable_data, named_representative_weeks
    )

    # Filter to time range and assign weekly structure
    demand_df = _filter_and_assign_weeks(demand_df, start_year, end_year, month)

    # Calculate metrics for each week
    week_metrics = _calculate_week_metrics(demand_df)

    # Find target weeks based on named criteria
    target_weeks = _find_target_weeks(week_metrics, named_representative_weeks)

    # Extract snapshots for target weeks
    return _extract_snapshots_for_weeks(snapshots["snapshots"], target_weeks)


def _prepare_demand_with_residual(
    demand_data: pd.DataFrame,
    renewable_data: pd.DataFrame | None,
    named_representative_weeks: list[str],
) -> pd.DataFrame:
    """Prepare demand data with residual demand column if needed."""
    df = demand_data.rename(columns={"Value": "demand"})

    needs_residual = any("residual" in metric for metric in named_representative_weeks)

    if renewable_data is not None:
        df = df.merge(
            renewable_data.rename(columns={"Value": "renewable"}),
            on="Datetime",
            how="left",
        )
        df["residual_demand"] = df["demand"] - df["renewable"].fillna(0)
    elif needs_residual:
        df["residual_demand"] = df["demand"]

    return df


def _filter_and_assign_weeks(
    demand_df: pd.DataFrame,
    start_year: int,
    end_year: int,
    month: int,
) -> pd.DataFrame:
    """Filter demand data to time range and assign week structure."""
    # Create year boundaries
    year_starts = pd.to_datetime(
        [datetime(y, month, 1) for y in range(start_year, end_year)]
    )
    year_ends = pd.to_datetime(
        [datetime(y + 1, month, 1) for y in range(start_year, end_year)]
    )

    output = []

    for year_start, year_end in zip(year_starts, year_ends):
        df = demand_df[
            (demand_df["Datetime"] > year_start) & (demand_df["Datetime"] <= year_end)
        ].copy()

        if month == 1:
            df["year"] = year_start.year
        else:
            df["year"] = year_end.year

        days_until_next_monday = (7 - df["Datetime"].dt.weekday) % 7
        days_until_next_monday = days_until_next_monday.where(
            days_until_next_monday != 0, 7
        )

        already_week_end_time = (
            (df["Datetime"].dt.weekday == 0)
            & (df["Datetime"].dt.hour == 0)
            & (df["Datetime"].dt.minute == 0)
        )

        df["week_end_time"] = np.where(
            already_week_end_time,
            df["Datetime"],
            df["Datetime"] + pd.to_timedelta(days_until_next_monday, unit="days"),
        )

        # round back to midnight
        df["week_end_time"] = df["week_end_time"].dt.normalize()

        # Filter out partial weeks.
        df = df[df["week_end_time"] <= year_end]
        df = df[df["week_end_time"] - timedelta(days=7) >= year_start]

        output.append(df.copy())

    return pd.concat(output)


def _calculate_week_metrics(demand_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate metrics for each week across all years.

    Only processes complete weeks (those with 7 full days of data).
    """
    # Calculate metrics for all weeks
    agg_dict = {"demand": ["max", "min", "mean"]}

    if "residual_demand" in demand_df.columns:
        agg_dict["residual_demand"] = ["max", "min", "mean"]

    metrics = demand_df.groupby(["year", "week_end_time"]).agg(agg_dict)
    metrics.columns = ["_".join(col).strip("_") for col in metrics.columns]

    return metrics.reset_index()


def _find_target_weeks(
    week_metrics: pd.DataFrame,
    named_representative_weeks: list[str],
) -> list[pd.Timestamp]:
    """Find target weeks based on named criteria."""
    week_type_mapping = {
        "peak-demand": ("demand_max", "max"),
        "residual-peak-demand": ("residual_demand_max", "max"),
        "minimum-demand": ("demand_min", "min"),
        "residual-minimum-demand": ("residual_demand_min", "min"),
        "peak-consumption": ("demand_mean", "max"),
        "residual-peak-consumption": ("residual_demand_mean", "max"),
    }

    target_weeks = []

    for week_type in named_representative_weeks:
        metric_col, selection = week_type_mapping[week_type]

        if selection == "max":
            idx = week_metrics.groupby("year")[metric_col].idxmax()
        else:
            idx = week_metrics.groupby("year")[metric_col].idxmin()

        target_weeks.extend(week_metrics.loc[idx, "week_end_time"])

    return target_weeks


def _extract_snapshots_for_weeks(
    snapshot_series: pd.Series,
    target_weeks: list[pd.Timestamp],
) -> pd.DataFrame:
    """Extract snapshots that fall within target weeks.

    Week runs from Monday 00:00:01 to Monday 00:00:00 (next week).
    Monday 00:00:00 belongs to the previous week.
    """
    mask = pd.concat(
        [
            (snapshot_series > week - timedelta(days=7)) & (snapshot_series <= week)
            for week in target_weeks
        ],
        axis=1,
    ).any(axis=1)

    return pd.DataFrame({"snapshots": snapshot_series[mask]})


def _prepare_data_for_named_weeks(
    named_representative_weeks: list[str],
    existing_generators: pd.DataFrame | None,
    demand_traces: dict[str, pd.DataFrame] | None,
    generator_traces: dict[str, pd.DataFrame] | None,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Prepare time series data needed for named representative weeks filtering.

    Args:
        named_representative_weeks: List of named week types
        existing_generators: DataFrame with generator data
        demand_traces: Dictionary of demand traces
        generator_traces: Dictionary of generator traces

    Returns:
        Tuple of (demand_data, renewable_data) DataFrames or None values
    """
    # Check if any residual metrics are requested
    residual_metrics = [
        "residual-peak-demand",
        "residual-minimum-demand",
        "residual-peak-consumption",
        "residual-minimum-consumption",
    ]
    needs_generation_data = any(
        metric in named_representative_weeks for metric in residual_metrics
    )

    # Aggregate demand data from all nodes
    demand_data = _aggregate_demand_traces(demand_traces)

    # Prepare renewable (wind+solar) data only if residual metrics are requested
    renewable_data = None
    if needs_generation_data:
        if existing_generators is None or generator_traces is None:
            raise ValueError(
                "existing_generators table and generator_traces must be provided when using "
                "named_representative_weeks with residual metrics"
            )
        renewable_data = _aggregate_wind_solar_traces(
            generator_traces, existing_generators
        )

    return demand_data, renewable_data


def _aggregate_demand_traces(
    demand_traces: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Aggregate demand time series data from all nodes.

    Args:
        demand_traces: Dictionary with node names as keys and demand traces as values

    Returns:
        DataFrame with columns: Datetime, Value (total demand in MW)
    """
    # Combine all demand data
    all_demand_data = list(demand_traces.values())
    combined_demand = pd.concat(all_demand_data)
    aggregated_demand = combined_demand.groupby("Datetime")["Value"].sum().reset_index()

    return aggregated_demand


def _aggregate_wind_solar_traces(
    generator_traces: dict[str, pd.DataFrame],
    existing_generators: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate wind and solar generation time series data together.

    Args:
        generator_traces: Dictionary with generator names as keys and traces as values
        existing_generators: DataFrame with generator data including fuel_type and reg_cap

    Returns:
        DataFrame with columns: Datetime, Value (total wind+solar MW)
    """
    # Create mapping of generator name to fuel type and capacity
    gen_info = existing_generators.set_index("generator")[
        ["fuel_type", "reg_cap"]
    ].to_dict("index")

    # Collect all wind and solar data
    renewable_data = []

    for gen_name, trace in generator_traces.items():
        if gen_name not in gen_info:
            continue

        gen_fuel_type = gen_info[gen_name]["fuel_type"]
        gen_capacity = gen_info[gen_name]["reg_cap"]

        # Only process wind and solar generators
        if "Wind" in gen_fuel_type or "Solar" in gen_fuel_type:
            # Convert from per-unit to MW
            trace_mw = trace.copy()
            trace_mw["Value"] = trace_mw["Value"] * gen_capacity
            renewable_data.append(trace_mw)

    # Aggregate all renewable data
    if renewable_data:
        renewable_combined = pd.concat(renewable_data)
        renewable_aggregated = (
            renewable_combined.groupby("Datetime")["Value"].sum().reset_index()
        )
    else:
        # Return empty dataframe with proper structure
        renewable_aggregated = pd.DataFrame(
            {"Datetime": pd.to_datetime([]), "Value": []}
        )

    return renewable_aggregated
