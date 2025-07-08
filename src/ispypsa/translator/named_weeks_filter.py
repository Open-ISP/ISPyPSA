from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import get_data

from ispypsa.translator.helpers import _get_iteration_start_and_end_time

# Named week types and their corresponding metric/aggregation
WEEK_METRICS = {
    "peak-demand-week": ("total", "max"),
    "minimum-demand-week": ("total", "min"),
    "peak-consumption-week": ("mean", "max"),
    "residual-peak-demand-week": ("total", "max"),
    "residual-minimum-demand-week": ("total", "min"),
    "residual-peak-consumption-week": ("mean", "max"),
}


def _filter_snapshots_for_named_weeks(
    named_weeks: list[str],
    snapshots: pd.DataFrame,
    isp_sub_regions: pd.DataFrame,
    trace_data_path: Path | str,
    scenario: str,
    regional_granularity: str,
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
    start_year: int,
    end_year: int,
    existing_generators: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Filters snapshots based on named week criteria, selecting weeks per year.

    Args:
        named_weeks: List of named week types to include
                - "peak-demand-week"
                - "minimum-demand-week"
                - "peak-consumption-week"
                - "residual-peak-demand-week"
                - "residual-minimum-demand-week"
                - "residual-peak-consumption-week"
        snapshots: DataFrame with 'snapshots' column containing timestamps
        isp_sub_regions:
        trace_data_path:
        scenario:
        regional_granularity:
        reference_year_mapping:
        year_type: 'fy' or 'calendar'
        start_year: Start year of analysis
        end_year: End year of analysis (inclusive)
        existing_generators:

    Returns:
        DataFrame with filtered snapshots

    """
    # Validate named weeks first (before loading any data)
    _validate_named_weeks(named_weeks, existing_generators)

    # Get demand traces
    demand_traces = get_aggregated_demand_traces(
        isp_sub_regions=isp_sub_regions,
        trace_data_path=trace_data_path,
        scenario=scenario,
        regional_granularity=regional_granularity,
        reference_year_mapping=reference_year_mapping,
        year_type=year_type,
    )

    # Get renewable traces if needed
    renewable_traces = None
    residual_weeks = [w for w in named_weeks if w.startswith("residual-")]
    if residual_weeks:
        renewable_traces = get_renewable_generation_traces(
            existing_generators=existing_generators,
            trace_data_path=trace_data_path,
            scenario=scenario,
            reference_year_mapping=reference_year_mapping,
            year_type=year_type,
        )

    # Use the new implementation
    return _filter_snapshots_by_named_weeks_impl(
        named_weeks=named_weeks,
        snapshots=snapshots,
        demand_traces=demand_traces,
        year_type=year_type,
        start_year=start_year,
        end_year=end_year,
        renewable_traces=renewable_traces,
    )


def _filter_snapshots_by_named_weeks_impl(
    named_weeks: list[str],
    snapshots: pd.DataFrame,
    demand_traces: pd.DataFrame,
    year_type: Literal["fy", "calendar"],
    start_year: int,
    end_year: int,
    renewable_traces: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Filter snapshots to include only those in the specified named weeks.

    Args:
        named_weeks: List of named week types to include
        snapshots: DataFrame with 'snapshots' column containing timestamps
        demand_traces: DataFrame with 'Datetime' and 'Value' columns for demand data
        year_type: 'fy' or 'calendar'
        start_year: Start year of analysis
        end_year: End year of analysis (inclusive)
        renewable_traces: Optional DataFrame with renewable generation data

    Returns:
        DataFrame with filtered snapshots
    """
    if snapshots.empty or demand_traces.empty:
        return pd.DataFrame({"snapshots": []})

    # Calculate residual demand if needed
    traces_dict = _prepare_trace_data(named_weeks, demand_traces, renewable_traces)

    # Calculate week metrics for all trace types
    metrics_dict = {}
    for trace_type, trace_data in traces_dict.items():
        metrics = calculate_week_metrics_vectorized(
            trace_data, year_type, start_year, end_year
        )
        if not metrics.empty:
            metrics_dict[trace_type] = metrics

    # Identify the specific weeks based on criteria
    selected_weeks = _identify_weeks_by_criteria(named_weeks, metrics_dict)

    # Convert selected weeks to snapshot ranges
    snapshot_ranges = _weeks_to_snapshot_ranges(
        selected_weeks, year_type, start_year, end_year
    )

    # Filter snapshots
    return _filter_snapshots_by_ranges(snapshots, snapshot_ranges)


def _validate_named_weeks(
    named_weeks: list[str], existing_generators: pd.DataFrame = None
):
    """Validate named weeks and check requirements."""
    unsupported = set(named_weeks) - set(WEEK_METRICS.keys())
    if unsupported:
        raise ValueError(
            f"Unsupported named weeks: {unsupported}. "
            f"Supported values: {list(WEEK_METRICS.keys())}"
        )

    residual_weeks = [w for w in named_weeks if w.startswith("residual-")]
    if residual_weeks and existing_generators is None:
        raise ValueError(
            "existing_generators DataFrame required for residual demand calculations"
        )


def _prepare_trace_data(
    named_weeks: list[str],
    demand_traces: pd.DataFrame,
    renewable_traces: pd.DataFrame = None,
) -> dict[str, pd.DataFrame]:
    """Prepare trace data for different week types."""
    traces = {"demand": demand_traces}

    # Only calculate residual if needed
    if (
        any(w.startswith("residual-") for w in named_weeks)
        and renewable_traces is not None
    ):
        residual = demand_traces.copy()
        # Ensure traces are aligned by Datetime
        merged = pd.merge(
            demand_traces[["Datetime", "Value"]],
            renewable_traces[["Datetime", "Value"]],
            on="Datetime",
            suffixes=("_demand", "_renewable"),
        )
        residual = pd.DataFrame(
            {
                "Datetime": merged["Datetime"],
                "Value": merged["Value_demand"] - merged["Value_renewable"],
            }
        )
        traces["residual"] = residual

    return traces


def calculate_week_metrics_vectorized(
    trace_data: pd.DataFrame,
    year_type: Literal["fy", "calendar"],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """
    Calculate weekly metrics using fully vectorized operations.

    This replaces the previous implementation with a more efficient approach.
    """
    if trace_data.empty:
        return pd.DataFrame()

    trace_data = trace_data.copy()
    trace_data["Datetime"] = pd.to_datetime(trace_data["Datetime"])

    # Add year and week assignments
    trace_data = _assign_year_week_vectorized(
        trace_data, year_type, start_year, end_year
    )

    if trace_data.empty:
        return pd.DataFrame()

    # Calculate all metrics at once using groupby
    metrics = (
        trace_data.groupby(["year", "week_of_year"])["Value"]
        .agg(
            [
                ("total", "sum"),
                ("mean", "mean"),
                ("max", "max"),
                ("min", "min"),
                ("count", "count"),
            ]
        )
        .reset_index()
    )

    # Add week start dates
    metrics["week_start"] = metrics.apply(
        lambda row: _get_week_start_date(row["year"], row["week_of_year"], year_type),
        axis=1,
    )

    return metrics


def _assign_year_week_vectorized(
    trace_data: pd.DataFrame,
    year_type: Literal["fy", "calendar"],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Assign year and week numbers using vectorized operations."""
    # Get year info
    _, _, month = _get_iteration_start_and_end_time(year_type, start_year, end_year)

    # Create year column based on year type
    if year_type == "fy" and month == 7:
        # Financial year: July-June, labeled by ending year
        trace_data["year"] = trace_data["Datetime"].apply(
            lambda dt: dt.year + 1 if dt.month >= 7 else dt.year
        )
    else:
        trace_data["year"] = trace_data["Datetime"].dt.year

    # Filter to valid years
    trace_data = trace_data[trace_data["year"].between(start_year, end_year)].copy()

    if trace_data.empty:
        return trace_data

    # Calculate week numbers for each year group
    def assign_weeks_for_year(year_data):
        year = year_data["year"].iloc[0]
        year_data = year_data.copy()

        # Get calendar year for date calculations
        calendar_year = year - 1 if year_type == "fy" and month == 7 else year

        # Find first Monday
        year_start = datetime(calendar_year, month, 1)
        days_to_monday = (7 - year_start.weekday()) % 7
        first_monday = year_start + timedelta(days=days_to_monday)

        # Calculate week numbers
        days_since_monday = (
            year_data["Datetime"] - first_monday
        ).dt.total_seconds() / 86400
        year_data["week_of_year"] = (days_since_monday // 7 + 1).astype("Int64")

        # Mark invalid weeks (before first Monday or incomplete at year end)
        year_data.loc[year_data["Datetime"] < first_monday, "week_of_year"] = pd.NA

        # Check year boundary
        year_end = datetime(calendar_year + 1, month, 1)
        year_data["week_end"] = year_data["Datetime"] + pd.Timedelta(days=7)
        year_data.loc[
            year_data.groupby("week_of_year")["week_end"].transform("max") > year_end,
            "week_of_year",
        ] = pd.NA

        return year_data

    # Apply week assignment to each year
    year_groups = []
    for year in trace_data["year"].unique():
        year_data = trace_data[trace_data["year"] == year]
        year_groups.append(assign_weeks_for_year(year_data))

    trace_data = (
        pd.concat(year_groups, ignore_index=True) if year_groups else pd.DataFrame()
    )

    # Clean up and return
    trace_data = trace_data.dropna(subset=["week_of_year"])
    trace_data["week_of_year"] = trace_data["week_of_year"].astype(int)
    trace_data = trace_data.drop(columns=["week_end"], errors="ignore")

    return trace_data


def _identify_weeks_by_criteria(
    named_weeks: list[str], metrics_dict: dict[str, pd.DataFrame]
) -> list[tuple[int, int]]:
    """Identify specific weeks based on named criteria."""
    selected_weeks = []

    for week_type in named_weeks:
        # Determine which trace data to use
        if week_type.startswith("residual-"):
            metrics = metrics_dict.get("residual")
            if metrics is None:
                continue
        else:
            metrics = metrics_dict.get("demand")
            if metrics is None:
                continue

        # Get metric and aggregation type
        metric_col, agg_type = WEEK_METRICS[week_type]

        # Find the week for each year
        for year in metrics["year"].unique():
            year_metrics = metrics[metrics["year"] == year]
            if year_metrics.empty:
                continue

            if agg_type == "max":
                selected_row = year_metrics.loc[year_metrics[metric_col].idxmax()]
            else:  # min
                selected_row = year_metrics.loc[year_metrics[metric_col].idxmin()]

            selected_weeks.append(
                (int(selected_row["year"]), int(selected_row["week_of_year"]))
            )

    # Remove duplicates while preserving order
    return list(dict.fromkeys(selected_weeks))


def _get_week_start_date(
    year: int, week_number: int, year_type: Literal["fy", "calendar"]
) -> datetime:
    """Get the start date of a specific week."""
    # Ensure inputs are integers
    year = int(year)
    week_number = int(week_number)

    _, _, month = _get_iteration_start_and_end_time(year_type, year, year)

    # Get calendar year
    calendar_year = year - 1 if year_type == "fy" and month == 7 else year

    # Find first Monday
    year_start = datetime(calendar_year, month, 1)
    days_to_monday = (7 - year_start.weekday()) % 7
    first_monday = year_start + timedelta(days=days_to_monday)

    # Calculate week start
    return first_monday + timedelta(weeks=week_number - 1)


def _weeks_to_snapshot_ranges(
    selected_weeks: list[tuple[int, int]],
    year_type: Literal["fy", "calendar"],
    start_year: int,
    end_year: int,
) -> list[tuple[datetime, datetime]]:
    """Convert week specifications to datetime ranges."""
    ranges = []

    for year, week_number in selected_weeks:
        week_start = _get_week_start_date(year, week_number, year_type)
        week_end = week_start + timedelta(days=7)
        ranges.append((week_start, week_end))

    return ranges


def _filter_snapshots_by_ranges(
    snapshots: pd.DataFrame, ranges: list[tuple[datetime, datetime]]
) -> pd.DataFrame:
    """Filter snapshots to include only those within the specified ranges."""
    if not ranges:
        return pd.DataFrame({"snapshots": []})

    snapshot_times = snapshots["snapshots"]

    # Create a mask for all ranges
    mask = pd.Series(False, index=snapshot_times.index)
    for start, end in ranges:
        mask |= (snapshot_times >= start) & (snapshot_times < end)

    filtered = snapshot_times[mask].copy()

    return pd.DataFrame({"snapshots": filtered}).reset_index(drop=True)


def get_aggregated_demand_traces(
    isp_sub_regions: pd.DataFrame,
    trace_data_path: Path | str,
    scenario: str,
    regional_granularity: str,
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
) -> pd.DataFrame:
    """Get demand traces aggregated to the specified regional level."""
    trace_data_path = Path(trace_data_path) / "demand"

    # Determine aggregation nodes
    if regional_granularity == "single_region":
        aggregation_col = "NEM"
    elif regional_granularity == "nem_regions":
        aggregation_col = isp_sub_regions["nem_region_id"]
    else:  # sub_regions
        aggregation_col = isp_sub_regions["isp_sub_region_id"]

    # Get traces for all sub-regions
    all_traces = []
    for sub_region in isp_sub_regions["isp_sub_region_id"].unique():
        trace = get_data.demand_multiple_reference_years(
            reference_years=reference_year_mapping,
            directory=trace_data_path,
            subregion=sub_region,
            scenario=scenario,
            year_type=year_type,
            demand_type="OPSO_MODELLING",
            poe="POE50",
        )
        all_traces.append(trace)

    # Aggregate
    combined = pd.concat(all_traces, ignore_index=True)
    return combined.groupby("Datetime", as_index=False)["Value"].sum()


def get_renewable_generation_traces(
    existing_generators: pd.DataFrame,
    trace_data_path: Path | str,
    scenario: str,
    reference_year_mapping: dict[int, int],
    year_type: Literal["fy", "calendar"],
) -> pd.DataFrame:
    """Get renewable generation traces for existing generators."""
    renewable_gens = existing_generators[
        existing_generators["technology_type_id"].isin(["Solar", "Wind"])
    ]

    if renewable_gens.empty:
        # Return zero generation for the time period
        return _create_zero_generation_trace(reference_year_mapping)

    all_traces = []

    # Process by technology type for efficiency
    for tech_type in ["Solar", "Wind"]:
        tech_gens = renewable_gens[renewable_gens["technology_type_id"] == tech_type]
        if tech_gens.empty:
            continue

        trace_path = Path(trace_data_path) / tech_type.lower()
        get_func = getattr(
            get_data, f"{tech_type.lower()}_project_multiple_reference_years"
        )

        for _, gen in tech_gens.iterrows():
            trace = get_func(
                reference_years=reference_year_mapping,
                directory=trace_path,
                project=gen["duid"],
                scenario=scenario,
                year_type=year_type,
            )
            # Scale by capacity
            trace["Value"] *= gen["reg_cap"]
            all_traces.append(trace)

    # Aggregate all generation
    combined = pd.concat(all_traces, ignore_index=True)
    return combined.groupby("Datetime", as_index=False)["Value"].sum()


def _create_zero_generation_trace(
    reference_year_mapping: dict[int, int],
) -> pd.DataFrame:
    """Create a zero generation trace for the specified time period."""
    min_year = min(reference_year_mapping.values())
    max_year = max(reference_year_mapping.values())

    date_range = pd.date_range(
        start=f"{min_year}-01-01", end=f"{max_year}-12-31 23:30:00", freq="30min"
    )

    return pd.DataFrame({"Datetime": date_range, "Value": 0.0})
