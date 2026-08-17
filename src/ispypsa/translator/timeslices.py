import logging

import pandas as pd
from isp_trace_parser import construct_reference_year_mapping

from ispypsa.config import ModelConfig

logger = logging.getLogger(__name__)

_TIMESLICE_SNAPSHOT_COLUMNS = ["timeslice_id", "investment_periods", "snapshots"]


def _create_timeslice_snapshot_mapping(
    timeslices: pd.DataFrame, snapshots: pd.DataFrame, config: ModelConfig
) -> pd.DataFrame:
    """Maps each snapshot to the timeslice active on its date, using the
    window pattern of the reference year the configured reference_year_cycle
    assigns to the snapshot's model year (the same assignment the demand and
    VRE traces use).

    I/O Example:
        timeslices (each year's windows tile (cover in full) its financial year:
        summer opens in November and wraps past New Year, a peak day interrupts it,
        summer resumes to April, then winter runs April to November, crossing 30
        June into the next financial year):
            timeslice_id          reference_year  start_month_day  end_month_day
            nsw_summer_typical    2011            11-01            01-31
            nsw_peak_demand       2011            01-31            02-02
            nsw_summer_typical    2011            02-02            04-01
            nsw_winter_reference  2011            04-01            11-01
            nsw_summer_typical    2018            11-01            01-07
            nsw_peak_demand       2018            01-07            01-08
            nsw_summer_typical    2018            01-08            04-01
            nsw_winter_reference  2018            04-01            11-01

        snapshots:
            investment_periods  snapshots
            2025                2024-08-15 12:00:00  # winter's tail at the FY start
            2025                2025-01-20 12:00:00
            2025                2025-01-31 12:00:00  # summer ended 01-31 00:00: peak
            2026                2026-01-07 12:00:00
            2026                2026-01-31 12:00:00  # 2011 peak dates; summer in 2018

        config:
            year_type fy, start_year 2025, end_year 2026,
            reference_year_cycle [2011, 2018]
            -> FY2025 uses 2011's pattern, FY2026 uses 2018's

        returns:
            timeslice_id          investment_periods  snapshots
            nsw_winter_reference  2025                2024-08-15 12:00:00
            nsw_summer_typical    2025                2025-01-20 12:00:00
            nsw_peak_demand       2025                2025-01-31 12:00:00
            nsw_peak_demand       2026                2026-01-07 12:00:00
            nsw_summer_typical    2026                2026-01-31 12:00:00
    """
    reference_year_mapping = _map_model_years_to_reference_years(config)
    snapshots = _add_interval_model_year_and_month_day(
        snapshots, config.temporal.year_type
    )
    mapped = [
        _tag_snapshots_with_pattern(
            snapshots[snapshots["model_year"] == model_year],
            timeslices[timeslices["reference_year"] == reference_year],
        )
        for model_year, reference_year in reference_year_mapping.items()
    ]
    return _concat_tagged_snapshots(mapped)


def _map_model_years_to_reference_years(config: ModelConfig) -> dict[int, int]:
    """The model-year -> reference-year assignment, via the identical
    construct_reference_year_mapping call the trace pipeline makes.

    I/O Example:
        start_year 2025, end_year 2027, reference_year_cycle [2011, 2018]
        -> {2025: 2011, 2026: 2018, 2027: 2011}
    """
    return construct_reference_year_mapping(
        start_year=config.temporal.range.start_year,
        end_year=config.temporal.range.end_year,
        reference_years=config.temporal.capacity_expansion.reference_year_cycle,
    )


def _add_interval_model_year_and_month_day(
    snapshots: pd.DataFrame, year_type: str
) -> pd.DataFrame:
    """Adds the model year and month-day for each snapshot. Snapshots are stamped
    with the interval's end time, so both are read off the interval's last instant, one
    second before the stamp: a stamp of exactly midnight belongs to the day (and year)
    just ended.

    I/O Example (year_type fy):
        investment_periods  snapshots
        2026                2025-07-01 00:00:00  # last interval of FY2025
        2026                2025-11-01 00:00:00  # last interval of 31 October
        2026                2025-11-01 00:30:00

        ->
        investment_periods  snapshots            model_year  month_day
        2026                2025-07-01 00:00:00  2025        06-30
        2026                2025-11-01 00:00:00  2026        10-31
        2026                2025-11-01 00:30:00  2026        11-01
    """
    last_instant = snapshots["snapshots"] - pd.Timedelta(seconds=1)
    snapshots = snapshots.copy()
    snapshots["model_year"] = last_instant.dt.year
    if year_type == "fy":
        snapshots["model_year"] += (last_instant.dt.month >= 7).astype(int)
    snapshots["month_day"] = last_instant.dt.strftime("%m-%d")
    return snapshots


def _tag_snapshots_with_pattern(
    snapshots: pd.DataFrame, pattern: pd.DataFrame
) -> pd.DataFrame:
    """Tags one model year's snapshots with the timeslice whose window their
    month-day falls in, by pairing every snapshot with every window and
    keeping the pairs where the month-day lies in the window. A pattern
    with no windows tags nothing.

    I/O Example:
        snapshots (FY2026, month_day already added):
            investment_periods  snapshots            month_day
            2026                2025-08-15 12:00:00  08-15
            2026                2026-01-07 12:00:00  01-07

        pattern (reference year 2018):
            timeslice_id          start_month_day  end_month_day
            nsw_summer_typical    11-01            01-07
            nsw_peak_demand       01-07            01-08
            nsw_summer_typical    01-08            04-01
            nsw_winter_reference  04-01            11-01

        returns:
            timeslice_id          investment_periods  snapshots
            nsw_winter_reference  2026                2025-08-15 12:00:00
            nsw_peak_demand       2026                2026-01-07 12:00:00
    """
    pairs = snapshots.merge(pattern, how="cross")
    in_window = _month_day_in_window(
        pairs["month_day"], pairs["start_month_day"], pairs["end_month_day"]
    )
    return pairs.loc[in_window, _TIMESLICE_SNAPSHOT_COLUMNS]


def _month_day_in_window(
    month_day: pd.Series, start: pd.Series, end: pd.Series
) -> pd.Series:
    """Element-wise: is month_day in the [start, end) month-day window? A
    window whose end is at or before its start wraps past New Year.

    I/O Example:
        month_day  start  end    ->
        12-15      11-01  01-07  True   # wraps: after start
        01-07      11-01  01-07  False  # equals end: excluded
        01-07      01-07  01-08  True
        08-15      04-01  11-01  True
    """
    after_start = month_day >= start
    before_end = month_day < end
    wraps = end <= start
    return (after_start | before_end).where(wraps, after_start & before_end)


def _concat_tagged_snapshots(mapped: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines the per-model-year tagged snapshots into one mapping table,
    in snapshot order."""
    mapping = pd.concat(mapped, ignore_index=True)
    mapping = mapping.sort_values(["snapshots", "timeslice_id"]).reset_index(drop=True)
    return mapping.loc[:, _TIMESLICE_SNAPSHOT_COLUMNS]


def _log_referenced_timeslices_without_snapshots(
    timeslice_snapshots: pd.DataFrame,
    link_timeslice_limits: pd.DataFrame,
    custom_constraints_rhs: pd.DataFrame,
) -> None:
    """Logs the timeslices referenced by a limit or constraint but mapped to
    no snapshots — those limits and constraints will never apply.

    This is expected when snapshot aggregation (e.g. representative weeks)
    selects no snapshots inside a timeslice's windows, and for calendar
    timeslices that never activate (tas_peak_demand in the Draft 2026 ISP
    calendar), but the user should know the affected inputs will not bind.
    """
    referenced = set(link_timeslice_limits["timeslice"]) | set(
        custom_constraints_rhs["timeslice"].dropna()
    )
    without_snapshots = referenced - set(timeslice_snapshots["timeslice_id"])
    if without_snapshots:
        logger.warning(
            f"Timeslices referenced by transmission limits or custom constraints "
            f"but with no snapshots in the model (these limits and constraints "
            f"will never apply): {sorted(without_snapshots)}"
        )
