"""Re-sequences the timeslice patterns and maps them onto model snapshots.

The templated ``timeslices`` table gives one month-day window pattern per
reference (weather) year. This module assigns a reference year to each model
financial year using the configured reference_year_cycle — the identical
assignment the trace pipeline uses, so timeslices stay consistent with the
demand and VRE traces — expands the assigned patterns into absolute date
windows, and joins them onto the model's snapshots. The resulting mapping is
what pypsa_build uses to expand per-timeslice link limits into series and to
restrict custom constraints to the snapshots their RHS values apply to.
"""

import calendar
import logging

import pandas as pd
from isp_trace_parser import construct_reference_year_mapping

from ispypsa.config import ModelConfig

logger = logging.getLogger(__name__)

_TIMESLICE_SNAPSHOT_COLUMNS = ["timeslice_id", "investment_periods", "snapshots"]


def _create_timeslice_snapshot_mapping(
    timeslices: pd.DataFrame, snapshots: pd.DataFrame, config: ModelConfig
) -> pd.DataFrame:
    """Maps each snapshot to the timeslices active on its date under the
    configured reference_year_cycle.

    I/O Example:
        timeslices:
            timeslice_id     reference_year  start_month_day  end_month_day
            nsw_peak_demand  2018            01-13            01-14
            vic_peak_demand  2018            01-20            01-21  # no snapshots inside

        snapshots (config: start_year 2025, reference_year_cycle [2018]):
            investment_periods  snapshots
            2025                2025-01-13 12:00:00
            2025                2025-01-15 12:00:00

        returns:
            timeslice_id     investment_periods  snapshots
            nsw_peak_demand  2025                2025-01-13 12:00:00
    """
    if config.temporal.year_type != "fy":
        raise NotImplementedError(
            "Timeslice re-sequencing is only implemented for fy year_type; "
            "AEMO's timeslice calendar is built on financial years."
        )
    if timeslices.empty:
        return pd.DataFrame(columns=_TIMESLICE_SNAPSHOT_COLUMNS)
    reference_year_mapping = _map_model_years_to_reference_years(config)
    _raise_on_reference_years_without_patterns(reference_year_mapping, timeslices)
    windows = _expand_patterns_to_absolute_windows(timeslices, reference_year_mapping)
    mapped = [
        _snapshots_in_window(snapshots, window) for window in windows.itertuples()
    ]
    return _concat_window_snapshots(mapped)


def _map_model_years_to_reference_years(config: ModelConfig) -> dict[int, int]:
    """The model-year -> reference-year assignment, via the identical
    construct_reference_year_mapping call the trace pipeline makes, plus the
    year before the first model year (cycle-consistent: the cycle's last
    reference year precedes its first) so windows starting in the prior
    financial year — winter runs April to October — cover the first model
    year's July-September snapshots.

    I/O Example:
        start_year 2025, end_year 2027, reference_year_cycle [2011, 2018]
        -> {2024: 2018, 2025: 2011, 2026: 2018, 2027: 2011}
    """
    cycle = config.temporal.capacity_expansion.reference_year_cycle
    mapping = construct_reference_year_mapping(
        start_year=config.temporal.range.start_year,
        end_year=config.temporal.range.end_year,
        reference_years=cycle,
    )
    return {config.temporal.range.start_year - 1: cycle[-1], **mapping}


def _raise_on_reference_years_without_patterns(
    reference_year_mapping: dict[int, int], timeslices: pd.DataFrame
) -> None:
    """Raise if the configured cycle uses reference years the timeslices
    table has no patterns for — silently producing no windows would let
    timeslice-tagged limits and constraints never bind."""
    missing = sorted(
        set(reference_year_mapping.values()) - set(timeslices["reference_year"])
    )
    if missing:
        raise ValueError(
            f"Configured reference_year_cycle includes reference years with "
            f"no timeslice window patterns: {missing}"
        )


def _expand_patterns_to_absolute_windows(
    timeslices: pd.DataFrame, reference_year_mapping: dict[int, int]
) -> pd.DataFrame:
    """Expands each model year's assigned pattern into absolute date windows.

    I/O Example:
        timeslices:
            timeslice_id     reference_year  start_month_day  end_month_day
            nsw_peak_demand  2018            11-18            11-20

        reference_year_mapping {2026: 2018} ->
            timeslice_id     start_date  end_date
            nsw_peak_demand  2025-11-18  2025-11-20
    """
    expanded = []
    for model_year, reference_year in reference_year_mapping.items():
        pattern = timeslices[timeslices["reference_year"] == reference_year]
        expanded.append(_place_pattern_in_financial_year(pattern, model_year))
    return pd.concat(expanded, ignore_index=True)


def _place_pattern_in_financial_year(
    pattern: pd.DataFrame, model_year: int
) -> pd.DataFrame:
    """Turns one pattern's month-day windows into absolute dates in the
    financial year ending model_year: starts in July-December land in
    model_year - 1, January-June in model_year, and each end is its first
    occurrence after the start (ends may fall past 30 June, e.g. winter
    April-October).

    I/O Example (model_year=2026):
        timeslice_id          start_month_day  end_month_day
        nsw_peak_demand       11-18            11-20
        nsw_summer_typical    11-20            03-20  # end wraps past new year
        nsw_winter_reference  04-01            10-01  # end past 30 June

        returns:
            timeslice_id          start_date  end_date
            nsw_peak_demand       2025-11-18  2025-11-20
            nsw_summer_typical    2025-11-20  2026-03-20
            nsw_winter_reference  2026-04-01  2026-10-01
    """
    placed = pattern.copy()
    placed["start_date"] = placed["start_month_day"].apply(
        lambda month_day: _place_start_in_financial_year(month_day, model_year)
    )
    placed["end_date"] = placed.apply(
        lambda row: _resolve_end_date(
            row["end_month_day"], row["start_month_day"], row["start_date"]
        ),
        axis=1,
    )
    return placed[["timeslice_id", "start_date", "end_date"]]


def _place_start_in_financial_year(month_day: str, model_year: int) -> pd.Timestamp:
    """'11-18' in FY2026 -> 2025-11-18; '04-01' in FY2026 -> 2026-04-01."""
    month = int(month_day.split("-")[0])
    year = model_year - 1 if month >= 7 else model_year
    return _timestamp_with_leap_day_clamp(year, month_day)


def _resolve_end_date(
    end_month_day: str, start_month_day: str, start: pd.Timestamp
) -> pd.Timestamp:
    """An end later in the calendar year than the start lands in the start's
    year; otherwise it wraps into the next. The comparison uses the original
    month-day strings (which compare lexically in chronological order)
    rather than the placed dates, so leap-day clamping a one-day 02-28 to
    02-29 window in a non-leap year collapses it to an empty window instead
    of wrapping its end a year out.

    I/O Example:
        ('03-20', '11-18', 2025-11-18) -> 2026-03-20  # wraps
        ('10-01', '04-01', 2026-04-01) -> 2026-10-01
        ('02-29', '02-28', 2025-02-28) -> 2025-02-28  # clamped: empty window
    """
    year = start.year if end_month_day > start_month_day else start.year + 1
    return _timestamp_with_leap_day_clamp(year, end_month_day)


def _timestamp_with_leap_day_clamp(year: int, month_day: str) -> pd.Timestamp:
    """'02-29' is clamped to 28 February in non-leap years — patterns from
    leap reference years land in non-leap model years under re-sequencing.

    I/O Example:
        (2025, '02-29') -> 2025-02-28; (2024, '02-29') -> 2024-02-29
    """
    month, day = (int(part) for part in month_day.split("-"))
    if (month, day) == (2, 29) and not calendar.isleap(year):
        day = 28
    return pd.Timestamp(year=year, month=month, day=day)


def _snapshots_in_window(snapshots: pd.DataFrame, window) -> pd.DataFrame:
    """Selects the snapshots inside one window, tagged with its timeslice.

    I/O Example:
        snapshots 2025-01-13 12:00 and 2025-01-15 12:00,
        window nsw_peak_demand [2025-01-13, 2025-01-14)
        -> the 2025-01-13 12:00 snapshot tagged nsw_peak_demand
    """
    in_window = (snapshots["snapshots"] >= window.start_date) & (
        snapshots["snapshots"] < window.end_date
    )
    tagged = snapshots.loc[in_window, ["investment_periods", "snapshots"]].copy()
    tagged["timeslice_id"] = window.timeslice_id
    return tagged


def _concat_window_snapshots(mapped: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines the per-window snapshot selections into one mapping table."""
    if not mapped:
        return pd.DataFrame(columns=_TIMESLICE_SNAPSHOT_COLUMNS)
    mapping = pd.concat(mapped, ignore_index=True)
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
