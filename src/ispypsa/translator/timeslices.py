import logging

import pandas as pd

logger = logging.getLogger(__name__)

_TIMESLICE_SNAPSHOT_COLUMNS = ["timeslice", "investment_periods", "snapshots"]


def _create_timeslice_snapshot_mapping(
    timeslices: pd.DataFrame,
    snapshots: pd.DataFrame,
    reference_year_mapping: dict[int, int],
    year_type: str,
) -> pd.DataFrame:
    """Maps each snapshot to the timeslice active on its date, using the
    window pattern of the reference year assigned to the snapshot's model
    year.

    Mapping is done on a model year by model year basis:
        - Each model year is mapped to a reference year.
        - Then the timeslice windows for that reference year are used to map each
          snapshot to a timeslice.

    I/O Example:
        timeslices (each year's windows tile (cover in full) its financial year:
        summer opens in November and wraps past New Year, a peak day interrupts it,
        summer resumes to April, then winter runs April to November, crossing 30
        June into the next financial year):
            timeslice             reference_year  start_month_day  end_month_day
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

        reference_year_mapping {2025: 2011, 2026: 2018}, year_type "fy"
            -> FY2025 uses 2011's pattern, FY2026 uses 2018's

        returns:
            timeslice             investment_periods  snapshots
            nsw_winter_reference  2025                2024-08-15 12:00:00
            nsw_summer_typical    2025                2025-01-20 12:00:00
            nsw_peak_demand       2025                2025-01-31 12:00:00
            nsw_peak_demand       2026                2026-01-07 12:00:00
            nsw_summer_typical    2026                2026-01-31 12:00:00
    """
    snapshots = _add_interval_model_year_and_month_day(snapshots, year_type)
    mapped = [
        _tag_snapshots_with_pattern(
            snapshots[snapshots["model_year"] == model_year],
            timeslices[timeslices["reference_year"] == reference_year],
        )
        for model_year, reference_year in reference_year_mapping.items()
    ]
    return _concat_tagged_snapshots(mapped)


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
            timeslice             start_month_day  end_month_day
            nsw_summer_typical    11-01            01-07
            nsw_peak_demand       01-07            01-08
            nsw_summer_typical    01-08            04-01
            nsw_winter_reference  04-01            11-01

        returns:
            timeslice             investment_periods  snapshots
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
    in_plain_window = after_start & before_end
    in_wrapping_window = after_start | before_end
    wraps = end <= start
    return (~wraps & in_plain_window) | (wraps & in_wrapping_window)


def _concat_tagged_snapshots(mapped: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines the per-model-year tagged snapshots into one mapping table,
    in snapshot order."""
    mapping = pd.concat(mapped, ignore_index=True)
    mapping = mapping.sort_values(["snapshots", "timeslice"]).reset_index(drop=True)
    return mapping.loc[:, _TIMESLICE_SNAPSHOT_COLUMNS]


def _log_referenced_timeslices_without_snapshots(
    timeslice_snapshots: pd.DataFrame,
    link_timeslice_limits: pd.DataFrame,
    custom_constraints_rhs: pd.DataFrame,
) -> None:
    """Logs the timeslices referenced by a limit or constraint but mapped to
    no snapshots where they would apply — those limits and constraints will
    never apply.

    This is expected when snapshot aggregation (e.g. representative weeks)
    selects no snapshots inside a timeslice's windows, and for calendar
    timeslices that never activate (tas_peak_demand in the Draft 2026 ISP
    calendar), but the user should know the affected inputs will not bind.

    Transmission limits apply in every investment period, so they are checked
    against the model as a whole. Custom constraint rows each apply in one
    investment period, so they are checked period by period: a short
    timeslice like qld_peak_demand can be caught by the representative weeks
    in 2025 but missed in 2030.
    """
    _log_link_timeslices_without_snapshots(timeslice_snapshots, link_timeslice_limits)
    _log_constraint_timeslices_without_snapshots_in_period(
        timeslice_snapshots, custom_constraints_rhs
    )


def _log_link_timeslices_without_snapshots(
    timeslice_snapshots: pd.DataFrame, link_timeslice_limits: pd.DataFrame
) -> None:
    """Logs the named timeslices in link_timeslice_limits with no snapshots in
    any investment period. Fallback rows (blank timeslice) aren't checked.

    I/O Example:
        timeslice_snapshots:
            timeslice        investment_periods  snapshots
            nsw_peak_demand  2026                2026-01-13 12:00:00

        link_timeslice_limits:
            name            attribute  timeslice        value
            CQ-NQ_existing  p_max_pu   nsw_peak_demand  0.8
            CQ-NQ_existing  p_max_pu   tas_peak_demand  0.9   # never mapped: logged
            CQ-NQ_existing  p_max_pu   ,                1.0   # fallback: not checked

        logs: [...] will never apply): ['tas_peak_demand']
    """
    referenced = set(link_timeslice_limits["timeslice"].dropna())
    without_snapshots = referenced - set(timeslice_snapshots["timeslice"])
    if without_snapshots:
        logger.warning(
            f"Timeslices referenced by transmission limits but with no snapshots "
            f"in the model (these limits will never apply): "
            f"{sorted(without_snapshots)}"
        )


def _log_constraint_timeslices_without_snapshots_in_period(
    timeslice_snapshots: pd.DataFrame, custom_constraints_rhs: pd.DataFrame
) -> None:
    """Logs each (timeslice, investment_period) pair named in
    custom_constraints_rhs with no snapshots in timeslice_snapshots. Fallback
    rows (blank timeslice) aren't checked; a named timeslice always has an
    investment_period (custom_constraints_rhs schema).

    I/O Example:
        timeslice_snapshots:
            timeslice        investment_periods  snapshots
            qld_peak_demand  2025                2025-01-31 12:00:00

        custom_constraints_rhs:
            constraint_name  investment_period  timeslice
            SWQLD1           2025               qld_peak_demand
            SWQLD1           2030               qld_peak_demand   # not in 2030: logged
            SWQLD1           2030               ,                 # fallback: not checked

        logs: [...] will never apply): [('qld_peak_demand', 2030)]
    """
    named = custom_constraints_rhs.dropna(subset=["timeslice"])
    referenced = set(
        zip(named["timeslice"], named["investment_period"].astype(int).tolist())
    )
    mapped = set(
        zip(
            timeslice_snapshots["timeslice"],
            timeslice_snapshots["investment_periods"].tolist(),
        )
    )
    without_snapshots = referenced - mapped
    if without_snapshots:
        logger.warning(
            f"Timeslices referenced by custom constraints but with no snapshots in "
            f"the constraint's investment period (these constraints will never "
            f"apply): {sorted(without_snapshots)}"
        )
