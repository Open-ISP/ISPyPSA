"""Templates the per-reference-year timeslice window patterns.

AEMO's PLEXOS data package ships a timeslice calendar (e.g.
``timeslice_RefYear5000.csv``) of on/off events on absolute dates across the
model horizon. The dates follow AEMO's rolling reference-year sequence: each
planning financial year's windows are derived from the weather of the
reference year assigned to it by Table 1 of the Draft 2026 ISP Market Model
Instructions, with the 16-year sequence repeating until the end of the
horizon. The templater inverts this: it assigns each calendar window to its
planning year, joins on the manually extracted sequence table, checks every
occurrence of a reference year carries an identical month-day pattern, and
emits one window pattern per reference year. The translator re-sequences the
patterns to match the user-configured reference_year_cycle, keeping
timeslices consistent with the demand and VRE traces.
"""

from importlib.resources import files

import pandas as pd

from ispypsa.templater.custom_constraints_from_plexos import _tag_to_timeslice

_TIMESLICE_COLUMNS = [
    "timeslice_id",
    "reference_year",
    "start_month_day",
    "end_month_day",
]

# The PLEXOS timeslice calendar shipped for each supported workbook version.
# The "RefYearNNNN" suffix is AEMO's identifier for the reference-year
# sequence the calendar was built with.
_TIMESLICE_CALENDAR_FILENAMES = {"7.5": "timeslice_RefYear5000.csv"}


def load_timeslice_calendar(iasr_workbook_version: str) -> pd.DataFrame:
    """Loads the PLEXOS timeslice calendar shipped with ISPyPSA for the
    given IASR workbook version (from ``src/ispypsa/templater/plexos/``)."""
    extract_dir = files("ispypsa.templater") / "plexos" / iasr_workbook_version
    return pd.read_csv(
        extract_dir / _TIMESLICE_CALENDAR_FILENAMES[iasr_workbook_version]
    )


def _template_timeslices(
    timeslice_calendar: pd.DataFrame, reference_year_sequence: pd.DataFrame
) -> pd.DataFrame:
    """Decodes the PLEXOS on/off timeslice calendar into one month-day window
    pattern per reference year.

    The raw calendar has one row per on/off event: TIMESLICE == -1 turns the
    named timeslice on at DATETIME, TIMESLICE == 0 turns it off. Each on
    event is paired with the next event for the same timeslice to form one
    active window. Windows are grouped into the planning financial year they
    start in and labelled with that year's reference year from
    reference_year_sequence (extended cyclically past its last row, matching
    AEMO's repeating sequence). Every occurrence of a reference year must
    carry an identical pattern; the first occurrence is kept.

    Windows starting before the sequence's first planning year are dropped
    (the early calendar years precede AEMO's documented sequence), as are
    windows still open when the calendar ends (a horizon artifact).

    I/O Example:
        timeslice_calendar:
            DATETIME    NAME         TIMESLICE
            30/06/2025  NSW Hot Day  0           # off with no preceding on: ignored
            18/11/2025  NSW Hot Day  -1
            20/11/2025  NSW Hot Day  0
            18/11/2041  NSW Hot Day  -1          # 2041-42: sequence repeats from 2026
            20/11/2041  NSW Hot Day  0
            01/04/2058  NSW Winter   -1          # never turned off: dropped

        reference_year_sequence:
            planning_year  reference_year
            2026           2015

        returns:
            timeslice_id     reference_year  start_month_day  end_month_day
            nsw_peak_demand  2015            11-18            11-20  # end exclusive
    """
    events = _parse_calendar_events(timeslice_calendar)
    events = _add_next_event_columns(events)
    _raise_on_consecutive_on_events(events)
    windows = _assign_windows_to_planning_years(_extract_windows(events))
    windows = _drop_horizon_truncated_planning_years(windows)
    if windows.empty or reference_year_sequence.empty:
        return pd.DataFrame(columns=_TIMESLICE_COLUMNS)
    sequence = _extend_sequence_to_horizon(reference_year_sequence, windows)
    patterns = _convert_windows_to_month_days(
        windows.merge(sequence, on="planning_year")
    )
    _raise_on_inconsistent_reference_year_patterns(patterns)
    return _keep_first_occurrence_per_reference_year(patterns)


def _parse_calendar_events(calendar: pd.DataFrame) -> pd.DataFrame:
    """Maps calendar names to canonical timeslice ids and orders the events.

    I/O Example:
        DATETIME=18/11/2021, NAME="NSW Hot Day", TIMESLICE=-1
        -> DATETIME=2021-11-18, timeslice_id="nsw_peak_demand", TIMESLICE=-1
    """
    events = calendar.copy()
    events["DATETIME"] = pd.to_datetime(events["DATETIME"], dayfirst=True)
    events["timeslice_id"] = events["NAME"].map(_tag_to_timeslice)
    return events.sort_values(["timeslice_id", "DATETIME"])


def _add_next_event_columns(events: pd.DataFrame) -> pd.DataFrame:
    """Annotates each event with the date and state of the next event for the
    same timeslice (NaN/NaT on each timeslice's last event).

    I/O Example:
        timeslice_id     DATETIME    TIMESLICE
        nsw_peak_demand  2021-11-18  -1
        nsw_peak_demand  2021-11-20  0

        ->
        timeslice_id     DATETIME    TIMESLICE  next_date   next_state
        nsw_peak_demand  2021-11-18  -1         2021-11-20  0
        nsw_peak_demand  2021-11-20  0          NaT         NaN
    """
    grouped = events.groupby("timeslice_id")
    events["next_date"] = grouped["DATETIME"].shift(-1)
    events["next_state"] = grouped["TIMESLICE"].shift(-1)
    return events


def _raise_on_consecutive_on_events(events: pd.DataFrame) -> None:
    """Raise if any on event is directly followed by another on event for the
    same timeslice — pairing on events with the next event would silently
    produce wrong windows otherwise. An on event with no following event is
    fine: the calendar legitimately ends with windows still open (e.g. winter
    turning on at the horizon's end)."""
    on_events = events[events["TIMESLICE"] == -1]
    doubled = on_events[on_events["next_state"] == -1]
    if not doubled.empty:
        dates = sorted(doubled["DATETIME"].dt.strftime("%Y-%m-%d"))
        raise ValueError(
            f"Timeslice calendar has on events directly followed by another "
            f"on event for the same timeslice, starting at: {dates}"
        )


def _extract_windows(events: pd.DataFrame) -> pd.DataFrame:
    """Turns each on event into a window row ending at the paired off event.

    I/O Example:
        timeslice_id     DATETIME    TIMESLICE  next_date   next_state
        nsw_peak_demand  2021-11-18  -1         2021-11-20  0
        nsw_peak_demand  2021-11-20  0          NaT         NaN

        returns:
            timeslice_id     start_date  end_date
            nsw_peak_demand  2021-11-18  2021-11-20
    """
    windows = events[events["TIMESLICE"] == -1]
    windows = windows.rename(
        columns={"DATETIME": "start_date", "next_date": "end_date"}
    )
    return windows[["timeslice_id", "start_date", "end_date"]].reset_index(drop=True)


def _drop_horizon_truncated_planning_years(windows: pd.DataFrame) -> pd.DataFrame:
    """Drops every window in planning years where the calendar ends with a
    window still open (e.g. winter turning on just before the horizon's end
    and never turning off). Keeping the year's other windows would make its
    pattern incomplete and fail the consistency check against earlier, fully
    covered occurrences of the same reference year — which is also why
    nothing is lost by dropping the year entirely.

    I/O Example:
        timeslice_id          start_date  end_date    planning_year
        nsw_peak_demand       2057-11-18  2057-11-20  2058           # dropped: shares
        nsw_winter_reference  2058-04-01  NaT         2058           # the truncated year
        nsw_peak_demand       2056-11-18  2056-11-20  2057           # kept

        returns the 2057 row only.
    """
    truncated_years = set(windows.loc[windows["end_date"].isna(), "planning_year"])
    keep = ~windows["planning_year"].isin(truncated_years)
    return windows[keep].reset_index(drop=True)


def _assign_windows_to_planning_years(windows: pd.DataFrame) -> pd.DataFrame:
    """Tags each window with the planning financial year it starts in
    (year-ending convention: FY2026 spans July 2025 to June 2026).

    I/O Example:
        start_date 2025-11-16 -> planning_year 2026
        start_date 2026-04-01 -> planning_year 2026
    """
    starts = windows["start_date"]
    windows["planning_year"] = starts.dt.year + (starts.dt.month >= 7).astype(int)
    return windows


def _extend_sequence_to_horizon(
    sequence: pd.DataFrame, windows: pd.DataFrame
) -> pd.DataFrame:
    """Cyclically repeats the documented reference-year sequence to cover
    every planning year in the calendar — AEMO: "the 16-year sequence
    repeating in the same order until the end of the outlook period". Only
    the windows' latest planning_year is used; if the windows end before the
    sequence does, the sequence is returned unshortened.

    I/O Example:
        sequence:
            planning_year  reference_year
            2026           2015
            2027           2011

        windows (only planning_year is read):
            timeslice_id     start_date  end_date    planning_year
            nsw_peak_demand  2028-11-18  2028-11-20  2029

        returns:
            planning_year  reference_year
            2026           2015
            2027           2011
            2028           2015  # cycle repeats
            2029           2011
    """
    sequence = sequence.sort_values("planning_year").reset_index(drop=True)
    first = sequence["planning_year"].iloc[0]
    last = max(windows["planning_year"].max(), sequence["planning_year"].max())
    years = range(first, last + 1)
    reference_years = [
        sequence["reference_year"].iloc[(year - first) % len(sequence)]
        for year in years
    ]
    return pd.DataFrame({"planning_year": years, "reference_year": reference_years})


def _convert_windows_to_month_days(windows: pd.DataFrame) -> pd.DataFrame:
    """Expresses each window's dates as month-day strings. The end is
    exclusive and, when re-expanded into a financial year, is interpreted as
    its first occurrence after the start (so 11-18 -> 03-20 wraps into the
    next calendar year, and winter's 04-01 -> 10-01 extends past 30 June).

    I/O Example:
        timeslice_id     start_date  end_date    planning_year  reference_year
        nsw_peak_demand  2025-11-18  2025-11-20  2026           2015

        ->
        timeslice_id     reference_year  planning_year  start_month_day  end_month_day
        nsw_peak_demand  2015            2026           11-18            11-20
    """
    windows["start_month_day"] = windows["start_date"].dt.strftime("%m-%d")
    windows["end_month_day"] = windows["end_date"].dt.strftime("%m-%d")
    return windows[
        ["timeslice_id", "reference_year", "planning_year"]
        + ["start_month_day", "end_month_day"]
    ]


def _raise_on_inconsistent_reference_year_patterns(patterns: pd.DataFrame) -> None:
    """Raise if two planning years assigned the same reference year carry
    different window patterns — that would mean the sequence table (or its
    cyclic extension) doesn't match how the calendar was actually built, and
    decoding one pattern per reference year would silently lose windows."""
    occurrences = patterns.groupby(["reference_year", "planning_year"]).apply(
        lambda x: frozenset(
            zip(x["timeslice_id"], x["start_month_day"], x["end_month_day"])
        ),
        include_groups=False,
    )
    distinct_patterns = occurrences.groupby("reference_year").nunique()
    inconsistent = sorted(distinct_patterns[distinct_patterns > 1].index)
    if inconsistent:
        raise ValueError(
            f"Reference years whose timeslice window patterns differ between "
            f"planning years in the calendar: {inconsistent}"
        )


def _keep_first_occurrence_per_reference_year(patterns: pd.DataFrame) -> pd.DataFrame:
    """Keeps each reference year's pattern from its first planning year (all
    occurrences are identical — validated before this is called).

    I/O Example:
        timeslice_id     reference_year  planning_year  start_month_day  end_month_day
        nsw_peak_demand  2015            2026           11-18            11-20
        nsw_peak_demand  2015            2031           11-18            11-20

        returns:
            timeslice_id     reference_year  start_month_day  end_month_day
            nsw_peak_demand  2015            11-18            11-20
    """
    first_occurrence = patterns.groupby("reference_year")["planning_year"].transform(
        "min"
    )
    deduped = patterns[patterns["planning_year"] == first_occurrence]
    deduped = deduped.drop(columns="planning_year")
    return deduped.sort_values(_TIMESLICE_COLUMNS).reset_index(drop=True)
