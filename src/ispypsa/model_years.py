"""The two time conventions the templater and translator share: where a model
year starts and ends, and which model year a snapshot belongs to. Both
packages import from here, so neither restates the financial-year boundary.

A model year is labelled with an int. Under financial year nomenclature
("fy") the label is the calendar year the financial year ends in, so 2030
spans 1 July 2029 to 30 June 2030. Under "calendar" the label is the calendar
year itself:

    year_type  year  starts at   ends just before
    fy         2030  2029-07-01  2030-07-01
    calendar   2030  2030-01-01  2031-01-01

_model_year_start maps a label to the datetime the model year starts at, and
_model_year_of is its inverse, mapping datetimes to labels.

Snapshots need one more step. A snapshot is stamped with the end time of the
interval it covers, so the stamp of a year's final interval is exactly the
start of the next model year:

    snapshot stamp       interval covered                      model year (fy)
    2029-06-30 23:30:00  2029-06-30 23:00 to 23:30             2029
    2029-07-01 00:00:00  2029-06-30 23:30 to 2029-07-01 00:00  2029
    2029-07-01 00:30:00  2029-07-01 00:00 to 00:30             2030

Reading the model year off the stamp itself would put the middle row in 2030.
_last_instant_of_interval steps each stamp back to the final second of its
interval, and callers holding snapshots pass that to _model_year_of.
"""

import pandas as pd


def _model_year_start(year_type: str, year: int) -> pd.Timestamp:
    """The datetime a model year starts at.

    I/O Example:
        ("fy", 2030)       -> 2029-07-01
        ("calendar", 2030) -> 2030-01-01
    """
    if year_type == "fy":
        return pd.Timestamp(year=year - 1, month=7, day=1)
    return pd.Timestamp(year=year, month=1, day=1)


def _model_year_of(year_type: str, timestamps: pd.Series) -> pd.Series:
    """The model year each timestamp falls in — the inverse of _model_year_start.

    Each timestamp is read as an instant, so one exactly on a boundary falls in
    the model year that starts there. Snapshot stamps are interval end times,
    not instants within the interval: pass them through
    _last_instant_of_interval first.

    I/O Example:
        ("fy",       [2029-06-30 23:00, 2029-07-01 00:00]) -> [2029, 2030]
        ("calendar", [2029-06-30 23:00, 2029-07-01 00:00]) -> [2029, 2029]
    """
    years = timestamps.dt.year.astype("int64")
    if year_type == "fy":
        return years + (timestamps.dt.month >= 7).astype("int64")
    return years


def _last_instant_of_interval(snapshots: pd.Series) -> pd.Series:
    """The final second of the interval each snapshot covers. Snapshots are
    stamped with their interval's end time, so this is one second before the
    stamp — the instant to read the interval's day and model year from.

    I/O Example:
        [2029-07-01 00:00:00, 2029-07-01 00:30:00]
        -> [2029-06-30 23:59:59, 2029-07-01 00:29:59]
    """
    return snapshots - pd.Timedelta(seconds=1)
