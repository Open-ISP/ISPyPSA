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
_model_year_of_dates is its inverse, mapping dates to labels: a date on the
boundary, such as 1 July, belongs to the model year starting that day.

Snapshots and other interval-end stamps need one more step. A snapshot is
stamped with the end time of the interval it covers. The last interval of a
model year ends at midnight on the year boundary, so its stamp shows the first
day of the next model year even though the whole interval lies in the year
just ended (the middle row below):

    snapshot stamp       interval covered                      model year (fy)
    2029-06-30 23:30:00  2029-06-30 23:00 to 23:30             2029
    2029-07-01 00:00:00  2029-06-30 23:30 to 2029-07-01 00:00  2029
    2029-07-01 00:30:00  2029-07-01 00:00 to 00:30             2030

Reading the model year off the stamp itself would put the middle row in 2030.
_model_year_of_interval_ends is the entry point for such stamps: it steps each
one back to the final second of its interval (_last_instant_of_interval) and
reads the model year off that.
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


def _model_year_of_dates(year_type: str, dates: pd.Series) -> pd.Series:
    """The model year each date falls in — the inverse of _model_year_start. A
    date on a boundary falls in the model year that starts that day.

    For datetimes that are stamped with the end of the interval they describe,
    such as snapshots, use _model_year_of_interval_ends instead.

    I/O Example:
        ("fy",       [2029-06-30, 2029-07-01]) -> [2029, 2030]
        ("calendar", [2029-06-30, 2029-07-01]) -> [2029, 2029]
    """
    years = dates.dt.year.astype("int64")
    if year_type == "fy":
        return years + (dates.dt.month >= 7).astype("int64")
    return years


def _model_year_of_interval_ends(year_type: str, interval_ends: pd.Series) -> pd.Series:
    """The model year of the interval each datetime is the end of.

    Assumes every value in interval_ends is the end time of an interval, the
    way snapshots are stamped: the model year is that of the date the
    interval's last instant falls on, so a stamp of exactly midnight on a year
    boundary belongs to the year just ended.

    I/O Example:
        ("fy",       [2029-07-01 00:00, 2029-07-01 00:30]) -> [2029, 2030]
        ("calendar", [2029-07-01 00:00, 2029-07-01 00:30]) -> [2029, 2029]
    """
    last_instants = _last_instant_of_interval(interval_ends)
    return _model_year_of_dates(year_type, last_instants)


def _last_instant_of_interval(interval_ends: pd.Series) -> pd.Series:
    """The final second of the interval each datetime is the end of — one
    second before the stamp, and the instant to read the interval's day and
    model year from.

    I/O Example:
        [2029-07-01 00:00:00, 2029-07-01 00:30:00]
        -> [2029-06-30 23:59:59, 2029-07-01 00:29:59]
    """
    return interval_ends - pd.Timedelta(seconds=1)
