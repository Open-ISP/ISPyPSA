import pandas as pd

from ispypsa.plotting.helpers import (
    _add_interval_starts,
    _build_step_coordinates,
    _calculate_week_starting,
)


def test_calculate_week_starting():
    """
    Test calculate_week_starting helper function.

    Verifies logic:
    - Subtracts 1 second from timestamp
    - Converts to weekly period (Monday start)
    - Returns date component

    Note: Pandas 'W' period is Week Ending Sunday.
    Mon 00:00:00 - 1s = Sun 23:59:59 -> Previous Week
    Mon 00:00:01 - 1s = Mon 00:00:00 -> Current Week
    """
    from datetime import date

    # Recent dates (November 2025)
    # Nov 17 (Mon) - Nov 23 (Sun) is one week.
    # Nov 24 (Mon) - Nov 30 (Sun) is next week.

    timesteps = pd.Series(
        pd.to_datetime(
            [
                "2025-11-23 23:59:00",  # Sunday late -> Week of Nov 17
                "2025-11-24 00:00:00",  # Monday midnight -> Week of Nov 17 (due to -1s)
                "2025-11-24 00:01:00",  # Monday just after midnight -> Week of Nov 24
                "2025-11-30 12:00:00",  # Sunday noon -> Week of Nov 24
            ]
        )
    )

    result = _calculate_week_starting(timesteps)

    expected = pd.Series(
        [
            date(2025, 11, 17),
            date(2025, 11, 17),
            date(2025, 11, 24),
            date(2025, 11, 24),
        ]
    )

    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_add_interval_starts(csv_str_to_df):
    table = csv_str_to_df(
        """
        investment_period,  timestep,             flow_mw
        2025,               2024-07-01 00:30:00,  100
        2025,               2024-07-01 02:00:00,  300
        2030,               2029-07-01 00:20:00,  200
        """,
        parse_dates=["timestep"],
    )
    # As read back from timesteps.csv: timestep is text, and the table has
    # columns the plots don't use. A third of an hour has no exact value in
    # hours, and still gives a start on the minute.
    timesteps = csv_str_to_df("""
        investment_period,  timestep,             duration_hours,      represented_hours_per_year
        2025,               2024-07-01 00:30:00,  0.5,                 2190.0
        2025,               2024-07-01 02:00:00,  1.5,                 6570.0
        2030,               2029-07-01 00:20:00,  0.3333333333333333,  8760.0
    """)

    result = _add_interval_starts(table, timesteps)

    expected = csv_str_to_df(
        """
        investment_period,  timestep,             flow_mw,  interval_start
        2025,               2024-07-01 00:30:00,  100,      2024-07-01 00:00:00
        2025,               2024-07-01 02:00:00,  300,      2024-07-01 00:30:00
        2030,               2029-07-01 00:20:00,  200,      2029-07-01 00:00:00
        """,
        parse_dates=["timestep", "interval_start"],
    )
    pd.testing.assert_frame_equal(result, expected)


_TABLE_COLUMNS = ["investment_period", "timestep", "flow_mw"]
_TIMESTEPS_COLUMNS = ["investment_period", "timestep", "duration_hours"]


def _empty_table():
    # The plots convert timestep to a datetime before the interval starts are
    # added, so an empty table arrives with a datetime column.
    table = pd.DataFrame(columns=_TABLE_COLUMNS)
    return table.astype({"timestep": "datetime64[ns]"})


def test_add_interval_starts_empty_table(csv_str_to_df):
    table = _empty_table()
    timesteps = csv_str_to_df("""
        investment_period,  timestep,             duration_hours
        2025,               2024-07-01 00:30:00,  0.5
    """)

    result = _add_interval_starts(table, timesteps)

    expected = csv_str_to_df("""
        investment_period,  timestep,  flow_mw,  interval_start
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_add_interval_starts_empty_timesteps(csv_str_to_df):
    table = csv_str_to_df(
        """
        investment_period,  timestep,             flow_mw
        2025,               2024-07-01 00:30:00,  100
        """,
        parse_dates=["timestep"],
    )
    timesteps = pd.DataFrame(columns=_TIMESTEPS_COLUMNS)

    result = _add_interval_starts(table, timesteps)

    expected = csv_str_to_df("""
        investment_period,  timestep,  flow_mw,  interval_start
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_add_interval_starts_both_empty(csv_str_to_df):
    table = _empty_table()
    timesteps = pd.DataFrame(columns=_TIMESTEPS_COLUMNS)

    result = _add_interval_starts(table, timesteps)

    expected = csv_str_to_df("""
        investment_period,  timestep,  flow_mw,  interval_start
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_build_step_coordinates_equal_length_intervals(csv_str_to_df):
    series = csv_str_to_df(
        """
        interval_start,       timestep,             flow_mw
        2024-07-01 00:00:00,  2024-07-01 00:30:00,  100
        2024-07-01 00:30:00,  2024-07-01 01:00:00,  300
        2024-07-01 01:00:00,  2024-07-01 01:30:00,  200
        """,
        parse_dates=["interval_start", "timestep"],
    )

    result = _build_step_coordinates(series, "flow_mw")

    # One point per interval at its end, and two at the start of the run: a
    # blank, then the first interval's value so that the interval has a width.
    expected = csv_str_to_df(
        """
        time,                 value
        2024-07-01 00:00:00,
        2024-07-01 00:00:00,  100
        2024-07-01 00:30:00,  100
        2024-07-01 01:00:00,  300
        2024-07-01 01:30:00,  200
        """,
        parse_dates=["time"],
    )
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_build_step_coordinates_unequal_segments(csv_str_to_df):
    # The rows are out of time order, and come back in it.
    series = csv_str_to_df(
        """
        interval_start,       timestep,             flow_mw
        2024-07-01 02:00:00,  2024-07-01 10:30:00,  200
        2024-07-01 00:00:00,  2024-07-01 00:30:00,  100
        2024-07-01 00:30:00,  2024-07-01 02:00:00,  300
        """,
        parse_dates=["interval_start", "timestep"],
    )

    result = _build_step_coordinates(series, "flow_mw")

    # The 8.5 hour segment is one point at its end like any other: its width
    # comes from the point before it, at 02:00, where it starts.
    expected = csv_str_to_df(
        """
        time,                 value
        2024-07-01 00:00:00,
        2024-07-01 00:00:00,  100
        2024-07-01 00:30:00,  100
        2024-07-01 02:00:00,  300
        2024-07-01 10:30:00,  200
        """,
        parse_dates=["time"],
    )
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_build_step_coordinates_gap_between_runs(csv_str_to_df):
    series = csv_str_to_df(
        """
        interval_start,       timestep,             flow_mw
        2024-07-01 00:00:00,  2024-07-01 00:30:00,  100
        2024-07-01 00:30:00,  2024-07-01 02:00:00,  300
        2024-07-08 00:00:00,  2024-07-08 01:00:00,  200
        2024-07-08 01:00:00,  2024-07-08 01:30:00,  400
        """,
        parse_dates=["interval_start", "timestep"],
    )

    result = _build_step_coordinates(series, "flow_mw")

    # The interval ending 2024-07-08 01:00 starts at 2024-07-08 00:00, not at
    # 2024-07-01 02:00 where the one before it ends, so it starts a new run: a
    # blank at its start breaks the line, and the next point holds its value
    # from there.
    expected = csv_str_to_df(
        """
        time,                 value
        2024-07-01 00:00:00,
        2024-07-01 00:00:00,  100
        2024-07-01 00:30:00,  100
        2024-07-01 02:00:00,  300
        2024-07-08 00:00:00,
        2024-07-08 00:00:00,  200
        2024-07-08 01:00:00,  200
        2024-07-08 01:30:00,  400
        """,
        parse_dates=["time"],
    )
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_build_step_coordinates_single_interval(csv_str_to_df):
    series = csv_str_to_df(
        """
        interval_start,       timestep,             flow_mw
        2024-07-01 00:30:00,  2024-07-01 02:00:00,  300
        """,
        parse_dates=["interval_start", "timestep"],
    )

    result = _build_step_coordinates(series, "flow_mw")

    expected = csv_str_to_df(
        """
        time,                 value
        2024-07-01 00:30:00,
        2024-07-01 00:30:00,  300
        2024-07-01 02:00:00,  300
        """,
        parse_dates=["time"],
    )
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_build_step_coordinates_empty(csv_str_to_df):
    series = pd.DataFrame(columns=["interval_start", "timestep", "flow_mw"])

    result = _build_step_coordinates(series, "flow_mw")

    expected = csv_str_to_df("""
        time,  value
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
