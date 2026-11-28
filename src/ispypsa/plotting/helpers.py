from datetime import timedelta

import numpy as np
import pandas as pd

# The plotly line shape the time-series traces are drawn with: "vh" holds each
# point's value back to the point before it (a vertical move at the previous
# point, then a horizontal one). The results stamp each value with the time its
# interval ends, so this draws the value flat over its interval. See
# _build_step_coordinates for the points the shape is used with.
_STEP_LINE_SHAPE = "vh"


def _calculate_week_starting(timesteps: pd.Series) -> pd.Series:
    """Calculate the week starting date (Monday) for each timestep.

    Args:
        timesteps: Series of datetime objects

    Returns:
        Series of dates representing the Monday of the week for each timestep.
    """
    return (timesteps - timedelta(seconds=1)).dt.to_period("W").dt.start_time.dt.date


def _add_interval_starts(table: pd.DataFrame, timesteps: pd.DataFrame) -> pd.DataFrame:
    """Adds the time each row's interval starts: its timestep less the
    timestep's duration_hours in the timesteps results table.

    The duration is rounded to the second, so that a length such as a third of
    an hour, which has no exact value in hours, still gives a start equal to
    the previous interval's end.

    I/O Example:
        table:
            investment_period  timestep             flow_mw
            2025               2024-07-01 00:30:00  100
            2025               2024-07-01 02:00:00  300

        timesteps:
            investment_period  timestep             duration_hours
            2025               2024-07-01 00:30:00  0.5
            2025               2024-07-01 02:00:00  1.5

        returns:
            investment_period  timestep             flow_mw  interval_start
            2025               2024-07-01 00:30:00  100      2024-07-01 00:00:00
            2025               2024-07-01 02:00:00  300      2024-07-01 00:30:00
    """
    timesteps = timesteps[["investment_period", "timestep", "duration_hours"]].copy()
    timesteps["timestep"] = pd.to_datetime(timesteps["timestep"])
    table = table.merge(timesteps, on=["investment_period", "timestep"])
    duration = pd.to_timedelta(table["duration_hours"], unit="h").dt.round("s")
    table["interval_start"] = table["timestep"] - duration
    return table.drop(columns="duration_hours")


def _build_step_coordinates(series: pd.DataFrame, value_column: str) -> pd.DataFrame:
    """Builds the points that draw each value of value_column as a flat step
    over its interval, from interval_start to timestep.

    A value in the results is a level held for the whole of its interval, and
    is stamped with the time the interval ends. Joining the stamped points with
    straight lines would show a ramp where the model has a flat block, so the
    points are instead drawn with the "vh" line shape (_STEP_LINE_SHAPE), which
    holds each point's value back to the point before it. That is right for
    every interval that starts where the previous one ends. It leaves two cases
    to handle, both at the start of a run (a stretch of intervals with no gap
    in time between them): the run's first interval has no previous point to
    be held back to, and the run before it must not be joined to it across the
    gap, as happens between representative weeks.

    series has one row per interval, with the columns interval_start, timestep
    and value_column. Returns a table of the points in drawing order, with the
    columns:

    - time: the x coordinate.
    - value: the y coordinate.

    The points are:

    - For every interval, a point at its timestep holding its value. These are
      the rows of series, so hovering over a chart shows the same timesteps and
      values as the results tables.
    - For the first interval of each run, two more points at its
      interval_start. The first is blank, which breaks a line and takes a
      stacked area down to zero across the gap. The second holds the
      interval's value, which gives the interval its width.

    The area under the drawn steps is then each value times the length of its
    interval, which is the energy.

    I/O Example:
        series (value_column "flow_mw"):
            interval_start       timestep             flow_mw
            2024-07-01 00:00:00  2024-07-01 00:30:00  100
            2024-07-01 00:30:00  2024-07-01 02:00:00  300
            2024-07-08 00:00:00  2024-07-08 00:30:00  200      # starts a new run

        returns:
            time                 value
            2024-07-01 00:00:00         # blank
            2024-07-01 00:00:00  100
            2024-07-01 00:30:00  100
            2024-07-01 02:00:00  300
            2024-07-08 00:00:00         # blank: breaks the line over the gap
            2024-07-08 00:00:00  200
            2024-07-08 00:30:00  200
    """
    series = series.sort_values("timestep")
    starts_a_run = series["interval_start"] != series["timestep"].shift()
    run_starts = series[starts_a_run]

    breaks = pd.DataFrame({"time": run_starts["interval_start"], "value": np.nan})
    run_start_points = pd.DataFrame(
        {"time": run_starts["interval_start"], "value": run_starts[value_column]}
    )
    interval_end_points = pd.DataFrame(
        {"time": series["timestep"], "value": series[value_column]}
    )

    # The sort is stable, so the two points at a run's start stay in this order.
    points = pd.concat([breaks, run_start_points, interval_end_points])
    return points.sort_values("time", kind="stable").reset_index(drop=True)
