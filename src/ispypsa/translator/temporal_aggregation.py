"""Reduces the number of snapshots in the capacity expansion model by merging
consecutive snapshots into segments of varying length, using the tsam package
(https://github.com/FZJ-IEK3-VSA/tsam). It runs in the translator after the
snapshots have been chosen (all of them, or the representative weeks) and before
the traces are saved, and only when the config sets
temporal.capacity_expansion.aggregation.tsam.

The input is the model's snapshots and its traces, both at the model's
resolution. The snapshots have one row per interval, stamped with the time the
interval ends:

    investment_periods  snapshots
    2025                2024-07-01 00:30:00
    2025                2024-07-01 01:00:00
    2025                2024-07-01 01:30:00
    2025                2024-07-01 02:00:00

and each trace (a demand node's load, or a wind or solar generator's
availability) has one row per interval:

    datetime             value
    2024-07-01 00:30:00  100
    2024-07-01 01:00:00  110
    2024-07-01 01:30:00  300
    2024-07-01 02:00:00  320

The output is the same two things with one row per segment, plus a table
recording which snapshots each segment replaced. With the four snapshots above
merged into two segments:

    snapshots
        investment_periods  snapshots
        2025                2024-07-01 01:00:00
        2025                2024-07-01 02:00:00

    each trace
        datetime             value
        2024-07-01 01:00:00  105
        2024-07-01 02:00:00  310

    snapshot map (one row per original snapshot)
        investment_periods  original_snapshots   snapshots
        2025                2024-07-01 00:30:00  2024-07-01 01:00:00
        2025                2024-07-01 01:00:00  2024-07-01 01:00:00
        2025                2024-07-01 01:30:00  2024-07-01 02:00:00
        2025                2024-07-01 02:00:00  2024-07-01 02:00:00

The steps, in order:

1. The snapshots are split into blocks: model years, weeks or days, depending
   on which of n_segments_per_year, n_segments_per_week and n_segments_per_day
   the config sets. Segments are chosen within a block and never cross from one
   block into the next. A week that is cut short by the start or end of a model
   year is its own block and gets a share of n_segments_per_week in proportion
   to its length.

2. For each block, tsam is given the traces chosen by the config's "features"
   option (the demand traces only, or every trace) and asked where to put the
   block's segments. It merges the neighbouring snapshots whose values are most
   alike, so quiet stretches end up as a few long segments and fast-changing
   stretches as many short ones. Only the segment lengths are taken from tsam.

3. Each segment is stamped with the time its last interval ends, which is the
   time the segment ends. This keeps the snapshots' convention: a snapshot is
   still stamped with the end of the stretch of time it covers. Stamping a
   segment with its first interval instead would give a time that is neither
   the segment's start nor its end.

4. Every trace, whether or not tsam was shown it, is averaged over each
   segment. tsam's own values for a segment are the same averages, so nothing
   is lost by not taking them from tsam.

5. Each segment's snapshot weightings are set from its length, so a segment
   covering four half-hours counts for four times as much of the year as a
   segment covering one, and storage sees it as lasting two hours.
"""

import math

import numpy as np
import pandas as pd
import tsam

from ispypsa.config.validators import TsamConfig, TsamSegmentsConfig
from ispypsa.translator.timeslices import _add_interval_model_year_and_month_day

_MINUTES_PER_WEEK = 7 * 24 * 60

_SNAPSHOT_MAP_COLUMNS = ["investment_periods", "original_snapshots", "snapshots"]


def _segment_snapshots_and_traces(
    snapshots: pd.DataFrame,
    traces: dict[str, dict[str, pd.DataFrame]],
    tsam_config: TsamConfig,
    year_type: str,
    temporal_resolution_min: int,
) -> tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame]], pd.DataFrame]:
    """Merges consecutive snapshots into segments, returning the segments as the
    new snapshots, every trace averaged over them, and the map from each original
    snapshot to its segment.

    See the module docstring for how the segments are chosen.

    I/O Example (features "demand", n_segments_per_day 2):
        snapshots:
            investment_periods  snapshots
            2025                2024-07-01 00:30:00
            2025                2024-07-01 01:00:00
            2025                2024-07-01 01:30:00
            2025                2024-07-01 02:00:00

        traces:
            {
                "demand_traces": {"NSW": <trace>},
                "solar_traces": {},
                "wind_traces": {"Bango Wind Farm": <trace>},
            }

        traces["demand_traces"]["NSW"]:
            datetime             value
            2024-07-01 00:30:00  100
            2024-07-01 01:00:00  110
            2024-07-01 01:30:00  300
            2024-07-01 02:00:00  320

        returns the snapshots:
            investment_periods  snapshots
            2025                2024-07-01 01:00:00
            2025                2024-07-01 02:00:00

        returns the traces, in the same layout, with "NSW" now:
            datetime             value
            2024-07-01 01:00:00  105
            2024-07-01 02:00:00  310

        returns the snapshot map:
            investment_periods  original_snapshots   snapshots
            2025                2024-07-01 00:30:00  2024-07-01 01:00:00
            2025                2024-07-01 01:00:00  2024-07-01 01:00:00
            2025                2024-07-01 01:30:00  2024-07-01 02:00:00
            2025                2024-07-01 02:00:00  2024-07-01 02:00:00
    """
    if tsam_config.features == "demand":
        feature_traces = traces["demand_traces"]
    else:
        feature_traces = (
            traces["demand_traces"] | traces["solar_traces"] | traces["wind_traces"]
        )

    snapshot_map = _create_segment_snapshot_map(
        snapshots,
        feature_traces,
        tsam_config.segments,
        year_type,
        temporal_resolution_min,
    )

    segmented_traces = {}
    for trace_type in traces:
        segmented_traces[trace_type] = _average_traces_over_segments(
            traces[trace_type], snapshot_map
        )

    segments = snapshot_map[["investment_periods", "snapshots"]].drop_duplicates()
    return segments.reset_index(drop=True), segmented_traces, snapshot_map


def _create_segment_snapshot_map(
    snapshots: pd.DataFrame,
    feature_traces: dict[str, pd.DataFrame],
    segments_config: TsamSegmentsConfig,
    year_type: str,
    temporal_resolution_min: int,
) -> pd.DataFrame:
    """Maps each snapshot to the segment it is merged into, with the segment
    named by the snapshot it ends on.

    The snapshots are split into blocks (model years, weeks or days) and each
    block is divided into segments separately, so no segment crosses from one
    block into the next.

    I/O Example (n_segments_per_day 2, year_type "fy"):
        snapshots:
            investment_periods  snapshots
            2025                2024-07-01 23:00:00
            2025                2024-07-01 23:30:00
            2025                2024-07-02 00:00:00  # ends 1 July, so it is in 1 July's block
            2025                2024-07-02 00:30:00  # the next day's block starts here
            2025                2024-07-02 01:00:00

        feature_traces:
            {"NSW": <trace>}

        feature_traces["NSW"]:
            datetime             value
            2024-07-01 23:00:00  100
            2024-07-01 23:30:00  110
            2024-07-02 00:00:00  300
            2024-07-02 00:30:00  310
            2024-07-02 01:00:00  500

        returns:
            investment_periods  original_snapshots   snapshots
            2025                2024-07-01 23:00:00  2024-07-01 23:30:00
            2025                2024-07-01 23:30:00  2024-07-01 23:30:00
            2025                2024-07-02 00:00:00  2024-07-02 00:00:00  # alike 00:30, but not merged across the blocks
            2025                2024-07-02 00:30:00  2024-07-02 00:30:00
            2025                2024-07-02 01:00:00  2024-07-02 01:00:00
    """
    snapshots = snapshots.sort_values("snapshots")
    snapshots = _add_block(snapshots, segments_config, year_type)
    features = _traces_as_columns(feature_traces)

    block_maps = []
    for _, block in snapshots.groupby("block", sort=False):
        number_of_segments = _number_of_segments(
            len(block), segments_config, temporal_resolution_min
        )
        segment_lengths = _segment_lengths(
            features.loc[block["snapshots"]], number_of_segments
        )
        block_maps.append(_map_snapshots_to_segment_ends(block, segment_lengths))

    return pd.concat(block_maps, ignore_index=True)


def _add_block(
    snapshots: pd.DataFrame, segments_config: TsamSegmentsConfig, year_type: str
) -> pd.DataFrame:
    """Adds the block each snapshot is segmented within: its model year, its week
    (Monday to Sunday) within the model year, or its day.

    Snapshots are stamped with the time their interval ends, so the block is read
    off the interval's last instant, one second before the stamp: a snapshot
    stamped exactly midnight belongs to the day, week and year just ended.

    I/O Example (n_segments_per_week set, year_type "fy"):
        snapshots:
            investment_periods  snapshots
            2025                2024-07-01 00:30:00  # Monday 1 July, first interval of FY2025
            2025                2024-07-08 00:00:00  # ends that same week
            2025                2024-07-08 00:30:00  # the next week
            2025                2025-07-01 00:00:00  # Monday 30 June, last interval of FY2025
            2026                2025-07-01 00:30:00  # same week, but in FY2026

        returns:
            investment_periods  snapshots            block
            2025                2024-07-01 00:30:00  2025 week of 2024-07-01
            2025                2024-07-08 00:00:00  2025 week of 2024-07-01
            2025                2024-07-08 00:30:00  2025 week of 2024-07-08
            2025                2025-07-01 00:00:00  2025 week of 2025-06-30
            2026                2025-07-01 00:30:00  2026 week of 2025-06-30

        with n_segments_per_year set, block is the model year: 2025, 2025, 2025, 2025, 2026
        with n_segments_per_day set, block is the day: 2024-07-01, 2024-07-07, 2024-07-08,
        2025-06-30, 2025-07-01
    """
    snapshots = _add_interval_model_year_and_month_day(snapshots, year_type)
    last_instant = snapshots["snapshots"] - pd.Timedelta(seconds=1)
    model_year = snapshots["model_year"].astype(str)
    week_start = last_instant.dt.to_period("W").dt.start_time.dt.strftime("%Y-%m-%d")

    if segments_config.n_segments_per_year is not None:
        snapshots["block"] = model_year
    if segments_config.n_segments_per_week is not None:
        snapshots["block"] = model_year + " week of " + week_start
    if segments_config.n_segments_per_day is not None:
        snapshots["block"] = last_instant.dt.strftime("%Y-%m-%d")

    return snapshots.drop(columns=["model_year", "month_day"])


def _traces_as_columns(traces: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Lays the traces out side by side, one column per trace, indexed by
    datetime.

    I/O Example:
        traces:
            {"NSW": <trace>, "QLD": <trace>}

        traces["NSW"]:
            datetime             value
            2024-07-01 00:30:00  100
            2024-07-01 01:00:00  110

        traces["QLD"]:
            datetime             value
            2024-07-01 00:30:00  50
            2024-07-01 01:00:00  60

        returns:
            datetime             NSW  QLD
            2024-07-01 00:30:00  100  50
            2024-07-01 01:00:00  110  60
    """
    columns = {name: traces[name].set_index("datetime")["value"] for name in traces}
    return pd.DataFrame(columns)


def _number_of_segments(
    snapshots_in_block: int,
    segments_config: TsamSegmentsConfig,
    temporal_resolution_min: int,
) -> int:
    """The number of segments to divide a block into: the configured number for
    a model year or a day, and for a week a share of the configured number in
    proportion to the week's length, rounded up.

    Model years start and end part way through a week, so the first and last
    weeks of a model year are shorter than seven days. So is a week that is only
    partly covered by the model's snapshots.

    I/O Example (n_segments_per_week 40, temporal_resolution_min 30):
        snapshots_in_block 336 (a full week)  -> 40
        snapshots_in_block 48 (a single day)  -> 6   # 40 x 48 / 336 = 5.7
    """
    if segments_config.n_segments_per_year is not None:
        return segments_config.n_segments_per_year
    if segments_config.n_segments_per_day is not None:
        return segments_config.n_segments_per_day
    snapshots_in_full_week = _MINUTES_PER_WEEK / temporal_resolution_min
    share_of_week = snapshots_in_block / snapshots_in_full_week
    return math.ceil(segments_config.n_segments_per_week * share_of_week)


def _segment_lengths(features: pd.DataFrame, number_of_segments: int) -> list[int]:
    """Asks tsam how to divide the rows of features into the given number of
    segments of consecutive rows, and returns how many rows each segment covers,
    in order.

    tsam merges the neighbouring rows whose values are most alike across all the
    columns, each column scaled to its own range. The rows are treated as one
    stretch of time in the order given: tsam is told nothing about their
    timestamps.

    I/O Example (number_of_segments 2):
        features:
            datetime             NSW  QLD
            2024-07-01 00:30:00  100  50
            2024-07-01 01:00:00  110  55
            2024-07-01 01:30:00  300  90
            2024-07-01 02:00:00  310  95
            2024-07-01 02:30:00  320  95

        returns:
            [2, 3]
    """
    # tsam can also group whole periods (days, say) into typical ones. That isn't
    # wanted here, so it is given a single period covering every row, with each
    # row counted as one hour long so the segment lengths come back as row counts.
    result = tsam.aggregate(
        features.reset_index(drop=True),
        n_clusters=1,
        period_duration=len(features),
        temporal_resolution=1,
        segments=tsam.SegmentConfig(n_segments=number_of_segments),
    )
    lengths_in_the_single_period = result.segment_durations[0]
    return list(lengths_in_the_single_period)


def _map_snapshots_to_segment_ends(
    snapshots: pd.DataFrame, segment_lengths: list[int]
) -> pd.DataFrame:
    """Maps each snapshot to the last snapshot of its segment, where the segments
    are consecutive runs of snapshots of the given lengths.

    Snapshots are stamped with the time their interval ends, so the last
    snapshot of a segment is stamped with the time the segment ends.

    I/O Example (segment_lengths [1, 3]):
        snapshots:
            investment_periods  snapshots
            2025                2024-07-01 00:30:00
            2025                2024-07-01 01:00:00
            2025                2024-07-01 01:30:00
            2025                2024-07-01 02:00:00

        returns:
            investment_periods  original_snapshots   snapshots
            2025                2024-07-01 00:30:00  2024-07-01 00:30:00
            2025                2024-07-01 01:00:00  2024-07-01 02:00:00
            2025                2024-07-01 01:30:00  2024-07-01 02:00:00
            2025                2024-07-01 02:00:00  2024-07-01 02:00:00
    """
    snapshot_map = snapshots.sort_values("snapshots")
    snapshot_map = snapshot_map.rename(columns={"snapshots": "original_snapshots"})
    segment_numbers = np.repeat(np.arange(len(segment_lengths)), segment_lengths)
    segment_ends = snapshot_map.groupby(segment_numbers)["original_snapshots"]
    snapshot_map["snapshots"] = segment_ends.transform("max")
    return snapshot_map[_SNAPSHOT_MAP_COLUMNS]


def _average_traces_over_segments(
    traces: dict[str, pd.DataFrame], snapshot_map: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Replaces each trace with its average over each segment, stamped with the
    segment's snapshot. Trace values at times outside the snapshot map are left
    out.

    I/O Example:
        traces:
            {"NSW": <trace>}

        traces["NSW"]:
            datetime             value
            2024-07-01 00:30:00  100
            2024-07-01 01:00:00  110
            2024-07-01 01:30:00  300
            2024-07-01 02:00:00  320
            2024-07-01 02:30:00  900  # not in the snapshot map

        snapshot_map:
            investment_periods  original_snapshots   snapshots
            2025                2024-07-01 00:30:00  2024-07-01 01:00:00
            2025                2024-07-01 01:00:00  2024-07-01 01:00:00
            2025                2024-07-01 01:30:00  2024-07-01 02:00:00
            2025                2024-07-01 02:00:00  2024-07-01 02:00:00

        returns {"NSW": <trace>}, where the trace is now:
            datetime             value
            2024-07-01 01:00:00  105
            2024-07-01 02:00:00  310
    """
    averaged_traces = {}
    for name, trace in traces.items():
        trace = trace.merge(
            snapshot_map, left_on="datetime", right_on="original_snapshots"
        )
        trace = trace.groupby("snapshots", as_index=False)["value"].mean()
        averaged_traces[name] = trace.rename(columns={"snapshots": "datetime"})
    return averaged_traces


def _add_segment_snapshot_weightings(
    snapshots: pd.DataFrame, snapshot_map: pd.DataFrame, temporal_resolution_min: int
) -> pd.DataFrame:
    """Adds the snapshot weightings to snapshots that are segments, each weighted
    by the length of time it covers.

    - stores is the segment's length in hours, so storage charges and discharges
      for as long as the segment lasts.
    - objective and generators are the segment's share of the hours modelled in
      its investment period, scaled to the 8760 hours of one year. This is the
      same weighting snapshots of equal length get from _add_snapshot_weightings
      (8760 divided by the number of snapshots), spread in proportion to length.

    I/O Example (temporal_resolution_min 30):
        snapshots:
            investment_periods  snapshots
            2025                2024-07-01 00:30:00
            2025                2024-07-01 02:00:00

        snapshot_map:
            investment_periods  original_snapshots   snapshots
            2025                2024-07-01 00:30:00  2024-07-01 00:30:00
            2025                2024-07-01 01:00:00  2024-07-01 02:00:00
            2025                2024-07-01 01:30:00  2024-07-01 02:00:00
            2025                2024-07-01 02:00:00  2024-07-01 02:00:00

        returns:
            investment_periods  snapshots            objective  generators  stores
            2025                2024-07-01 00:30:00  2190       2190        0.5     # 8760 x 0.5 / 2
            2025                2024-07-01 02:00:00  6570       6570        1.5     # 8760 x 1.5 / 2
    """
    segment_keys = ["investment_periods", "snapshots"]
    weightings = snapshot_map.groupby(segment_keys, as_index=False).size()
    weightings["stores"] = weightings["size"] * temporal_resolution_min / 60
    hours_in_investment_period = weightings.groupby("investment_periods")[
        "stores"
    ].transform("sum")
    weightings["objective"] = 8760 * weightings["stores"] / hours_in_investment_period
    weightings["generators"] = weightings["objective"]
    weightings = weightings[segment_keys + ["objective", "generators", "stores"]]
    return snapshots.merge(weightings, on=segment_keys)
