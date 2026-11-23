import pandas as pd
import pytest

from ispypsa.config.validators import TsamConfig, TsamSegmentsConfig
from ispypsa.translator.temporal_aggregation import (
    _add_block,
    _add_segment_snapshot_weightings,
    _average_traces_over_segments,
    _create_segment_snapshot_map,
    _map_snapshots_to_segment_ends,
    _number_of_segments,
    _segment_lengths,
    _segment_snapshots_and_traces,
    _traces_as_columns,
)

_SNAPSHOT_MAP_DATES = ["original_snapshots", "snapshots"]


def _snapshots_spanning_weeks_and_model_years(csv_str_to_df):
    return csv_str_to_df(
        """
        investment_periods,  snapshots
        2025,                2024-07-01__00:30:00
        2025,                2024-07-08__00:00:00
        2025,                2024-07-08__00:30:00
        2025,                2025-07-01__00:00:00
        2026,                2025-07-01__00:30:00
        """,
        parse_dates=["snapshots"],
    )


def test_add_block_per_year(csv_str_to_df):
    snapshots = _snapshots_spanning_weeks_and_model_years(csv_str_to_df)

    result = _add_block(snapshots, TsamSegmentsConfig(n_segments_per_year=10), "fy")

    # 2025-07-01 00:00 ends the last interval of FY2025, so it stays in 2025's
    # block. The interval ending 00:30 is the first of FY2026.
    expected = csv_str_to_df(
        """
        investment_periods,  snapshots,             block
        2025,                2024-07-01__00:30:00,  2025
        2025,                2024-07-08__00:00:00,  2025
        2025,                2024-07-08__00:30:00,  2025
        2025,                2025-07-01__00:00:00,  2025
        2026,                2025-07-01__00:30:00,  2026
        """,
        parse_dates=["snapshots"],
        dtype={"block": str},
    )
    pd.testing.assert_frame_equal(result, expected)


def test_add_block_per_week(csv_str_to_df):
    snapshots = _snapshots_spanning_weeks_and_model_years(csv_str_to_df)

    result = _add_block(snapshots, TsamSegmentsConfig(n_segments_per_week=10), "fy")

    # Monday 2024-07-08 00:00 ends the week that began on Monday 1 July. The
    # week of Monday 2025-06-30 is cut in two by the model year boundary.
    expected = csv_str_to_df(
        """
        investment_periods,  snapshots,             block
        2025,                2024-07-01__00:30:00,  2025__week__of__2024-07-01
        2025,                2024-07-08__00:00:00,  2025__week__of__2024-07-01
        2025,                2024-07-08__00:30:00,  2025__week__of__2024-07-08
        2025,                2025-07-01__00:00:00,  2025__week__of__2025-06-30
        2026,                2025-07-01__00:30:00,  2026__week__of__2025-06-30
        """,
        parse_dates=["snapshots"],
    )
    pd.testing.assert_frame_equal(result, expected)


def test_add_block_per_day(csv_str_to_df):
    snapshots = _snapshots_spanning_weeks_and_model_years(csv_str_to_df)

    result = _add_block(snapshots, TsamSegmentsConfig(n_segments_per_day=10), "fy")

    # A snapshot stamped midnight ends the last interval of the day before.
    expected = csv_str_to_df(
        """
        investment_periods,  snapshots,             block
        2025,                2024-07-01__00:30:00,  2024-07-01
        2025,                2024-07-08__00:00:00,  2024-07-07
        2025,                2024-07-08__00:30:00,  2024-07-08
        2025,                2025-07-01__00:00:00,  2025-06-30
        2026,                2025-07-01__00:30:00,  2025-07-01
        """,
        parse_dates=["snapshots"],
    )
    pd.testing.assert_frame_equal(result, expected)


def test_traces_as_columns(csv_str_to_df):
    traces = {
        "NSW": csv_str_to_df(
            """
            datetime,              value
            2024-07-01__00:30:00,  100
            2024-07-01__01:00:00,  110
            """,
            parse_dates=["datetime"],
        ),
        "QLD": csv_str_to_df(
            """
            datetime,              value
            2024-07-01__00:30:00,  50
            2024-07-01__01:00:00,  60
            """,
            parse_dates=["datetime"],
        ),
    }

    result = _traces_as_columns(traces)

    expected = csv_str_to_df(
        """
        datetime,              NSW,  QLD
        2024-07-01__00:30:00,  100,  50
        2024-07-01__01:00:00,  110,  60
        """,
        parse_dates=["datetime"],
    ).set_index("datetime")
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "segments_config, snapshots_in_block, expected",
    [
        (TsamSegmentsConfig(n_segments_per_year=2000), 17520, 2000),
        (TsamSegmentsConfig(n_segments_per_day=8), 48, 8),
        # A full week of half-hours gets the configured number.
        (TsamSegmentsConfig(n_segments_per_week=40), 336, 40),
        # A single day is a seventh of a week: 40 x 48 / 336 = 5.7, rounded up.
        (TsamSegmentsConfig(n_segments_per_week=40), 48, 6),
    ],
)
def test_number_of_segments(segments_config, snapshots_in_block, expected):
    result = _number_of_segments(
        snapshots_in_block, segments_config, temporal_resolution_min=30
    )

    assert result == expected


def test_segment_lengths_merges_the_rows_that_are_most_alike(csv_str_to_df):
    features = csv_str_to_df(
        """
        datetime,              NSW,  QLD
        2024-07-01__00:30:00,  100,  50
        2024-07-01__01:00:00,  110,  55
        2024-07-01__01:30:00,  300,  90
        2024-07-01__02:00:00,  310,  95
        2024-07-01__02:30:00,  320,  95
        """,
        parse_dates=["datetime"],
    ).set_index("datetime")

    result = _segment_lengths(features, number_of_segments=2)

    # The step from 110 to 300 (and 55 to 90) is the largest, so the split is
    # there: two rows, then three.
    assert result == [2, 3]


def test_segment_lengths_with_one_segment_per_row_and_one_segment_overall(
    csv_str_to_df,
):
    features = csv_str_to_df(
        """
        datetime,              NSW
        2024-07-01__00:30:00,  100
        2024-07-01__01:00:00,  110
        2024-07-01__01:30:00,  300
        """,
        parse_dates=["datetime"],
    ).set_index("datetime")

    assert _segment_lengths(features, number_of_segments=3) == [1, 1, 1]
    assert _segment_lengths(features, number_of_segments=1) == [3]


def test_map_snapshots_to_segment_ends(csv_str_to_df):
    snapshots = csv_str_to_df(
        """
        investment_periods,  snapshots
        2025,                2024-07-01__00:30:00
        2025,                2024-07-01__01:00:00
        2025,                2024-07-01__01:30:00
        2025,                2024-07-01__02:00:00
        """,
        parse_dates=["snapshots"],
    )

    result = _map_snapshots_to_segment_ends(snapshots, segment_lengths=[1, 3])

    # The three-snapshot segment covers 00:30 to 02:00. It is stamped 02:00, the
    # time it ends, not 01:00, the stamp of its first snapshot.
    expected = csv_str_to_df(
        """
        investment_periods,  original_snapshots,    snapshots
        2025,                2024-07-01__00:30:00,  2024-07-01__00:30:00
        2025,                2024-07-01__01:00:00,  2024-07-01__02:00:00
        2025,                2024-07-01__01:30:00,  2024-07-01__02:00:00
        2025,                2024-07-01__02:00:00,  2024-07-01__02:00:00
        """,
        parse_dates=_SNAPSHOT_MAP_DATES,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_create_segment_snapshot_map_does_not_merge_across_blocks(csv_str_to_df):
    snapshots = csv_str_to_df(
        """
        investment_periods,  snapshots
        2025,                2024-07-01__23:00:00
        2025,                2024-07-01__23:30:00
        2025,                2024-07-02__00:00:00
        2025,                2024-07-02__00:30:00
        2025,                2024-07-02__01:00:00
        """,
        parse_dates=["snapshots"],
    )
    feature_traces = {
        "NSW": csv_str_to_df(
            """
            datetime,              value
            2024-07-01__23:00:00,  100
            2024-07-01__23:30:00,  110
            2024-07-02__00:00:00,  300
            2024-07-02__00:30:00,  310
            2024-07-02__01:00:00,  500
            """,
            parse_dates=["datetime"],
        )
    }

    result = _create_segment_snapshot_map(
        snapshots,
        feature_traces,
        TsamSegmentsConfig(n_segments_per_day=2),
        year_type="fy",
        temporal_resolution_min=30,
    )

    # The first three snapshots are 1 July's block (the one stamped midnight
    # ends that day) and the last two are 2 July's. In 1 July's block 100 and
    # 110 are merged. 300 and 310 are the most alike of all, but are in
    # different blocks, so they stay apart.
    expected = csv_str_to_df(
        """
        investment_periods,  original_snapshots,    snapshots
        2025,                2024-07-01__23:00:00,  2024-07-01__23:30:00
        2025,                2024-07-01__23:30:00,  2024-07-01__23:30:00
        2025,                2024-07-02__00:00:00,  2024-07-02__00:00:00
        2025,                2024-07-02__00:30:00,  2024-07-02__00:30:00
        2025,                2024-07-02__01:00:00,  2024-07-02__01:00:00
        """,
        parse_dates=_SNAPSHOT_MAP_DATES,
    )
    pd.testing.assert_frame_equal(result, expected)


def test_average_traces_over_segments(csv_str_to_df):
    traces = {
        "NSW": csv_str_to_df(
            """
            datetime,              value
            2024-07-01__00:30:00,  100
            2024-07-01__01:00:00,  110
            2024-07-01__01:30:00,  300
            2024-07-01__02:00:00,  320
            2024-07-01__02:30:00,  900
            """,
            parse_dates=["datetime"],
        )
    }
    snapshot_map = csv_str_to_df(
        """
        investment_periods,  original_snapshots,    snapshots
        2025,                2024-07-01__00:30:00,  2024-07-01__01:00:00
        2025,                2024-07-01__01:00:00,  2024-07-01__01:00:00
        2025,                2024-07-01__01:30:00,  2024-07-01__02:00:00
        2025,                2024-07-01__02:00:00,  2024-07-01__02:00:00
        """,
        parse_dates=_SNAPSHOT_MAP_DATES,
    )

    result = _average_traces_over_segments(traces, snapshot_map)

    # 02:30 (value 900) is not in the snapshot map, so it is left out.
    expected = csv_str_to_df(
        """
        datetime,              value
        2024-07-01__01:00:00,  105.0
        2024-07-01__02:00:00,  310.0
        """,
        parse_dates=["datetime"],
    )
    assert list(result) == ["NSW"]
    pd.testing.assert_frame_equal(result["NSW"], expected)


def test_average_traces_over_segments_with_no_traces(csv_str_to_df):
    snapshot_map = csv_str_to_df(
        """
        investment_periods,  original_snapshots,    snapshots
        2025,                2024-07-01__00:30:00,  2024-07-01__01:00:00
        """,
        parse_dates=_SNAPSHOT_MAP_DATES,
    )

    assert _average_traces_over_segments({}, snapshot_map) == {}


def test_add_segment_snapshot_weightings(csv_str_to_df):
    snapshots = csv_str_to_df(
        """
        investment_periods,  snapshots
        2025,                2024-07-01__00:30:00
        2025,                2024-07-01__02:00:00
        2026,                2025-07-01__01:00:00
        """,
        parse_dates=["snapshots"],
    )
    snapshot_map = csv_str_to_df(
        """
        investment_periods,  original_snapshots,    snapshots
        2025,                2024-07-01__00:30:00,  2024-07-01__00:30:00
        2025,                2024-07-01__01:00:00,  2024-07-01__02:00:00
        2025,                2024-07-01__01:30:00,  2024-07-01__02:00:00
        2025,                2024-07-01__02:00:00,  2024-07-01__02:00:00
        2026,                2025-07-01__00:30:00,  2025-07-01__01:00:00
        2026,                2025-07-01__01:00:00,  2025-07-01__01:00:00
        """,
        parse_dates=_SNAPSHOT_MAP_DATES,
    )

    result = _add_segment_snapshot_weightings(
        snapshots, snapshot_map, temporal_resolution_min=30
    )

    # 2025 models 2 hours: a half-hour segment (8760 x 0.5 / 2 = 2190) and a
    # one and a half hour segment (8760 x 1.5 / 2 = 6570). 2026 models a single
    # one hour segment, which stands for the whole year.
    expected = csv_str_to_df(
        """
        investment_periods,  snapshots,             objective,  generators,  stores
        2025,                2024-07-01__00:30:00,  2190.0,     2190.0,      0.5
        2025,                2024-07-01__02:00:00,  6570.0,     6570.0,      1.5
        2026,                2025-07-01__01:00:00,  8760.0,     8760.0,      1.0
        """,
        parse_dates=["snapshots"],
    )
    pd.testing.assert_frame_equal(result, expected)


def _traces_where_wind_changes_while_demand_is_flat(csv_str_to_df):
    """Demand steps up after the second snapshot; wind steps up after the third."""
    demand = csv_str_to_df(
        """
        datetime,              value
        2024-07-01__00:30:00,  100
        2024-07-01__01:00:00,  100
        2024-07-01__01:30:00,  300
        2024-07-01__02:00:00,  300
        """,
        parse_dates=["datetime"],
    )
    wind = csv_str_to_df(
        """
        datetime,              value
        2024-07-01__00:30:00,  0.1
        2024-07-01__01:00:00,  0.1
        2024-07-01__01:30:00,  0.1
        2024-07-01__02:00:00,  0.9
        """,
        parse_dates=["datetime"],
    )
    return {
        "demand_traces": {"NSW": demand},
        "solar_traces": {},
        "wind_traces": {"Bango Wind Farm": wind},
    }


def _four_half_hour_snapshots(csv_str_to_df):
    return csv_str_to_df(
        """
        investment_periods,  snapshots
        2025,                2024-07-01__00:30:00
        2025,                2024-07-01__01:00:00
        2025,                2024-07-01__01:30:00
        2025,                2024-07-01__02:00:00
        """,
        parse_dates=["snapshots"],
    )


def test_segment_snapshots_and_traces_using_demand_features(csv_str_to_df):
    snapshots = _four_half_hour_snapshots(csv_str_to_df)
    traces = _traces_where_wind_changes_while_demand_is_flat(csv_str_to_df)
    tsam_config = TsamConfig(
        features="demand", segments=TsamSegmentsConfig(n_segments_per_day=2)
    )

    segments, segmented_traces, snapshot_map = _segment_snapshots_and_traces(
        snapshots, traces, tsam_config, "fy", temporal_resolution_min=30
    )

    # Only demand is shown to tsam, so the two segments split where demand
    # steps from 100 to 300: after the second snapshot.
    expected_segments = csv_str_to_df(
        """
        investment_periods,  snapshots
        2025,                2024-07-01__01:00:00
        2025,                2024-07-01__02:00:00
        """,
        parse_dates=["snapshots"],
    )
    pd.testing.assert_frame_equal(segments, expected_segments)

    expected_snapshot_map = csv_str_to_df(
        """
        investment_periods,  original_snapshots,    snapshots
        2025,                2024-07-01__00:30:00,  2024-07-01__01:00:00
        2025,                2024-07-01__01:00:00,  2024-07-01__01:00:00
        2025,                2024-07-01__01:30:00,  2024-07-01__02:00:00
        2025,                2024-07-01__02:00:00,  2024-07-01__02:00:00
        """,
        parse_dates=_SNAPSHOT_MAP_DATES,
    )
    pd.testing.assert_frame_equal(snapshot_map, expected_snapshot_map)

    expected_demand = csv_str_to_df(
        """
        datetime,              value
        2024-07-01__01:00:00,  100.0
        2024-07-01__02:00:00,  300.0
        """,
        parse_dates=["datetime"],
    )
    pd.testing.assert_frame_equal(
        segmented_traces["demand_traces"]["NSW"], expected_demand
    )

    # Wind wasn't shown to tsam but is still averaged over the segments, so its
    # step from 0.1 to 0.9 inside the second segment is smoothed to 0.5.
    expected_wind = csv_str_to_df(
        """
        datetime,              value
        2024-07-01__01:00:00,  0.1
        2024-07-01__02:00:00,  0.5
        """,
        parse_dates=["datetime"],
    )
    pd.testing.assert_frame_equal(
        segmented_traces["wind_traces"]["Bango Wind Farm"], expected_wind
    )
    assert segmented_traces["solar_traces"] == {}


def test_segment_snapshots_and_traces_using_all_traces_as_features(csv_str_to_df):
    snapshots = _four_half_hour_snapshots(csv_str_to_df)
    traces = _traces_where_wind_changes_while_demand_is_flat(csv_str_to_df)
    tsam_config = TsamConfig(
        features="all-traces", segments=TsamSegmentsConfig(n_segments_per_day=3)
    )

    segments, segmented_traces, snapshot_map = _segment_snapshots_and_traces(
        snapshots, traces, tsam_config, "fy", temporal_resolution_min=30
    )

    # With wind shown to tsam as well, three segments keep both steps: demand's
    # after the second snapshot and wind's after the third. Only the first two
    # snapshots, where neither trace changes, are merged.
    expected_snapshot_map = csv_str_to_df(
        """
        investment_periods,  original_snapshots,    snapshots
        2025,                2024-07-01__00:30:00,  2024-07-01__01:00:00
        2025,                2024-07-01__01:00:00,  2024-07-01__01:00:00
        2025,                2024-07-01__01:30:00,  2024-07-01__01:30:00
        2025,                2024-07-01__02:00:00,  2024-07-01__02:00:00
        """,
        parse_dates=_SNAPSHOT_MAP_DATES,
    )
    pd.testing.assert_frame_equal(snapshot_map, expected_snapshot_map)

    expected_segments = csv_str_to_df(
        """
        investment_periods,  snapshots
        2025,                2024-07-01__01:00:00
        2025,                2024-07-01__01:30:00
        2025,                2024-07-01__02:00:00
        """,
        parse_dates=["snapshots"],
    )
    pd.testing.assert_frame_equal(segments, expected_segments)

    expected_wind = csv_str_to_df(
        """
        datetime,              value
        2024-07-01__01:00:00,  0.1
        2024-07-01__01:30:00,  0.1
        2024-07-01__02:00:00,  0.9
        """,
        parse_dates=["datetime"],
    )
    pd.testing.assert_frame_equal(
        segmented_traces["wind_traces"]["Bango Wind Farm"], expected_wind
    )
