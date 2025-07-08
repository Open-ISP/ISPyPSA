"""Tests for the _merge_snapshot_dataframes helper function."""

import pandas as pd
import pytest

from ispypsa.translator.temporal_filters import _merge_snapshot_dataframes


def test_merge_empty_list(csv_str_to_df):
    """Test merging an empty list returns empty DataFrame."""
    result = _merge_snapshot_dataframes([])

    assert isinstance(result, pd.DataFrame)
    assert "snapshots" in result.columns
    assert len(result) == 0


def test_merge_single_dataframe(csv_str_to_df):
    """Test merging a single DataFrame returns it unchanged."""
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    """
    df = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    result = _merge_snapshot_dataframes([df])

    pd.testing.assert_frame_equal(result, df)


def test_merge_two_dataframes_no_overlap(csv_str_to_df):
    """Test merging two DataFrames with no overlapping snapshots."""
    snapshots1_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    """
    df1 = csv_str_to_df(snapshots1_csv, parse_dates=["snapshots"])

    snapshots2_csv = """
    snapshots
    2024-01-03__00:00:00
    2024-01-04__00:00:00
    """
    df2 = csv_str_to_df(snapshots2_csv, parse_dates=["snapshots"])

    result = _merge_snapshot_dataframes([df1, df2])

    expected_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    2024-01-04__00:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_merge_two_dataframes_with_overlap(csv_str_to_df):
    """Test merging two DataFrames with overlapping snapshots removes duplicates."""
    snapshots1_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    """
    df1 = csv_str_to_df(snapshots1_csv, parse_dates=["snapshots"])

    snapshots2_csv = """
    snapshots
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    2024-01-04__00:00:00
    """
    df2 = csv_str_to_df(snapshots2_csv, parse_dates=["snapshots"])

    result = _merge_snapshot_dataframes([df1, df2])

    expected_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    2024-01-04__00:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_merge_multiple_dataframes_sorts_chronologically(csv_str_to_df):
    """Test merging multiple DataFrames maintains chronological order."""
    # Create DataFrames with out-of-order dates
    snapshots1_csv = """
    snapshots
    2024-01-03__00:00:00
    2024-01-01__00:00:00
    """
    df1 = csv_str_to_df(snapshots1_csv, parse_dates=["snapshots"])

    snapshots2_csv = """
    snapshots
    2024-01-04__00:00:00
    2024-01-02__00:00:00
    """
    df2 = csv_str_to_df(snapshots2_csv, parse_dates=["snapshots"])

    # Third DataFrame with a duplicate entry
    snapshots3_csv = """
    snapshots
    2024-01-05__00:00:00
    2024-01-01__00:00:00
    """
    df3 = csv_str_to_df(snapshots3_csv, parse_dates=["snapshots"])

    result = _merge_snapshot_dataframes([df1, df2, df3])

    # Expected result should be sorted and without duplicates
    expected_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    2024-01-04__00:00:00
    2024-01-05__00:00:00
    """
    expected = csv_str_to_df(expected_csv, parse_dates=["snapshots"])

    pd.testing.assert_frame_equal(result, expected)


def test_merge_identical_dataframes(csv_str_to_df):
    """Test merging identical DataFrames results in no duplicates."""
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    """
    df = csv_str_to_df(snapshots_csv, parse_dates=["snapshots"])

    # Merge three identical DataFrames
    result = _merge_snapshot_dataframes([df.copy(), df.copy(), df.copy()])

    # Should be the same as the original
    pd.testing.assert_frame_equal(result, df)


def test_merge_preserves_datetime_type(csv_str_to_df):
    """Test that merging preserves datetime type of snapshots column."""
    snapshots1_csv = """
    snapshots
    2024-01-01__12:30:00
    2024-01-02__06:45:00
    """
    df1 = csv_str_to_df(snapshots1_csv, parse_dates=["snapshots"])

    snapshots2_csv = """
    snapshots
    2024-01-03__18:00:00
    2024-01-04__09:15:00
    """
    df2 = csv_str_to_df(snapshots2_csv, parse_dates=["snapshots"])

    result = _merge_snapshot_dataframes([df1, df2])

    # Check that the snapshots column is still datetime
    assert pd.api.types.is_datetime64_any_dtype(result["snapshots"])

    # Check specific timestamps are preserved
    assert pd.Timestamp("2024-01-01 12:30:00") in result["snapshots"].values
    assert pd.Timestamp("2024-01-04 09:15:00") in result["snapshots"].values


def test_merge_handles_different_index_names(csv_str_to_df):
    """Test merging DataFrames with different index configurations."""
    # First DataFrame with default index
    snapshots1_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    """
    df1 = csv_str_to_df(snapshots1_csv, parse_dates=["snapshots"])

    # Second DataFrame with custom index
    snapshots2_csv = """
    snapshots
    2024-01-03__00:00:00
    2024-01-04__00:00:00
    """
    df2 = csv_str_to_df(snapshots2_csv, parse_dates=["snapshots"])
    df2.index = pd.Index([100, 101], name="custom_index")

    result = _merge_snapshot_dataframes([df1, df2])

    # Result should have reset index (no name)
    assert result.index.name is None
    assert list(result.index) == [0, 1, 2, 3]
    assert len(result) == 4
