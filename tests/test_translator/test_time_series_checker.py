import pandas as pd
import pytest


from ispypsa.translator.time_series_checker import check_time_series


def test_identical_series_passes():
    """Test that identical series pass validation"""
    series_a = pd.Series(
        [
            pd.Timestamp("2024-01-01 12:00:00"),
            pd.Timestamp("2024-01-01 13:00:00"),
            pd.Timestamp("2024-01-01 14:00:00"),
            pd.Timestamp("2024-01-01 15:00:00"),
            pd.Timestamp("2024-01-01 16:00:00"),
        ]
    )
    series_b = series_a.copy()

    # Should not raise any exceptions
    check_time_series(series_a, series_b, "time_process", "measurements")


def test_extra_values_raises_error():
    """Test that extra values in time_series raises ValueError"""
    expected = pd.Series(
        [
            pd.Timestamp("2024-01-01 12:00:00"),
            pd.Timestamp("2024-01-01 13:00:00"),
            pd.Timestamp("2024-01-01 14:00:00"),
        ]
    )
    actual = pd.Series(
        [
            pd.Timestamp("2024-01-01 12:00:00"),
            pd.Timestamp("2024-01-01 13:00:00"),
            pd.Timestamp("2024-01-01 14:00:00"),
            pd.Timestamp("2024-01-01 15:00:00"),  # Extra value
        ]
    )

    with pytest.raises(ValueError) as exc_info:
        check_time_series(actual, expected, "time_process", "measurements")

    assert "unexpected time series values" in str(exc_info.value)
    assert "15:00:00" in str(exc_info.value)


def test_missing_values_raises_error():
    """Test that missing values in time_series raises ValueError"""
    expected = pd.Series(
        [
            pd.Timestamp("2024-01-01 12:00:00"),
            pd.Timestamp("2024-01-01 13:00:00"),
            pd.Timestamp("2024-01-01 14:00:00"),
        ]
    )
    actual = pd.Series(
        [
            pd.Timestamp("2024-01-01 12:00:00"),
            pd.Timestamp("2024-01-01 13:00:00"),  # Missing last value
        ]
    )

    with pytest.raises(ValueError) as exc_info:
        check_time_series(actual, expected, "time_process", "measurements")

    assert "expected time series values where missing" in str(exc_info.value)
    assert "14:00:00" in str(exc_info.value)


def test_different_order_raises_error():
    """Test that different order raises ValueError"""
    expected = pd.Series(
        [
            pd.Timestamp("2024-01-01 12:00:00"),
            pd.Timestamp("2024-01-01 13:00:00"),
            pd.Timestamp("2024-01-01 14:00:00"),
        ]
    )
    actual = pd.Series(
        [
            pd.Timestamp("2024-01-01 13:00:00"),  # Swapped order
            pd.Timestamp("2024-01-01 12:00:00"),
            pd.Timestamp("2024-01-01 14:00:00"),
        ]
    )

    with pytest.raises(ValueError) as exc_info:
        check_time_series(actual, expected, "time_process", "measurements")

    assert "did not have the expect order" in str(exc_info.value)
    assert "13:00:00" in str(exc_info.value)
    assert "12:00:00" in str(exc_info.value)


def test_different_units_raises_error():
    """Test that different datetime units raise ValueError"""
    expected = pd.Series(
        [pd.Timestamp("2024-01-01 12:00:00"), pd.Timestamp("2024-01-01 13:00:00")]
    ).astype("datetime64[s]")

    actual = pd.Series(
        [pd.Timestamp("2024-01-01 12:00:00"), pd.Timestamp("2024-01-01 13:00:00")]
    ).astype("datetime64[ms]")

    with pytest.raises(ValueError) as exc_info:
        check_time_series(actual, expected, "time_process", "measurements")

    assert "incorrect units" in str(exc_info.value)
    assert "datetime64[s]" in str(exc_info.value)
    assert "datetime64[ms]" in str(exc_info.value)
