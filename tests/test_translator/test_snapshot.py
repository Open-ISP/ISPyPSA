from datetime import datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ispypsa.translator.snapshots import (
    _add_investment_periods,
    _create_complete_snapshots_index,
    create_pypsa_friendly_snapshots,
)
from ispypsa.translator.timeslices import _add_interval_model_year_and_month_day


@pytest.mark.parametrize(
    "start_year,end_year, year_type, temporal_resolution_min, expected_first_datetime, expected_last_datetime, expected_length",
    [
        # One financial year with half hour resolution
        (
            2021,
            2021,
            "fy",
            30,
            datetime(year=2020, month=7, day=1, minute=30),
            datetime(year=2021, month=7, day=1, minute=0),
            8760 * 2,
        ),
        # One financial year with hourly resolution
        (
            2021,
            2021,
            "fy",
            60,
            datetime(year=2020, month=7, day=1, hour=1, minute=0),
            datetime(year=2021, month=7, day=1, minute=0),
            8760,
        ),
        # One financial year with four hourly resolution
        (
            2021,
            2021,
            "fy",
            240,
            datetime(year=2020, month=7, day=1, hour=4, minute=0),
            datetime(year=2021, month=7, day=1, minute=0),
            8760 / 4,
        ),
        # One financial year with fifteen minute resolution
        (
            2021,
            2021,
            "fy",
            15,
            datetime(year=2020, month=7, day=1, hour=0, minute=15),
            datetime(year=2021, month=7, day=1, minute=0),
            8760 * 4,
        ),
        # Three financial years with half hour resolution
        (
            2021,
            2023,
            "fy",
            30,
            datetime(year=2020, month=7, day=1, minute=30),
            datetime(year=2023, month=7, day=1, minute=0),
            8760 * 2 * 3,
        ),
        # One calendar year with half hour resolution
        (
            2021,
            2021,
            "calendar",
            30,
            datetime(year=2021, month=1, day=1, minute=30),
            datetime(year=2022, month=1, day=1, minute=0),
            8760 * 2,
        ),
        # One calendar year with hourly resolution
        (
            2021,
            2021,
            "calendar",
            60,
            datetime(year=2021, month=1, day=1, hour=1, minute=0),
            datetime(year=2022, month=1, day=1, minute=0),
            8760,
        ),
        # One calendar year with four hourly resolution
        (
            2021,
            2021,
            "calendar",
            240,
            datetime(year=2021, month=1, day=1, hour=4, minute=0),
            datetime(year=2022, month=1, day=1, minute=0),
            8760 / 4,
        ),
        # One calendar year with fifteen minute resolution
        (
            2021,
            2021,
            "calendar",
            15,
            datetime(year=2021, month=1, day=1, hour=0, minute=15),
            datetime(year=2022, month=1, day=1, minute=0),
            8760 * 4,
        ),
        # Three calendar year with half hour resolution
        (
            2021,
            2023,
            "calendar",
            30,
            datetime(year=2021, month=1, day=1, minute=30),
            datetime(year=2024, month=1, day=1, minute=0),
            8760 * 2 * 3,
        ),
    ],
)
def test_snapshot_creation(
    start_year: int,
    end_year: int,
    year_type: str,
    temporal_resolution_min: int,
    expected_first_datetime: datetime,
    expected_last_datetime: datetime,
    expected_length: int,
):
    snapshot = _create_complete_snapshots_index(
        start_year=start_year,
        end_year=end_year,
        year_type=year_type,
        temporal_resolution_min=temporal_resolution_min,
    )
    assert snapshot["snapshots"].iloc[0] == expected_first_datetime
    assert snapshot["snapshots"].iloc[-1] == expected_last_datetime
    assert len(snapshot) == expected_length


def test_add_investment_periods_calendar_year_mapping():
    """Test basic calendar year mapping."""
    # Setup test data
    timestamps = ["2015-03-15", "2018-11-20", "2022-05-10"]
    df = pd.DataFrame({"snapshots": pd.to_datetime(timestamps)})
    investment_periods = [2015, 2020]

    # Expected result (2015-03-15 -> 2015, 2018-11-20 -> 2015, 2022-05-10 -> 2020)
    expected = pd.DataFrame(
        {
            "investment_periods": [2015, 2015, 2020],
            "snapshots": pd.to_datetime(timestamps),
        }
    )

    # Call function
    result = _add_investment_periods(df, investment_periods, "calendar")

    # Assert
    assert_frame_equal(result, expected)


def test_add_investment_periods_financial_year_mapping():
    """Test financial year mapping (FY starts in July)."""
    # Setup test data - mixing dates before and after July
    timestamps = ["2016-05-10", "2016-08-15", "2019-12-01"]
    df = pd.DataFrame({"snapshots": pd.to_datetime(timestamps)})
    investment_periods = [2015, 2017, 2020]

    # Expected result:
    # 2016-05-10 -> FY2016 (maps to 2015)
    # 2016-08-15 -> FY2017 (maps to 2017)
    # 2019-12-01 -> FY2020 (maps to 2020)
    expected = pd.DataFrame(
        {
            "investment_periods": [2015, 2017, 2020],
            "snapshots": pd.to_datetime(timestamps),
        }
    )

    # Call function
    result = _add_investment_periods(df, investment_periods, "fy")

    # Assert
    assert_frame_equal(result, expected)


def test_add_investment_periods_financial_year_boundary():
    """Test timestamps exactly at the financial year boundary."""
    # Setup test data - dates exactly on July 1st
    timestamps = ["2017-06-30", "2017-07-01"]
    df = pd.DataFrame({"snapshots": pd.to_datetime(timestamps)})
    investment_periods = [2016, 2018]

    # Expected result:
    # 2017-06-30 -> FY2017 (maps to 2016)
    # 2017-07-01 -> FY2018 (maps to 2018)
    expected = pd.DataFrame(
        {"investment_periods": [2016, 2018], "snapshots": pd.to_datetime(timestamps)}
    )

    # Call function
    result = _add_investment_periods(df, investment_periods, "fy")

    # Assert
    assert_frame_equal(result, expected)


def test_add_investment_periods_non_sequential_investment_periods():
    """Test with non-sequential investment periods."""
    timestamps = ["2014-05-10", "2018-03-15", "2022-11-20"]
    df = pd.DataFrame({"snapshots": pd.to_datetime(timestamps)})
    investment_periods = [2010, 2015, 2022]  # Note the gap between 2015 and 2022

    # Expected result:
    # 2014-05-10 -> 2010
    # 2018-03-15 -> 2015
    # 2022-11-20 -> 2022
    expected = pd.DataFrame(
        {
            "investment_periods": [2010, 2015, 2022],
            "snapshots": pd.to_datetime(timestamps),
        }
    )

    # Call function
    result = _add_investment_periods(df, investment_periods, "calendar")

    # Assert
    assert_frame_equal(result, expected)


def test_add_investment_periods_unmapped_timestamps_error():
    """Test error is raised when timestamps can't be mapped."""
    # Setup test data with a timestamp before the earliest investment period
    timestamps = ["2005-01-15", "2016-05-10"]
    df = pd.DataFrame({"snapshots": pd.to_datetime(timestamps)})
    investment_periods = [2010, 2015]

    # Test for ValueError
    with pytest.raises(ValueError) as excinfo:
        _add_investment_periods(df, investment_periods, "calendar")

    # Verify error message contains useful information
    assert "Investment periods not compatible with modelling time window." in str(
        excinfo.value
    )
    assert "2005-01-15" in str(excinfo.value)
    assert "2010" in str(excinfo.value)


# --- The model years of the snapshots ---
#
# The carbon budget translator scales a partly modelled budget on the basis
# that every year from the config's start_year to its end_year is modelled.
# These tests pin that the snapshots have exactly those model years, with and
# without temporal aggregation.


def test_snapshots_have_every_model_year_in_the_range(
    csv_str_to_df, sample_model_config
):
    config = sample_model_config
    config.temporal.range.start_year = 2027
    config.temporal.range.end_year = 2028
    config.temporal.capacity_expansion.investment_periods = [2027]
    config.temporal.capacity_expansion.aggregation.representative_weeks = None

    snapshots = create_pypsa_friendly_snapshots(config, "capacity_expansion")
    snapshots = _add_interval_model_year_and_month_day(
        snapshots, config.temporal.year_type
    )
    result = snapshots.groupby("model_year", as_index=False).size()

    # Half-hourly snapshots: 365 x 48 = 17520 in financial year 2027, and
    # 366 x 48 = 17568 in 2028, which has 29 February 2028 in it. The
    # snapshots stamped midnight on 1 July 2027 and 2028 are the last of the
    # year just ended, so there are no snapshots in 2026 or 2029.
    expected = csv_str_to_df("""
        model_year,  size
        2027,        17520
        2028,        17568
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


def test_representative_weeks_have_every_model_year_in_the_range(
    csv_str_to_df, sample_model_config
):
    config = sample_model_config
    config.temporal.range.start_year = 2027
    config.temporal.range.end_year = 2030
    config.temporal.capacity_expansion.investment_periods = [2027, 2029]
    config.temporal.capacity_expansion.aggregation.representative_weeks = [1, 51]

    snapshots = create_pypsa_friendly_snapshots(config, "capacity_expansion")
    snapshots = _add_interval_model_year_and_month_day(
        snapshots, config.temporal.year_type
    )
    result = snapshots.groupby("model_year", as_index=False).size()

    # Two weeks of half-hourly snapshots in every year: 2 x 7 x 48 = 672.
    # 2028 and 2030 have snapshots although no investment period starts in
    # them.
    expected = csv_str_to_df("""
        model_year,  size
        2027,        672
        2028,        672
        2029,        672
        2030,        672
    """)
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)
