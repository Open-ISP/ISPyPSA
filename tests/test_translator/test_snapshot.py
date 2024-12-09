import pytest
from datetime import datetime

from ispypsa.translator.snapshot import create_snapshot_index


@pytest.mark.parametrize(
    "start_year,end_year, year_type, operational_temporal_resolution_min, expected_first_datetime, expected_last_datetime, expected_length",
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
    operational_temporal_resolution_min: int,
    expected_first_datetime: datetime,
    expected_last_datetime: datetime,
    expected_length: int,
):
    snapshot = create_snapshot_index(
        start_year=start_year,
        end_year=end_year,
        year_type=year_type,
        operational_temporal_resolution_min=operational_temporal_resolution_min,
    )
    # assert snapshot.index[0] == expected_first_datetime
    assert snapshot.index[-1] == expected_last_datetime
    assert len(snapshot) == expected_length
