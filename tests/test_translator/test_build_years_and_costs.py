import pandas as pd
import pytest

from ispypsa.translator.build_years_and_costs import (
    _build_years_from_commissioning_dates,
)


def test_build_years_from_commissioning_dates():
    commissioning_dates = pd.Series(
        [
            "01/08/2025",  # FY 2026
            "01/07/2027",  # FY 2028
            "01/01/2020",  # before model start
            "06/07/2029",  # day-month format: July + 'fy' -> 2030
            "07/06/2029",  #                   June + 'fy' -> 2029
            float("nan"),  # blank -> start_year - 1 -> 2025
        ],
    )
    result = _build_years_from_commissioning_dates(commissioning_dates, "fy", 2026)

    expected = pd.Series([2026, 2028, 2025, 2030, 2029, 2025])
    pd.testing.assert_series_equal(result, expected)
