import pandas as pd
from pandas.testing import assert_frame_equal

from ispypsa.translator.snapshots import _create_investment_period_weightings


def test_create_investment_period_weightings_basic():
    """Test with simple investment periods of 2020, 2030, 2040."""
    # Setup
    investment_periods = [2020, 2030, 2040]
    model_end_year = 2050
    discount_rate = 0.05

    # Expected result
    expected = pd.DataFrame(
        {
            "period": [2020, 2030, 2040],
            "years": [10, 10, 10],
            "objective": [
                sum([(1 / (1 + 0.05) ** t) for t in range(0, 10)]),
                sum([(1 / (1 + 0.05) ** t) for t in range(10, 20)]),
                sum([(1 / (1 + 0.05) ** t) for t in range(20, 30)]),
            ],
        }
    )

    # Call function
    result = _create_investment_period_weightings(
        investment_periods, model_end_year, discount_rate
    )

    # Assert
    assert_frame_equal(result, expected)


def test_create_investment_period_weightings_variable_length():
    """Test with variable length investment periods."""
    # Setup
    investment_periods = [2020, 2025, 2035]
    model_end_year = 2050
    discount_rate = 0.05

    # Expected result
    expected = pd.DataFrame(
        {
            "period": [2020, 2025, 2035],
            "years": [5, 10, 15],
            "objective": [
                sum([(1 / (1 + 0.05) ** t) for t in range(0, 5)]),
                sum([(1 / (1 + 0.05) ** t) for t in range(5, 15)]),
                sum([(1 / (1 + 0.05) ** t) for t in range(15, 30)]),
            ],
        }
    )

    # Call function
    result = _create_investment_period_weightings(
        investment_periods, model_end_year, discount_rate
    )

    # Assert
    assert_frame_equal(result, expected)


def test_create_investment_period_weightings_zero_discount():
    """Test with zero discount rate."""
    # Setup
    investment_periods = [2020, 2030]
    model_end_year = 2040
    discount_rate = 0.0

    # Expected result - with zero discount rate, the weight is just the number of years
    expected = pd.DataFrame(
        {
            "period": [2020, 2030],
            "years": [10, 10],
            "objective": [10.0, 10.0],  # Weight equals years with no discounting
        }
    )

    # Call function
    result = _create_investment_period_weightings(
        investment_periods, model_end_year, discount_rate
    )

    # Assert
    assert_frame_equal(result, expected)


def test_create_investment_period_weightings_single_period():
    """Test with a single investment period."""
    # Setup
    investment_periods = [2020]
    model_end_year = 2030
    discount_rate = 0.05

    # Expected result
    expected = pd.DataFrame(
        {
            "period": [2020],
            "years": [10],
            "objective": [sum([(1 / (1 + 0.05) ** t) for t in range(0, 10)])],
        }
    )

    # Call function
    result = _create_investment_period_weightings(
        investment_periods, model_end_year, discount_rate
    )

    # Assert
    assert_frame_equal(result, expected)


def test_create_investment_period_weightings_alternative_discount():
    """Test with different discount rate."""
    # Setup
    investment_periods = [2020, 2025]
    model_end_year = 2030
    discount_rate = 0.10  # 10% discount rate

    # Expected result
    expected = pd.DataFrame(
        {
            "period": [2020, 2025],
            "years": [5, 5],
            "objective": [
                sum([(1 / (1 + 0.10) ** t) for t in range(0, 5)]),
                sum([(1 / (1 + 0.10) ** t) for t in range(5, 10)]),
            ],
        }
    )

    # Call function
    result = _create_investment_period_weightings(
        investment_periods, model_end_year, discount_rate
    )

    # Assert
    assert_frame_equal(result, expected)


def test_create_investment_period_weightings_trivial_discount():
    """Test with a discount rate of 100% (1.0) for easy manual verification.

    With a discount rate of 100%, each year the weight becomes halved:
    - Year 0: 1/(1+1)^0 = 1
    - Year 1: 1/(1+1)^1 = 0.5
    - Year 2: 1/(1+1)^2 = 0.25
    - Year 3: 1/(1+1)^3 = 0.125
    - etc.

    This makes it very easy to manually verify the calculation.
    """
    # Setup
    investment_periods = [2020, 2022]
    model_end_year = 2024
    discount_rate = 1.0  # 100% discount rate

    # With r = 1.0, the discounted weights are:
    # Period 1 (2020-2022): [1, 0.5] = 1.5
    # Period 2 (2022-2024): [0.25, 0.125] = 0.375

    # Expected result with manually calculated values
    expected = pd.DataFrame(
        {
            "period": [2020, 2022],
            "years": [2, 2],
            "objective": [1.5, 0.375],  # Manually verified
        }
    )

    # Call function
    result = _create_investment_period_weightings(
        investment_periods, model_end_year, discount_rate
    )

    # Assert
    assert_frame_equal(result, expected)
