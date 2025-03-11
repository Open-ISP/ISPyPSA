from datetime import datetime

import numpy as np
import pandas as pd

from ispypsa.translator.helpers import _get_iteration_start_and_end_time


def _create_complete_snapshots_index(
    start_year: int,
    end_year: int,
    operational_temporal_resolution_min: int,
    year_type: str,
) -> pd.DataFrame:
    """Creates a DatetimeIndex, stored in DataFrame, defining the snapshots for the model before temporal aggregation.

    The index will start at the beginning of `start_year` and finish at the end of
    `end_year` with the specified temporal resolution.

    Args:
        start_year: int specifying the start year
        end_year: int specifying the end year
        operational_temporal_resolution_min: int specifying the snapshot temporal resolution in minutes
        year_type: str specifying the year type. 'fy' for financial year means that start_year and end_year refer to
            the financial year ending in the given year, and calendar means start_year and end_year refer to
            standard calendar years.

    Returns:
        pd.DataFrame
    """
    start_year, end_year, month = _get_iteration_start_and_end_time(
        year_type, start_year, end_year
    )

    if operational_temporal_resolution_min < 60:
        hour = 0
        minute = operational_temporal_resolution_min
    else:
        hour = operational_temporal_resolution_min // 60
        minute = operational_temporal_resolution_min % 60

    start_date = datetime(year=start_year, month=month, day=1, hour=hour, minute=minute)
    end_date = datetime(year=end_year, month=month, day=1, hour=0, minute=0)

    time_index = pd.date_range(
        start=start_date,
        end=end_date,
        freq=str(operational_temporal_resolution_min) + "min",
        name="snapshots",
    )
    return pd.DataFrame(time_index).reset_index(drop=False)


def _add_investment_periods(
    snapshots: pd.DataFrame,
    investment_periods: list[int],
    year_type: str,
) -> pd.DataFrame:
    """Add a column to the snapshots pd.DataFrame specifying the investment period that
    each model time interval belongs too.

    Args:
        snapshots: pd.DataFrame with "snapshots" column specifying the time intervals
            of the model as datetime objects.
        investment_periods: list of ints specifying the investment period. Each int
            specifies the year an investment period begins and each period lasts until
            the next one starts.
        year_type: str which should be "fy" or "calendar". If "fy" then investment
            period ints are interpreted as specifying financial years (according to the
            calendar year the financial year ends in).


    Returns: pd.DataFrame with column "investment_periods" and "snapshots".
    """
    snapshots = snapshots.copy()
    snapshots["calendar_year"] = snapshots["snapshots"].dt.year
    snapshots["effective_year"] = snapshots["calendar_year"].astype("int64")

    if year_type == "fy":
        mask = snapshots["snapshots"].dt.month >= 7
        snapshots.loc[mask, "effective_year"] = (
            snapshots.loc[mask, "effective_year"] + 1
        )

    inv_periods_df = pd.DataFrame({"investment_periods": investment_periods})
    inv_periods_df["investment_periods"] = inv_periods_df["investment_periods"]
    inv_periods_df = inv_periods_df.sort_values("investment_periods")

    result = pd.merge_asof(
        snapshots,
        inv_periods_df,
        left_on="effective_year",
        right_on="investment_periods",
    )

    # Check if any timestamps couldn't be mapped to an investment period
    unmapped = result["investment_periods"].isna()
    if unmapped.any():
        # Get the earliest unmapped timestamp for the error message
        earliest_unmapped = result.loc[unmapped, "snapshots"].min()
        # Get the earliest investment period
        earliest_period = min(investment_periods)
        raise ValueError(
            f"Investment periods not compatible with modelling time window."
            f"Earliest unmapped timestamp: {earliest_unmapped}. "
            f"Earliest investment period: {earliest_period}."
        )

    return result.loc[:, ["investment_periods", "snapshots"]]


def _create_investment_period_weightings(
    investment_periods: list[int], model_end_year: int, discount_rate: float
) -> pd.DataFrame:
    """Create a pd.DataFrame specifying the weighting of each investment period based on
    the sum of discounted periods during the period.

    Args:
        investment_periods: list of years in which investment periods start.
        model_end_year: int specifying the last year modelling
        discount_rate: fraction (float) specifying the discount rate i.e. 5% is 0.05.

    Returns: pd.DataFrame with columns 'period', "years" and 'objective'. Where
        "period" is the start years of the investment periods, "years" is the length
        of each investment period, and "objective" is the relative weight of the
        objective function in each investment period.
    """
    # Add model_end_year to calculate final period length
    all_years = investment_periods + [model_end_year]

    # Calculate period lengths
    investment_period_lengths = np.diff(all_years).astype("int64")

    # Create DataFrame with periods and their lengths
    investment_period_weightings = pd.DataFrame(
        {"period": investment_periods, "years": investment_period_lengths}
    )

    model_start_year = investment_periods[0]

    def calc_weighting(period_start_year, period_length):
        T0 = period_start_year - model_start_year
        T1 = T0 + period_length
        r = discount_rate
        discounted_weights = [(1 / (1 + r) ** t) for t in range(T0, T1)]
        return sum(discounted_weights)

    investment_period_weightings["objective"] = investment_period_weightings.apply(
        lambda row: calc_weighting(row["period"], row["years"]), axis=1
    )

    return investment_period_weightings
