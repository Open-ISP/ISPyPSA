import pandas as pd

from ispypsa.plotting.utils import calculate_week_starting


def test_calculate_week_starting():
    """
    Test calculate_week_starting helper function.

    Verifies logic:
    - Subtracts 1 second from timestamp
    - Converts to weekly period (Monday start)
    - Returns date component

    Note: Pandas 'W' period is Week Ending Sunday.
    Mon 00:00:00 - 1s = Sun 23:59:59 -> Previous Week
    Mon 00:00:01 - 1s = Mon 00:00:00 -> Current Week
    """
    from datetime import date

    # Recent dates (November 2025)
    # Nov 17 (Mon) - Nov 23 (Sun) is one week.
    # Nov 24 (Mon) - Nov 30 (Sun) is next week.

    timesteps = pd.Series(
        pd.to_datetime(
            [
                "2025-11-23 23:59:00",  # Sunday late -> Week of Nov 17
                "2025-11-24 00:00:00",  # Monday midnight -> Week of Nov 17 (due to -1s)
                "2025-11-24 00:01:00",  # Monday just after midnight -> Week of Nov 24
                "2025-11-30 12:00:00",  # Sunday noon -> Week of Nov 24
            ]
        )
    )

    result = calculate_week_starting(timesteps)

    expected = pd.Series(
        [
            date(2025, 11, 17),
            date(2025, 11, 17),
            date(2025, 11, 24),
            date(2025, 11, 24),
        ]
    )

    pd.testing.assert_series_equal(result, expected, check_names=False)
