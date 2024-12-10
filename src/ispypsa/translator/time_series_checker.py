import pandas as pd


def check_time_series(
    time_series: pd.Series,
    expected_time_series: pd.Series,
    process_name: str,
    table_name: str,
):
    """Compares a Datetime series against an expected Datetime series
    and raises errors if the two series don't match.

    Args:
        time_series: pd.Series of type Datetime
        expected_time_series: pd.Series of type Datetime
        process_name: str, type of data being checked by higher level process
        table_name: str, name of table that time_series comes from

    Returns: None

    Raises: ValueError if series don't match
    """
    # Check datetime units
    time_unit = str(time_series.dtype)
    expected_unit = str(expected_time_series.dtype)
    if time_unit != expected_unit:
        raise ValueError(
            f"When processing {process_name}, time series for {table_name} had incorrect units. "
            f"expected: {expected_unit}, got: {time_unit}"
        )

    extra = set(time_series) - set(expected_time_series)
    if extra:
        raise ValueError(
            f"When processing {process_name}, unexpected time series values where found in {table_name}: {extra}"
        )

    missing = set(expected_time_series) - set(time_series)
    if missing:
        raise ValueError(
            f"When processing {process_name}, expected time series values where missing from {table_name}: {missing}"
        )

    # Check if the order is different
    if not time_series.equals(expected_time_series):
        # Find first difference in order
        for i, (val_a, val_b) in enumerate(zip(time_series, expected_time_series)):
            if val_a != val_b:
                raise ValueError(
                    f"When processing {process_name}, time series for {table_name} did not have the expect order. Series differ in order at position {i}: "
                    f"got={val_a}, expected={val_b}"
                )
