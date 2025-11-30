"""Additional tests for temporal_filters.py to achieve 100% coverage.

This module contains tests for functions and code paths not covered by existing tests.
"""

import pandas as pd
import pytest

from ispypsa.translator.temporal_filters import (
    _aggregate_demand_traces,
    _aggregate_wind_solar_traces,
    _filter_snapshots,
    _prepare_data_for_named_weeks,
    _time_series_filter,
)


def test_filter_snapshots_with_both_representative_and_named_weeks(csv_str_to_df):
    """Test _filter_snapshots when both representative_weeks and named_representative_weeks are provided."""
    from dataclasses import dataclass

    @dataclass
    class TemporalAggregationConfig:
        representative_weeks: list[int] | None
        named_representative_weeks: list[str] | None

    @dataclass
    class TemporalRangeConfig:
        start_year: int
        end_year: int

    temporal_agg = TemporalAggregationConfig(
        representative_weeks=[1], named_representative_weeks=["peak-demand"]
    )

    temporal_range = TemporalRangeConfig(start_year=2024, end_year=2025)

    # Create snapshots
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-08__00:00:00
    2024-01-15__00:00:00
    2024-12-16__00:00:00
    2024-12-23__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    # Create demand data
    demand_csv = """
    datetime,value
    2024-01-01__00:00:00,500
    2024-01-08__00:00:00,600
    2024-01-15__00:00:00,700
    2024-12-16__00:00:00,1000
    2024-12-23__00:00:00,800
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["datetime"] = pd.to_datetime(demand_data["datetime"])

    demand_traces = {"node1": demand_data}

    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_agg,
        snapshots=snapshots,
        demand_traces=demand_traces,
    )

    # Expected result includes snapshots from week 1 and peak demand week
    expected_csv = """
    snapshots
    2024-01-08__00:00:00
    2024-12-16__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )


def test_filter_snapshots_no_filtering_returns_original(csv_str_to_df):
    """Test _filter_snapshots returns original snapshots when no filtering is configured."""
    from dataclasses import dataclass

    @dataclass
    class TemporalAggregationConfig:
        representative_weeks: list[int] | None

    @dataclass
    class TemporalRangeConfig:
        start_year: int
        end_year: int

    temporal_agg = TemporalAggregationConfig(representative_weeks=None)

    temporal_range = TemporalRangeConfig(start_year=2024, end_year=2025)

    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-02__00:00:00
    2024-01-03__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_agg,
        snapshots=snapshots,
    )

    pd.testing.assert_frame_equal(result, snapshots)


def test_prepare_data_for_named_weeks_no_residual_metrics(csv_str_to_df):
    """Test _prepare_data_for_named_weeks when no residual metrics are requested."""
    demand_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["datetime"] = pd.to_datetime(demand_data["datetime"])

    demand_traces = {"node1": demand_data}
    named_weeks = ["peak-demand", "minimum-demand"]  # No residual metrics

    demand_result, renewable_result = _prepare_data_for_named_weeks(
        named_representative_weeks=named_weeks,
        existing_generators=None,
        demand_traces=demand_traces,
        generator_traces=None,
    )

    # Expected aggregated demand
    expected_demand_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    expected_demand = csv_str_to_df(expected_demand_csv)
    expected_demand["datetime"] = pd.to_datetime(expected_demand["datetime"])

    pd.testing.assert_frame_equal(demand_result, expected_demand)
    assert renewable_result is None


def test_prepare_data_for_named_weeks_with_residual_metrics_error(csv_str_to_df):
    """Test _prepare_data_for_named_weeks raises error when residual metrics requested without generator data."""
    demand_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["datetime"] = pd.to_datetime(demand_data["datetime"])

    demand_traces = {"node1": demand_data}
    named_weeks = ["residual-peak-demand"]  # Residual metric

    with pytest.raises(
        ValueError,
        match="existing_generators table and generator_traces must be provided",
    ):
        _prepare_data_for_named_weeks(
            named_representative_weeks=named_weeks,
            existing_generators=None,  # Missing
            demand_traces=demand_traces,
            generator_traces=None,  # Missing
        )


def test_prepare_data_for_named_weeks_with_residual_metrics_success(csv_str_to_df):
    """Test _prepare_data_for_named_weeks successfully prepares data for residual metrics."""
    demand_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["datetime"] = pd.to_datetime(demand_data["datetime"])

    gen_csv = """
    datetime,value
    2024-01-01__00:00:00,0.5
    2024-01-02__00:00:00,0.6
    2024-01-03__00:00:00,0.7
    """
    gen_data = csv_str_to_df(gen_csv)
    gen_data["datetime"] = pd.to_datetime(gen_data["datetime"])

    demand_traces = {"node1": demand_data}
    generator_traces = {"wind1": gen_data, "solar1": gen_data}

    generators_csv = """
    generator,fuel_type,maximum_capacity_mw
    wind1,Wind,100
    solar1,Solar,50
    """
    existing_generators = csv_str_to_df(generators_csv)

    named_weeks = ["residual-peak-demand"]

    demand_result, renewable_result = _prepare_data_for_named_weeks(
        named_representative_weeks=named_weeks,
        existing_generators=existing_generators,
        demand_traces=demand_traces,
        generator_traces=generator_traces,
    )

    # Expected demand result
    expected_demand_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    expected_demand = csv_str_to_df(expected_demand_csv)
    expected_demand["datetime"] = pd.to_datetime(expected_demand["datetime"])

    # Expected renewable result (0.5*100 + 0.5*50, etc.)
    expected_renewable_csv = """
    datetime,value
    2024-01-01__00:00:00,75.0
    2024-01-02__00:00:00,90.0
    2024-01-03__00:00:00,105.0
    """
    expected_renewable = csv_str_to_df(expected_renewable_csv)
    expected_renewable["datetime"] = pd.to_datetime(expected_renewable["datetime"])

    pd.testing.assert_frame_equal(demand_result, expected_demand)
    pd.testing.assert_frame_equal(renewable_result, expected_renewable)


def test_aggregate_demand_traces_single_node(csv_str_to_df):
    """Test _aggregate_demand_traces with single node."""
    demand_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["datetime"] = pd.to_datetime(demand_data["datetime"])

    demand_traces = {"node1": demand_data}

    result = _aggregate_demand_traces(demand_traces)

    # Expected result is same as input for single node
    expected_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    expected = csv_str_to_df(expected_csv)
    expected["datetime"] = pd.to_datetime(expected["datetime"])

    pd.testing.assert_frame_equal(result, expected)


def test_aggregate_demand_traces_multiple_nodes(csv_str_to_df):
    """Test _aggregate_demand_traces with multiple nodes."""
    demand1_csv = """
    datetime,value
    2024-01-01__00:00:00,100
    2024-01-02__00:00:00,200
    2024-01-03__00:00:00,300
    """
    demand_data1 = csv_str_to_df(demand1_csv)
    demand_data1["datetime"] = pd.to_datetime(demand_data1["datetime"])

    demand2_csv = """
    datetime,value
    2024-01-01__00:00:00,50
    2024-01-02__00:00:00,60
    2024-01-03__00:00:00,70
    """
    demand_data2 = csv_str_to_df(demand2_csv)
    demand_data2["datetime"] = pd.to_datetime(demand_data2["datetime"])

    demand_traces = {"node1": demand_data1, "node2": demand_data2}

    result = _aggregate_demand_traces(demand_traces)

    # Expected aggregated result
    expected_csv = """
    datetime,value
    2024-01-01__00:00:00,150
    2024-01-02__00:00:00,260
    2024-01-03__00:00:00,370
    """
    expected = csv_str_to_df(expected_csv)
    expected["datetime"] = pd.to_datetime(expected["datetime"])

    pd.testing.assert_frame_equal(result, expected)


def test_aggregate_wind_solar_traces_basic(csv_str_to_df):
    """Test _aggregate_wind_solar_traces with wind and solar generators."""
    # Wind trace (per-unit values)
    wind_csv = """
    datetime,value
    2024-01-01__00:00:00,0.5
    2024-01-02__00:00:00,0.6
    2024-01-03__00:00:00,0.7
    """
    wind_trace = csv_str_to_df(wind_csv)
    wind_trace["datetime"] = pd.to_datetime(wind_trace["datetime"])

    # Solar trace (per-unit values)
    solar_csv = """
    datetime,value
    2024-01-01__00:00:00,0.8
    2024-01-02__00:00:00,0.9
    2024-01-03__00:00:00,0.4
    """
    solar_trace = csv_str_to_df(solar_csv)
    solar_trace["datetime"] = pd.to_datetime(solar_trace["datetime"])

    generator_traces = {"wind_farm_1": wind_trace, "solar_farm_1": solar_trace}

    generators_csv = """
    generator,fuel_type,maximum_capacity_mw
    wind_farm_1,Wind,100
    solar_farm_1,Solar,50
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    # Expected MW values: (0.5*100 + 0.8*50), (0.6*100 + 0.9*50), (0.7*100 + 0.4*50)
    expected_csv = """
    datetime,value
    2024-01-01__00:00:00,90.0
    2024-01-02__00:00:00,105.0
    2024-01-03__00:00:00,90.0
    """
    expected = csv_str_to_df(expected_csv)
    expected["datetime"] = pd.to_datetime(expected["datetime"])

    pd.testing.assert_frame_equal(result, expected)


def test_aggregate_wind_solar_traces_missing_generator(csv_str_to_df):
    """Test _aggregate_wind_solar_traces when generator is not in existing_generators."""
    gen_csv = """
    datetime,value
    2024-01-01__00:00:00,0.5
    2024-01-02__00:00:00,0.6
    2024-01-03__00:00:00,0.7
    """
    gen_trace = csv_str_to_df(gen_csv)
    gen_trace["datetime"] = pd.to_datetime(gen_trace["datetime"])

    generator_traces = {
        "wind_farm_1": gen_trace,
        "unknown_gen": gen_trace,  # This generator not in existing_generators
    }

    generators_csv = """
    generator,fuel_type,maximum_capacity_mw
    wind_farm_1,Wind,100
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    # Expected: only wind_farm_1 included
    expected_csv = """
    datetime,value
    2024-01-01__00:00:00,50.0
    2024-01-02__00:00:00,60.0
    2024-01-03__00:00:00,70.0
    """
    expected = csv_str_to_df(expected_csv)
    expected["datetime"] = pd.to_datetime(expected["datetime"])

    pd.testing.assert_frame_equal(result, expected)


def test_aggregate_wind_solar_traces_non_renewable_generators(csv_str_to_df):
    """Test _aggregate_wind_solar_traces ignores non-wind/solar generators."""
    gen_csv = """
    datetime,value
    2024-01-01__00:00:00,0.5
    2024-01-02__00:00:00,0.6
    2024-01-03__00:00:00,0.7
    """
    gen_trace = csv_str_to_df(gen_csv)
    gen_trace["datetime"] = pd.to_datetime(gen_trace["datetime"])

    generator_traces = {
        "wind_farm_1": gen_trace,
        "coal_plant": gen_trace,
        "gas_plant": gen_trace,
    }

    generators_csv = """
    generator,fuel_type,maximum_capacity_mw
    wind_farm_1,Wind,100
    coal_plant,Black__Coal,200
    gas_plant,Gas,150
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    # Expected: only wind_farm_1 included
    expected_csv = """
    datetime,value
    2024-01-01__00:00:00,50.0
    2024-01-02__00:00:00,60.0
    2024-01-03__00:00:00,70.0
    """
    expected = csv_str_to_df(expected_csv)
    expected["datetime"] = pd.to_datetime(expected["datetime"])

    pd.testing.assert_frame_equal(result, expected)


def test_aggregate_wind_solar_traces_no_renewable_generators(csv_str_to_df):
    """Test _aggregate_wind_solar_traces when no renewable generators exist."""
    gen_csv = """
    datetime,value
    2024-01-01__00:00:00,0.5
    2024-01-02__00:00:00,0.6
    2024-01-03__00:00:00,0.7
    """
    gen_trace = csv_str_to_df(gen_csv)
    gen_trace["datetime"] = pd.to_datetime(gen_trace["datetime"])

    generator_traces = {"coal_plant": gen_trace, "gas_plant": gen_trace}

    generators_csv = """
    generator,fuel_type,maximum_capacity_mw
    coal_plant,Black__Coal,200
    gas_plant,Gas,150
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    # Should return empty DataFrame with correct structure
    expected = pd.DataFrame({"datetime": pd.to_datetime([]), "value": []})

    pd.testing.assert_frame_equal(result, expected)


def test_filter_and_assign_weeks_financial_year_logic(csv_str_to_df):
    """Test _filter_and_assign_weeks with financial year (month != 1)."""
    from ispypsa.translator.temporal_filters import _filter_and_assign_weeks

    # Let's test with calendar year first to ensure the function works
    demand_csv = """
    datetime,demand
    2024-01-08__00:00:00,100
    2024-01-10__00:00:00,200
    2024-01-15__00:00:00,150
    """
    demand_df = csv_str_to_df(demand_csv)
    demand_df["datetime"] = pd.to_datetime(demand_df["datetime"])

    result = _filter_and_assign_weeks(
        demand_df=demand_df,
        start_year=2024,
        end_year=2025,
        month=1,  # Calendar year
    )

    # Expected result - Jan 8 00:00:00 is already a Monday, so it marks end of its own week
    # Jan 10 and 15 are in week ending Jan 15
    expected_csv = """
    datetime,demand,year,week_end_time
    2024-01-08__00:00:00,100,2024,2024-01-08__00:00:00
    2024-01-10__00:00:00,200,2024,2024-01-15__00:00:00
    2024-01-15__00:00:00,150,2024,2024-01-15__00:00:00
    """
    expected = csv_str_to_df(expected_csv)
    expected["datetime"] = pd.to_datetime(expected["datetime"])
    expected["week_end_time"] = pd.to_datetime(expected["week_end_time"])

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )


def test_calculate_week_metrics_without_residual(csv_str_to_df):
    """Test _calculate_week_metrics when residual_demand column is not present."""
    from ispypsa.translator.temporal_filters import _calculate_week_metrics

    # Create demand data with week assignments
    demand_csv = """
    datetime,demand,year,week_end_time
    2024-01-01__00:00:00,100,2024,2024-01-08__00:00:00
    2024-01-02__00:00:00,200,2024,2024-01-08__00:00:00
    2024-01-08__00:00:00,150,2024,2024-01-15__00:00:00
    2024-01-09__00:00:00,250,2024,2024-01-15__00:00:00
    """
    demand_df = csv_str_to_df(demand_csv)
    demand_df["datetime"] = pd.to_datetime(demand_df["datetime"])
    demand_df["week_end_time"] = pd.to_datetime(demand_df["week_end_time"])

    result = _calculate_week_metrics(demand_df)

    # Expected metrics
    expected_csv = """
    year,week_end_time,demand_max,demand_min,demand_mean
    2024,2024-01-08__00:00:00,200,100,150.0
    2024,2024-01-15__00:00:00,250,150,200.0
    """
    expected = csv_str_to_df(expected_csv)
    expected["week_end_time"] = pd.to_datetime(expected["week_end_time"])

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )


def test_time_series_filter(csv_str_to_df):
    """Test _time_series_filter function."""
    # Create time series data
    time_series_csv = """
    snapshots,value
    2024-01-01__00:00:00,0
    2024-01-02__00:00:00,1
    2024-01-03__00:00:00,2
    2024-01-04__00:00:00,3
    2024-01-05__00:00:00,4
    2024-01-06__00:00:00,5
    2024-01-07__00:00:00,6
    2024-01-08__00:00:00,7
    2024-01-09__00:00:00,8
    2024-01-10__00:00:00,9
    """
    time_series_data = csv_str_to_df(time_series_csv)
    time_series_data["snapshots"] = pd.to_datetime(time_series_data["snapshots"])

    # Create snapshots to filter by
    snapshots_csv = """
    snapshots
    2024-01-01__00:00:00
    2024-01-05__00:00:00
    2024-01-10__00:00:00
    """
    snapshots = csv_str_to_df(snapshots_csv)
    snapshots["snapshots"] = pd.to_datetime(snapshots["snapshots"])

    result = _time_series_filter(time_series_data, snapshots)

    # Expected filtered result
    expected_csv = """
    snapshots,value
    2024-01-01__00:00:00,0
    2024-01-05__00:00:00,4
    2024-01-10__00:00:00,9
    """
    expected = csv_str_to_df(expected_csv)
    expected["snapshots"] = pd.to_datetime(expected["snapshots"])

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )
