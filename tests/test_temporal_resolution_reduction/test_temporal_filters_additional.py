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
    Datetime,Value
    2024-01-01__00:00:00,500
    2024-01-08__00:00:00,600
    2024-01-15__00:00:00,700
    2024-12-16__00:00:00,1000
    2024-12-23__00:00:00,800
    """
    demand_data = csv_str_to_df(demand_csv)
    demand_data["Datetime"] = pd.to_datetime(demand_data["Datetime"])

    demand_traces = {"node1": demand_data}

    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_agg,
        snapshots=snapshots,
        demand_traces=demand_traces,
    )

    # Should include snapshots from both week 1 (representative) and week containing peak demand
    assert len(result) > 0
    # Week 1 snapshots
    assert pd.Timestamp("2024-01-08 00:00:00") in result["snapshots"].values
    # Peak demand week snapshots (Dec 16 has highest demand at 1000)
    assert pd.Timestamp("2024-12-16 00:00:00") in result["snapshots"].values


def test_filter_snapshots_no_filtering_returns_original():
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

    snapshots = pd.DataFrame(
        {"snapshots": pd.date_range("2024-01-01", "2024-01-03", freq="D")}
    )

    result = _filter_snapshots(
        year_type="calendar",
        temporal_range=temporal_range,
        temporal_aggregation_config=temporal_agg,
        snapshots=snapshots,
    )

    pd.testing.assert_frame_equal(result, snapshots)


def test_prepare_data_for_named_weeks_no_residual_metrics():
    """Test _prepare_data_for_named_weeks when no residual metrics are requested."""
    demand_data = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [100, 200, 300],
        }
    )

    demand_traces = {"node1": demand_data}
    named_weeks = ["peak-demand", "minimum-demand"]  # No residual metrics

    demand_result, renewable_result = _prepare_data_for_named_weeks(
        named_representative_weeks=named_weeks,
        existing_generators=None,
        demand_traces=demand_traces,
        generator_traces=None,
    )

    # Should aggregate demand data
    assert demand_result is not None
    assert len(demand_result) == 3
    assert renewable_result is None  # No renewable data needed


def test_prepare_data_for_named_weeks_with_residual_metrics_error():
    """Test _prepare_data_for_named_weeks raises error when residual metrics requested without generator data."""
    demand_data = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [100, 200, 300],
        }
    )

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
    demand_data = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [100, 200, 300],
        }
    )

    gen_data = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [0.5, 0.6, 0.7],  # Per-unit values
        }
    )

    demand_traces = {"node1": demand_data}
    generator_traces = {"wind1": gen_data, "solar1": gen_data}

    generators_csv = """
    generator,fuel_type,reg_cap
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

    assert demand_result is not None
    assert renewable_result is not None
    assert len(renewable_result) == 3


def test_aggregate_demand_traces_single_node():
    """Test _aggregate_demand_traces with single node."""
    demand_data = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [100, 200, 300],
        }
    )

    demand_traces = {"node1": demand_data}

    result = _aggregate_demand_traces(demand_traces)

    assert len(result) == 3
    assert result["Value"].sum() == 600
    assert list(result.columns) == ["Datetime", "Value"]


def test_aggregate_demand_traces_multiple_nodes():
    """Test _aggregate_demand_traces with multiple nodes."""
    demand_data1 = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [100, 200, 300],
        }
    )

    demand_data2 = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [50, 60, 70],
        }
    )

    demand_traces = {"node1": demand_data1, "node2": demand_data2}

    result = _aggregate_demand_traces(demand_traces)

    assert len(result) == 3
    assert result["Value"].tolist() == [150, 260, 370]


def test_aggregate_wind_solar_traces_basic(csv_str_to_df):
    """Test _aggregate_wind_solar_traces with wind and solar generators."""
    # Generator traces (per-unit values)
    wind_trace = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [0.5, 0.6, 0.7],
        }
    )

    solar_trace = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [0.8, 0.9, 0.4],
        }
    )

    generator_traces = {"wind_farm_1": wind_trace, "solar_farm_1": solar_trace}

    generators_csv = """
    generator,fuel_type,reg_cap
    wind_farm_1,Wind,100
    solar_farm_1,Solar,50
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    assert len(result) == 3
    # Check MW values: (0.5*100 + 0.8*50), (0.6*100 + 0.9*50), (0.7*100 + 0.4*50)
    expected_values = [90, 105, 90]
    assert result["Value"].tolist() == expected_values


def test_aggregate_wind_solar_traces_missing_generator(csv_str_to_df):
    """Test _aggregate_wind_solar_traces when generator is not in existing_generators."""
    gen_trace = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [0.5, 0.6, 0.7],
        }
    )

    generator_traces = {
        "wind_farm_1": gen_trace,
        "unknown_gen": gen_trace,  # This generator not in existing_generators
    }

    generators_csv = """
    generator,fuel_type,reg_cap
    wind_farm_1,Wind,100
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    # Should only include wind_farm_1
    assert len(result) == 3
    assert result["Value"].tolist() == [50, 60, 70]


def test_aggregate_wind_solar_traces_non_renewable_generators(csv_str_to_df):
    """Test _aggregate_wind_solar_traces ignores non-wind/solar generators."""
    gen_trace = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [0.5, 0.6, 0.7],
        }
    )

    generator_traces = {
        "wind_farm_1": gen_trace,
        "coal_plant": gen_trace,
        "gas_plant": gen_trace,
    }

    generators_csv = """
    generator,fuel_type,reg_cap
    wind_farm_1,Wind,100
    coal_plant,Black__Coal,200
    gas_plant,Gas,150
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    # Should only include wind_farm_1
    assert len(result) == 3
    assert result["Value"].tolist() == [50, 60, 70]


def test_aggregate_wind_solar_traces_no_renewable_generators(csv_str_to_df):
    """Test _aggregate_wind_solar_traces when no renewable generators exist."""
    gen_trace = pd.DataFrame(
        {
            "Datetime": pd.date_range("2024-01-01", "2024-01-03", freq="D"),
            "Value": [0.5, 0.6, 0.7],
        }
    )

    generator_traces = {"coal_plant": gen_trace, "gas_plant": gen_trace}

    generators_csv = """
    generator,fuel_type,reg_cap
    coal_plant,Black__Coal,200
    gas_plant,Gas,150
    """
    existing_generators = csv_str_to_df(generators_csv)

    result = _aggregate_wind_solar_traces(generator_traces, existing_generators)

    # Should return empty DataFrame with correct structure
    assert len(result) == 0
    assert list(result.columns) == ["Datetime", "Value"]
    assert result["Datetime"].dtype == "datetime64[ns]"


def test_representative_weeks_filter_error_week_out_of_range():
    """Test that _filter_snapshots_for_representative_weeks raises error for out-of-range week."""
    from ispypsa.translator.temporal_filters import (
        _filter_snapshots_for_representative_weeks,
    )

    # Create snapshots for a short year
    snapshots = pd.DataFrame(
        {"snapshots": pd.date_range("2024-01-01", "2024-12-31", freq="D")}
    )

    # Week 53 would extend beyond year end
    with pytest.raises(
        ValueError, match="Representative week 53 ends after end of model year"
    ):
        _filter_snapshots_for_representative_weeks(
            representative_weeks=[53],
            snapshots=snapshots,
            start_year=2024,
            end_year=2025,
            year_type="calendar",
        )


def test_filter_and_assign_weeks_financial_year_logic(csv_str_to_df):
    """Test _filter_and_assign_weeks with financial year (month != 1)."""
    from ispypsa.translator.temporal_filters import _filter_and_assign_weeks

    demand_csv = """
    Datetime,Value
    2023-07-01__00:00:00,100
    2023-07-03__00:00:00,200
    2024-06-30__00:00:00,300
    """
    demand_df = csv_str_to_df(demand_csv)
    demand_df["Datetime"] = pd.to_datetime(demand_df["Datetime"])
    demand_df = demand_df.rename(columns={"Value": "demand"})

    result = _filter_and_assign_weeks(
        demand_df=demand_df,
        start_year=2024,  # FY2024 starts July 2023
        end_year=2025,  # FY2024 ends June 2024
        month=7,  # Financial year starts in July
    )

    # Check that year is assigned based on end year (FY ending)
    assert all(result["year"] == 2024)


def test_calculate_week_metrics_without_residual(csv_str_to_df):
    """Test _calculate_week_metrics when residual_demand column is not present."""
    from ispypsa.translator.temporal_filters import _calculate_week_metrics

    # Create demand data with week assignments
    demand_csv = """
    Datetime,demand,year,week_end_time
    2024-01-01__00:00:00,100,2024,2024-01-08__00:00:00
    2024-01-02__00:00:00,200,2024,2024-01-08__00:00:00
    2024-01-08__00:00:00,150,2024,2024-01-15__00:00:00
    2024-01-09__00:00:00,250,2024,2024-01-15__00:00:00
    """
    demand_df = csv_str_to_df(demand_csv)
    demand_df["Datetime"] = pd.to_datetime(demand_df["Datetime"])
    demand_df["week_end_time"] = pd.to_datetime(demand_df["week_end_time"])

    result = _calculate_week_metrics(demand_df)

    # Should only have demand metrics, not residual
    assert "demand_max" in result.columns
    assert "demand_min" in result.columns
    assert "demand_mean" in result.columns
    assert "residual_demand_max" not in result.columns
    assert len(result) == 2  # Two weeks


def test_time_series_filter():
    """Test _time_series_filter function."""
    # Create time series data
    time_series_data = pd.DataFrame(
        {
            "snapshots": pd.date_range("2024-01-01", "2024-01-10", freq="D"),
            "value": range(10),
        }
    )

    # Create snapshots to filter by (only select specific dates)
    snapshots = pd.DataFrame(
        {"snapshots": pd.to_datetime(["2024-01-01", "2024-01-05", "2024-01-10"])}
    )

    result = _time_series_filter(time_series_data, snapshots)

    # Should only include rows matching snapshots
    assert len(result) == 3
    assert result["value"].tolist() == [0, 4, 9]
    assert all(result["snapshots"].isin(snapshots["snapshots"]))
