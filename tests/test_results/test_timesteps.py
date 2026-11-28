import pandas as pd

from ispypsa.data_fetch import read_csvs, write_csvs
from ispypsa.pypsa_build import build_pypsa_network, update_network_timeseries
from ispypsa.results.timesteps import extract_timesteps


def test_timesteps_are_weighted_within_their_model_year_not_investment_period(
    csv_str_to_df,
):
    # The 2030 investment period covers the model years 2030 and 2031. Two
    # half-hours are modelled in 2030 and four in 2031.
    #
    # PyPSA's generators weighting shares 8760 hours between all six, 1460
    # each, so 100 MW throughout would come to 100 x 1460 x 6 = 876 GWh for
    # the two years together, and neither year's energy could be told apart.
    #
    # hours_represented shares 8760 hours within each model year: 4380 each in
    # 2030 and 2190 each in 2031, so 100 MW throughout comes to 876 GWh in
    # 2030 and 876 GWh in 2031.
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            objective,  generators,  stores,  model_year
        2030,                2029-07-02 00:30:00,  1460.0,     1460.0,      0.5,     2030
        2030,                2029-07-02 01:00:00,  1460.0,     1460.0,      0.5,     2030
        2030,                2030-07-01 00:30:00,  1460.0,     1460.0,      0.5,     2031
        2030,                2030-07-01 01:00:00,  1460.0,     1460.0,      0.5,     2031
        2030,                2031-01-06 00:30:00,  1460.0,     1460.0,      0.5,     2031
        2030,                2031-01-06 01:00:00,  1460.0,     1460.0,      0.5,     2031
    """)

    result = extract_timesteps(snapshots)

    expected = csv_str_to_df("""
        investment_period,  timestep,             model_year,  duration_hours,  hours_represented
        2030,               2029-07-02 00:30:00,  2030,        0.5,             4380.0
        2030,               2029-07-02 01:00:00,  2030,        0.5,             4380.0
        2030,               2030-07-01 00:30:00,  2031,        0.5,             2190.0
        2030,               2030-07-01 01:00:00,  2031,        0.5,             2190.0
        2030,               2031-01-06 00:30:00,  2031,        0.5,             2190.0
        2030,               2031-01-06 01:00:00,  2031,        0.5,             2190.0
    """)
    expected["timestep"] = pd.to_datetime(expected["timestep"])
    pd.testing.assert_frame_equal(result, expected)


def test_timesteps_covering_the_whole_model_year_stand_for_their_own_length(
    csv_str_to_df,
):
    # Nothing in the model year is left out, so each timestep stands for
    # exactly its own length. Two 4380 hour timesteps stand in for a full year
    # of half-hours to keep the table short.
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            objective,  generators,  stores,  model_year
        2025,                2024-12-30 12:00:00,  4380.0,     4380.0,      4380.0,  2025
        2025,                2025-07-01 00:00:00,  4380.0,     4380.0,      4380.0,  2025
    """)

    result = extract_timesteps(snapshots)

    expected = csv_str_to_df("""
        investment_period,  timestep,             model_year,  duration_hours,  hours_represented
        2025,               2024-12-30 12:00:00,  2025,        4380.0,          4380.0
        2025,               2025-07-01 00:00:00,  2025,        4380.0,          4380.0
    """)
    expected["timestep"] = pd.to_datetime(expected["timestep"])
    pd.testing.assert_frame_equal(result, expected)


def test_timesteps_that_are_segments_of_unequal_length(csv_str_to_df):
    # Two hours of 2025 merged into a segment of one half-hour and a segment
    # of three, stamped with the time each ends. The second is three times as
    # long and stands for three times as much of the year. 2026 has a single
    # one hour segment, which stands for the whole of that year.
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            objective,  generators,  stores,  model_year
        2025,                2024-07-01 00:30:00,  2190.0,     2190.0,      0.5,     2025
        2025,                2024-07-01 02:00:00,  6570.0,     6570.0,      1.5,     2025
        2026,                2025-07-01 01:00:00,  8760.0,     8760.0,      1.0,     2026
    """)

    result = extract_timesteps(snapshots)

    expected = csv_str_to_df("""
        investment_period,  timestep,             model_year,  duration_hours,  hours_represented
        2025,               2024-07-01 00:30:00,  2025,        0.5,             2190.0
        2025,               2024-07-01 02:00:00,  2025,        1.5,             6570.0
        2026,               2025-07-01 01:00:00,  2026,        1.0,             8760.0
    """)
    expected["timestep"] = pd.to_datetime(expected["timestep"])
    pd.testing.assert_frame_equal(result, expected)


def test_timesteps_of_an_operational_model_match_the_updated_network(
    csv_str_to_df, tmp_path
):
    """Given the operational snapshots, the table describes the timesteps the
    network has after update_network_timeseries, not the capacity expansion
    ones it was built with."""
    demand_dir = tmp_path / "demand_traces"
    demand_dir.mkdir()

    # Capacity expansion ran on two segments of unequal length.
    capacity_expansion_snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            objective,  generators,  stores,  model_year
        2025,                2025-01-01 00:30:00,  2190.0,     2190.0,      0.5,     2025
        2025,                2025-01-01 02:00:00,  6570.0,     6570.0,      1.5,     2025
    """)
    buses = csv_str_to_df("""
        name
        bus1
    """)
    generators = csv_str_to_df("""
        name,  carrier,  bus,   p_nom,  p_nom_extendable,  p_nom_max,  capital_cost,  marginal_cost
        gas,   Gas,      bus1,  200,    False,             200,        0,             50
    """)
    investment_period_weights = csv_str_to_df("""
        period,  years,  objective
        2025,    1,      1
    """)
    pypsa_friendly_inputs = {
        "snapshots": capacity_expansion_snapshots,
        "buses": buses,
        "generators": generators,
        "investment_period_weights": investment_period_weights,
        "custom_constraints_lhs": pd.DataFrame(),
        "custom_constraints_rhs": pd.DataFrame(),
        "custom_constraints_generators": pd.DataFrame(),
    }
    capacity_expansion_demand = csv_str_to_df("""
        investment_periods,  snapshots,            p_set
        2025,                2025-01-01 00:30:00,  100
        2025,                2025-01-01 02:00:00,  100
    """)
    capacity_expansion_demand["snapshots"] = pd.to_datetime(
        capacity_expansion_demand["snapshots"]
    )
    capacity_expansion_demand.to_parquet(demand_dir / "bus1.parquet")
    network = build_pypsa_network(pypsa_friendly_inputs, tmp_path)

    # The operational model runs on the four half-hours the segments covered.
    operational_snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            objective,  generators,  stores,  model_year
        2025,                2025-01-01 00:30:00,  2190.0,     2190.0,      0.5,     2025
        2025,                2025-01-01 01:00:00,  2190.0,     2190.0,      0.5,     2025
        2025,                2025-01-01 01:30:00,  2190.0,     2190.0,      0.5,     2025
        2025,                2025-01-01 02:00:00,  2190.0,     2190.0,      0.5,     2025
    """)
    operational_demand = csv_str_to_df("""
        investment_periods,  snapshots,            p_set
        2025,                2025-01-01 00:30:00,  100
        2025,                2025-01-01 01:00:00,  100
        2025,                2025-01-01 01:30:00,  100
        2025,                2025-01-01 02:00:00,  100
    """)
    operational_demand["snapshots"] = pd.to_datetime(operational_demand["snapshots"])
    operational_demand.to_parquet(demand_dir / "bus1.parquet")
    update_network_timeseries(
        network, pypsa_friendly_inputs, operational_snapshots, tmp_path
    )

    result = extract_timesteps(operational_snapshots)

    # 02:00 was a 1.5 hour segment in the capacity expansion model. In the
    # operational model it is a half-hour like the rest.
    expected = csv_str_to_df("""
        investment_period,  timestep,             model_year,  duration_hours,  hours_represented
        2025,               2025-01-01 00:30:00,  2025,        0.5,             2190.0
        2025,               2025-01-01 01:00:00,  2025,        0.5,             2190.0
        2025,               2025-01-01 01:30:00,  2025,        0.5,             2190.0
        2025,               2025-01-01 02:00:00,  2025,        0.5,             2190.0
    """)
    expected["timestep"] = pd.to_datetime(expected["timestep"])
    pd.testing.assert_frame_equal(result, expected)

    # The table's timesteps and lengths are the ones on the network.
    network_lengths = network.snapshot_weightings["stores"].reset_index()
    network_lengths.columns = ["investment_period", "timestep", "duration_hours"]
    pd.testing.assert_frame_equal(
        result[["investment_period", "timestep", "duration_hours"]], network_lengths
    )


def test_timesteps_merge_onto_dispatch_after_being_written_to_csv(
    csv_str_to_df, tmp_path
):
    """The plot tasks read the results back from CSV, where timestep is text.
    The timesteps table still has to match the other tables row for row."""
    snapshots = csv_str_to_df("""
        investment_periods,  snapshots,            objective,  generators,  stores,  model_year
        2025,                2024-07-01 00:30:00,  2190.0,     2190.0,      0.5,     2025
        2025,                2024-07-01 02:00:00,  6570.0,     6570.0,      1.5,     2025
    """)
    dispatch = csv_str_to_df("""
        generator,  investment_period,  timestep,             dispatch_mw
        gas,        2025,               2024-07-01 00:30:00,  100.0
        gas,        2025,               2024-07-01 02:00:00,  40.0
    """)
    dispatch["timestep"] = pd.to_datetime(dispatch["timestep"])

    results = {
        "generator_dispatch": dispatch,
        "timesteps": extract_timesteps(snapshots),
    }
    write_csvs(results, tmp_path)
    results = read_csvs(tmp_path)
    result = results["generator_dispatch"].merge(
        results["timesteps"], on=["investment_period", "timestep"]
    )

    expected = csv_str_to_df("""
        generator,  investment_period,  timestep,             dispatch_mw,  model_year,  duration_hours,  hours_represented
        gas,        2025,               2024-07-01 00:30:00,  100.0,        2025,        0.5,             2190.0
        gas,        2025,               2024-07-01 02:00:00,  40.0,         2025,        1.5,             6570.0
    """)
    pd.testing.assert_frame_equal(result, expected)
