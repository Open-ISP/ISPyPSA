import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from ispypsa.model import build_pypsa_network, update_network_timeseries


def test_expand_then_operate():
    """
    Test expanding the network with two snapshots per year for two single-year investment
    periods and then operating the two years with five snapshot in each year.

    The example uses:
    - A single bus
    - Three generators: solar, wind, and gas (fixed capacity)

    CAPACITY EXPANSION PHASE:
    - Two snapshots per investment period: one normal, one peak
    - Normal demand: 100MW in 2025, 120MW in 2026
    - Peak demand: 150MW in 2025, 180MW in 2026
    - Generator parameters:
        - Solar:
            - Capital cost 0.4 $/MW (very low to force build to displace gas)
            - marginal cost 0 $/MWh
            - availability 0.5 (50%) in normal, 0 in peak
            - Limited to 100MW max capacity
        - Wind:
            - Capital cost 0.5 $/MW (very low to force build to displace gas, but higher
              than solar)
            - marginal cost 0 $/MWh
            - availability 0.4 (40%) in normal 0 in peak
            - Limited to 200MW max capacity
        - Gas:
            - Fixed capacity of 200MW (not extendable)
            - marginal cost 100 $/MWh

    Expected capacity expansion results:
    - Solar: 100MW built (provides 50MW at 50% availability)
    - Wind: 175MW built (provides 70MW at 40% availability)
    - Gas: 200MW (fixed)

    OPERATIONAL PHASE:
    - Five snapshots per investment period with simplified patterns
    - Solar availability: [0, 0, 1, 0, 0] (only available at noon)
    - Wind availability: [1, 0, 0, 0, 1] (only available at night)
    - Demand: Flat 100MW in 2025, flat 120MW in 2026

    Expected operational results (generation):
    - Solar: [0, 0, 100, 0, 0] in both years
    - Wind:
        - 2025: [100, 0, 0, 0, 100]
        - 2026: [120, 0, 0, 0, 120]
    - Gas:
        - 2025: [0, 100, 0, 100, 0]
        - 2026: [0, 120, 20, 120, 0]
    """

    # Create temporary directory for the test
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)

        # Create directories for time series data
        solar_dir = temp_dir / "solar_traces"
        wind_dir = temp_dir / "wind_traces"
        demand_dir = temp_dir / "demand_traces"

        solar_dir.mkdir()
        wind_dir.mkdir()
        demand_dir.mkdir()

        # 1. Create capacity expansion snapshots (TWO per investment period - normal and peak)
        capacity_expansion_snapshots = pd.DataFrame(
            {
                "investment_periods": [2025, 2025, 2026, 2026],
                "snapshots": pd.to_datetime(
                    [
                        "2025-01-01 12:00",  # Normal snapshot
                        "2025-01-15 18:00",  # Peak snapshot
                        "2026-01-01 12:00",  # Normal snapshot
                        "2026-01-15 18:00",  # Peak snapshot
                    ],
                ),
                "generators": 1.0,
                "objective": 1.0,
                "stores": 1.0,
            }
        )

        # Make sure discount rate is effectively zero by using equal weights
        # This makes the optimization simple and deterministic

        # 2. Create PyPSA friendly input tables
        buses = pd.DataFrame(
            {
                "name": ["bus1"],
            }
        )

        generators = pd.DataFrame(
            {
                "name": ["solar", "wind", "gas"],
                "carrier": ["Solar", "Wind", "Gas"],
                "bus": ["bus1", "bus1", "bus1"],
                "p_nom": [0, 0, 200],  # Gas starts with 200MW capacity
                "p_nom_extendable": [True, True, False],  # Gas not extendable
                "p_nom_max": [100, 200, 200],  # Build limits
                "capital_cost": [
                    0.4,
                    0.5,
                    0,
                ],  # capital cost (very low so wind solar are built to displace gas)
                "marginal_cost": [0, 0, 100],  # Marginal costs in $/MWh
            }
        )

        investment_period_weights = pd.DataFrame(
            {
                "period": [2025, 2026],
                "years": [1, 1],
                "objective": [1, 1],  # Equal weights for both years (no discounting)
            }
        )

        # Empty custom constraints
        custom_constraints_lhs = pd.DataFrame()
        custom_constraints_rhs = pd.DataFrame()
        custom_constraints_generators = pd.DataFrame()

        # Compile all inputs
        pypsa_friendly_inputs = {
            "snapshots": capacity_expansion_snapshots,
            "buses": buses,
            "generators": generators,
            "investment_period_weights": investment_period_weights,
            "custom_constraints_lhs": custom_constraints_lhs,
            "custom_constraints_rhs": custom_constraints_rhs,
            "custom_constraints_generators": custom_constraints_generators,
        }

        # 3. Create time series data for capacity expansion
        # Simple time series data with deterministic values
        solar_cap_exp = pd.DataFrame(
            {
                "investment_periods": [2025, 2025, 2026, 2026],
                "snapshots": pd.to_datetime(
                    [
                        "2025-01-01 12:00",
                        "2025-01-15 18:00",
                        "2026-01-01 12:00",
                        "2026-01-15 18:00",
                    ]
                ),
                "p_max_pu": [0.5, 0.0, 0.5, 0.0],  # 50% normal, 0% peak
            }
        )

        wind_cap_exp = pd.DataFrame(
            {
                "investment_periods": [2025, 2025, 2026, 2026],
                "snapshots": pd.to_datetime(
                    [
                        "2025-01-01 12:00",
                        "2025-01-15 18:00",
                        "2026-01-01 12:00",
                        "2026-01-15 18:00",
                    ]
                ),
                "p_max_pu": [0.4, 0.0, 0.4, 0.0],  # 40% normal, 0% peak
            }
        )

        demand_cap_exp = pd.DataFrame(
            {
                "investment_periods": [2025, 2025, 2026, 2026],
                "snapshots": pd.to_datetime(
                    [
                        "2025-01-01 12:00",
                        "2025-01-15 18:00",
                        "2026-01-01 12:00",
                        "2026-01-15 18:00",
                    ]
                ),
                "p_set": [100, 150, 120, 180],  # Normal and peak demand
            }
        )

        # Save capacity expansion time series data
        solar_cap_exp.to_parquet(solar_dir / "solar.parquet")
        wind_cap_exp.to_parquet(wind_dir / "wind.parquet")
        demand_cap_exp.to_parquet(demand_dir / "bus1.parquet")

        # 4. Build PyPSA network for capacity expansion
        network = build_pypsa_network(pypsa_friendly_inputs, temp_dir)

        # 5. Run capacity expansion optimization
        network.optimize.solve_model(solver_name="highs")

        # Save the capacity expansion results
        cap_exp_results = {
            "solar": network.generators.loc["solar", "p_nom_opt"],
            "wind": network.generators.loc["wind", "p_nom_opt"],
            "gas": network.generators.loc["gas", "p_nom_opt"],
        }

        # 6. Create operational snapshots (5 per year)
        operational_snapshots = pd.DataFrame(
            {
                "investment_periods": np.repeat([2025, 2026], 5),
                "snapshots": pd.to_datetime(
                    [
                        # 2025 snapshots - midnight, 6am, noon, 6pm, midnight
                        "2025-01-01 00:00",
                        "2025-01-01 06:00",
                        "2025-01-01 12:00",
                        "2025-01-01 18:00",
                        "2025-01-02 00:00",
                        # 2026 snapshots - same times
                        "2026-01-01 00:00",
                        "2026-01-01 06:00",
                        "2026-01-01 12:00",
                        "2026-01-01 18:00",
                        "2026-01-02 00:00",
                    ]
                ),
                "generators": 1.0,
                "objective": 1.0,
                "stores": 1.0,
            }
        )

        # 7. Create operational time series data with simple deterministic patterns
        # Solar: only available at noon
        solar_op = pd.DataFrame(
            {
                "investment_periods": np.repeat([2025, 2026], 5),
                "snapshots": pd.to_datetime(
                    [
                        # 2025 snapshots
                        "2025-01-01 00:00",
                        "2025-01-01 06:00",
                        "2025-01-01 12:00",
                        "2025-01-01 18:00",
                        "2025-01-02 00:00",
                        # 2026 snapshots
                        "2026-01-01 00:00",
                        "2026-01-01 06:00",
                        "2026-01-01 12:00",
                        "2026-01-01 18:00",
                        "2026-01-02 00:00",
                    ]
                ),
                "p_max_pu": [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
                # Only at noon
            }
        )

        # Wind: only available at midnight
        wind_op = pd.DataFrame(
            {
                "investment_periods": np.repeat([2025, 2026], 5),
                "snapshots": pd.to_datetime(
                    [
                        # 2025 snapshots
                        "2025-01-01 00:00",
                        "2025-01-01 06:00",
                        "2025-01-01 12:00",
                        "2025-01-01 18:00",
                        "2025-01-02 00:00",
                        # 2026 snapshots
                        "2026-01-01 00:00",
                        "2026-01-01 06:00",
                        "2026-01-01 12:00",
                        "2026-01-01 18:00",
                        "2026-01-02 00:00",
                    ]
                ),
                "p_max_pu": [1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0],
                # Only at midnight
            }
        )

        # Demand: flat load profile
        demand_op = pd.DataFrame(
            {
                "investment_periods": np.repeat([2025, 2026], 5),
                "snapshots": pd.to_datetime(
                    [
                        # 2025 snapshots
                        "2025-01-01 00:00",
                        "2025-01-01 06:00",
                        "2025-01-01 12:00",
                        "2025-01-01 18:00",
                        "2025-01-02 00:00",
                        # 2026 snapshots
                        "2026-01-01 00:00",
                        "2026-01-01 06:00",
                        "2026-01-01 12:00",
                        "2026-01-01 18:00",
                        "2026-01-02 00:00",
                    ]
                ),
                "p_set": [
                    100,
                    100,
                    100,
                    100,
                    100,
                    120,
                    120,
                    120,
                    120,
                    120,
                ],  # Flat demand
            }
        )

        # Save operational time series data, overwriting the capacity expansion data
        solar_op.to_parquet(solar_dir / "solar.parquet")
        wind_op.to_parquet(wind_dir / "wind.parquet")
        demand_op.to_parquet(demand_dir / "bus1.parquet")

        # 8. Update network time series data
        update_network_timeseries(
            network, pypsa_friendly_inputs, operational_snapshots, temp_dir
        )

        # 9. Fix optimal capacities and run operational optimization
        network.optimize.fix_optimal_capacities()

        network.optimize.solve_model()

        # 10. Verify the results
        # 10.1 Check capacity expansion results
        np.testing.assert_allclose(
            cap_exp_results["solar"],
            100,
            rtol=1e-5,
            err_msg="Should build exactly 100 MW of solar (limited by p_nom_max)",
        )
        np.testing.assert_allclose(
            cap_exp_results["wind"],
            175,
            rtol=1e-5,
            err_msg="Should build exactly 175 MW of wind",
        )
        np.testing.assert_allclose(
            cap_exp_results["gas"],
            200,
            rtol=1e-5,
            err_msg="Gas should remain at fixed 200 MW capacity",
        )

        # 10.2 Check operational results
        solar_output = network.generators_t.p["solar"]
        wind_output = network.generators_t.p["wind"]
        gas_output = network.generators_t.p["gas"]

        # Expected generation patterns for each generator
        expected_solar = np.array([0, 0, 100, 0, 0, 0, 0, 100, 0, 0])
        expected_wind = np.array([100, 0, 0, 0, 100, 120, 0, 0, 0, 120])
        expected_gas = np.array([0, 100, 0, 100, 0, 0, 120, 20, 120, 0])

        # Test that generation follows expected patterns
        np.testing.assert_allclose(
            solar_output,
            expected_solar,
            rtol=1e-5,
            atol=1e-5,
            err_msg="Solar generation doesn't match expected pattern",
        )
        np.testing.assert_allclose(
            wind_output,
            expected_wind,
            rtol=1e-5,
            atol=1e-5,
            err_msg="Wind generation doesn't match expected pattern",
        )
        np.testing.assert_allclose(
            gas_output,
            expected_gas,
            rtol=1e-5,
            atol=1e-5,
            err_msg="Gas generation doesn't match expected pattern",
        )

        # 10.3 Verify that total generation matches demand
        demand = network.loads_t.p_set["load_bus1"]
        total_generation = solar_output + wind_output + gas_output

        np.testing.assert_allclose(
            total_generation,
            demand,
            rtol=1e-5,
            atol=1e-5,
            err_msg="Total generation doesn't match demand",
        )
