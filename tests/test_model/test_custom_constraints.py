from datetime import datetime
from pathlib import Path

import pandas as pd

from ispypsa.data_fetch import read_csvs
from ispypsa.pypsa_build import build_pypsa_network
from ispypsa.pypsa_build.custom_constraints import _add_custom_constraints


def test_custom_constraints():
    start_date = datetime(year=2025, month=1, day=1, hour=0, minute=0)
    end_date = datetime(year=2025, month=1, day=2, hour=0, minute=0)

    snapshots = pd.date_range(
        start=start_date, end=end_date, freq="30min", name="snapshots"
    )

    snapshots = pd.DataFrame(
        {
            "investment_periods": 2025,
            "snapshots": snapshots,
        }
    )
    pypsa_friendly_inputs_location = Path(
        "tests/test_model/test_pypsa_friendly_inputs/test_custom_constraints"
    )
    snapshots.to_csv(pypsa_friendly_inputs_location / Path("snapshots.csv"))

    pypsa_friendly_inputs = read_csvs(pypsa_friendly_inputs_location)

    demand_data = snapshots.copy()
    demand_data["p_set"] = 1000.0
    demand_data.to_parquet(
        pypsa_friendly_inputs_location / Path("demand_traces/bus_two.parquet")
    )

    network = build_pypsa_network(pypsa_friendly_inputs, pypsa_friendly_inputs_location)

    network.optimize.solve_model()

    assert network.generators.loc["con_one-EXPANSION", "p_nom_opt"] == 1500.0


def test_custom_constraints_greater_equal(csv_str_to_df):
    """Test custom constraints with >= constraint type for Generator p_nom variables."""
    import pypsa

    # Create a simple network
    network = pypsa.Network()
    network.set_snapshots(pd.date_range("2025-01-01", periods=4, freq="h"))

    # Add buses
    network.add("Bus", "bus1")
    network.add("Bus", "bus2")

    # Add generators with extendable capacity
    network.add(
        "Generator",
        "gen1",
        bus="bus1",
        p_nom_extendable=True,
        capital_cost=100,
        marginal_cost=50,
    )
    network.add(
        "Generator",
        "gen2",
        bus="bus2",
        p_nom_extendable=True,
        capital_cost=150,
        marginal_cost=40,
    )

    # Add loads
    network.loads_t.p_set = pd.DataFrame(
        {"load1": [100, 150, 200, 250]}, index=network.snapshots
    )
    network.add("Load", "load1", bus="bus1")

    # Define custom constraints using csv_str_to_df
    custom_constraints_rhs_csv = """
    constraint_name,    rhs,    constraint_type
    min_gen_capacity,   300,    >=
    """

    custom_constraints_lhs_csv = """
    constraint_name,     component,   attribute,   variable_name,   coefficient
    min_gen_capacity,    Generator,   p_nom,       gen1,            1.0
    min_gen_capacity,    Generator,   p_nom,       gen2,            1.0
    """

    custom_constraints_rhs = csv_str_to_df(custom_constraints_rhs_csv)
    custom_constraints_lhs = csv_str_to_df(custom_constraints_lhs_csv)

    # Apply custom constraints and solve
    network.optimize.create_model()
    _add_custom_constraints(network, custom_constraints_rhs, custom_constraints_lhs)
    network.optimize.solve_model()

    # Check that the sum of generator capacities is at least 300
    total_capacity = network.generators.p_nom_opt.sum()
    assert total_capacity >= 300.0


def test_custom_constraints_equal_link_p_nom(csv_str_to_df):
    """Test custom constraints with == constraint type for Link p_nom variables."""
    import pypsa

    # Create a simple network
    network = pypsa.Network()
    network.set_snapshots(pd.date_range("2025-01-01", periods=4, freq="h"))

    # Add buses
    network.add("Bus", "bus1")
    network.add("Bus", "bus2")
    network.add("Bus", "bus3")

    # Add links with extendable capacity
    network.add(
        "Link",
        "link1",
        bus0="bus1",
        bus1="bus2",
        p_nom_extendable=True,
        capital_cost=1000,
        efficiency=0.95,
    )
    network.add(
        "Link",
        "link2",
        bus0="bus2",
        bus1="bus3",
        p_nom_extendable=True,
        capital_cost=1200,
        efficiency=0.95,
    )

    # Add generators and loads
    network.add("Generator", "gen1", bus="bus1", p_nom=500, marginal_cost=30)
    network.loads_t.p_set = pd.DataFrame(
        {"load2": [50, 100, 150, 200], "load3": [100, 150, 200, 250]},
        index=network.snapshots,
    )
    network.add("Load", "load2", bus="bus1")
    network.add("Load", "load3", bus="bus3")

    # Define custom constraints - link capacities must be equal
    custom_constraints_rhs_csv = """
    constraint_name,         rhs,    constraint_type
    equal_link_capacity,     0,      ==
    """

    custom_constraints_lhs_csv = """
    constraint_name,         component,   attribute,   variable_name,   coefficient
    equal_link_capacity,     Link,        p_nom,       link1,           1.0
    equal_link_capacity,     Link,        p_nom,       link2,           -1.0
    """

    custom_constraints_rhs = csv_str_to_df(custom_constraints_rhs_csv)
    custom_constraints_lhs = csv_str_to_df(custom_constraints_lhs_csv)

    # Apply custom constraints and solve
    network.optimize.create_model()
    _add_custom_constraints(network, custom_constraints_rhs, custom_constraints_lhs)
    network.optimize.solve_model()

    # Check that link capacities are equal
    assert (
        abs(
            network.links.loc["link1", "p_nom_opt"]
            - network.links.loc["link2", "p_nom_opt"]
        )
        < 0.01
    )


def test_custom_constraints_mixed_components(csv_str_to_df):
    """Test custom constraints with mixed component types (Generator p and Link p) with <= constraint."""
    import pypsa

    # Create a simple network
    network = pypsa.Network()
    network.set_snapshots(pd.date_range("2025-01-01", periods=4, freq="h"))

    # Add carriers
    network.add("Carrier", "AC")
    network.add("Carrier", "gas")

    # Add buses
    network.add("Bus", "bus1")
    network.add("Bus", "bus2")

    # Add generators
    network.add(
        "Generator", "gen1", bus="bus1", p_nom=200, marginal_cost=1, carrier="gas"
    )
    network.add(
        "Generator", "gen2", bus="bus2", p_nom=150, marginal_cost=35, carrier="gas"
    )

    # Add link
    network.add(
        "Link",
        "link1",
        bus0="bus1",
        bus1="bus2",
        p_nom=200,
        efficiency=0.95,
        carrier="AC",
    )

    # Add loads
    network.loads_t.p_set = pd.DataFrame(
        {
            "load1": [10, 10, 10, 10],
            "load2": [200, 200, 200, 200],
        },
        index=network.snapshots,
    )
    network.add("Load", "load1", bus="bus1")
    network.add("Load", "load2", bus="bus2")

    # Define custom constraints - sum of gen1 and link1 power at each timestep <= 150
    custom_constraints_rhs_csv = """
    constraint_name,         rhs,    constraint_type
    max_combined_power,      150,    <=
    """

    custom_constraints_lhs_csv = """
    constraint_name,         component,   attribute,   variable_name,   coefficient
    max_combined_power,      Generator,   p,           gen1,            1.0
    max_combined_power,      Link,        p,           link1,           1.0
    """

    custom_constraints_rhs = csv_str_to_df(custom_constraints_rhs_csv)
    custom_constraints_lhs = csv_str_to_df(custom_constraints_lhs_csv)

    # Apply custom constraints and solve
    network.optimize.create_model()
    _add_custom_constraints(network, custom_constraints_rhs, custom_constraints_lhs)
    network.optimize.solve_model()

    # Check that combined power does not exceed 150 at any timestep
    gen_power = network.generators_t.p["gen1"]
    link_power = network.links_t.p0["link1"]
    combined_power = gen_power + link_power
    assert combined_power.max() <= 150.01  # Small tolerance for numerical precision
    assert combined_power.max() >= 149.9  # Small tolerance for numerical precision


def test_custom_constraints_multiple_types(csv_str_to_df):
    """Test multiple custom constraints with different constraint types."""
    import pypsa

    # Create a simple network
    network = pypsa.Network()
    network.set_snapshots(pd.date_range("2025-01-01", periods=4, freq="h"))

    # Add buses
    network.add("Bus", "bus1")
    network.add("Bus", "bus2")
    network.add("Bus", "bus3")

    # Add generators with extendable capacity
    network.add(
        "Generator",
        "gen1",
        bus="bus1",
        p_nom_extendable=True,
        capital_cost=100,
        marginal_cost=50,
    )
    network.add(
        "Generator",
        "gen2",
        bus="bus2",
        p_nom_extendable=True,
        capital_cost=120,
        marginal_cost=45,
    )

    # Add links with extendable capacity
    network.add(
        "Link",
        "link1",
        bus0="bus1",
        bus1="bus3",
        p_nom_extendable=True,
        capital_cost=1000,
        efficiency=0.95,
    )
    network.add(
        "Link",
        "link2",
        bus0="bus2",
        bus1="bus3",
        p_nom_extendable=True,
        capital_cost=1100,
        efficiency=0.95,
    )

    # Add loads
    network.loads_t.p_set = pd.DataFrame(
        {
            "load1": [100, 150, 200, 250],
            "load2": [80, 120, 160, 200],
            "load3": [150, 200, 200, 200],
        },
        index=network.snapshots,
    )
    network.add("Load", "load1", bus="bus1")
    network.add("Load", "load2", bus="bus2")
    network.add("Load", "load3", bus="bus3")

    # Define multiple custom constraints with different types
    custom_constraints_rhs_csv = """
    constraint_name,         rhs,    constraint_type
    min_total_gen,           400,    >=
    max_link_sum,            300,    <=
    gen_ratio,               0,      ==
    """

    custom_constraints_lhs_csv = """
    constraint_name,    component,   attribute,   variable_name,   coefficient
    min_total_gen,      Generator,   p_nom,       gen1,            1.0
    min_total_gen,      Generator,   p_nom,       gen2,            1.0
    max_link_sum,       Link,        p_nom,       link1,           1.0
    max_link_sum,       Link,        p_nom,       link2,           1.0
    gen_ratio,          Generator,   p_nom,       gen1,            1.0
    gen_ratio,          Generator,   p_nom,       gen2,            -1.2
    """

    custom_constraints_rhs = csv_str_to_df(custom_constraints_rhs_csv)
    custom_constraints_lhs = csv_str_to_df(custom_constraints_lhs_csv)

    # Apply custom constraints and solve
    network.optimize.create_model()
    _add_custom_constraints(network, custom_constraints_rhs, custom_constraints_lhs)
    network.optimize.solve_model()

    # Check all constraints are satisfied
    # 1. Minimum total generation capacity >= 400
    total_gen = network.generators.p_nom_opt.sum()
    assert total_gen >= 399.99  # Small tolerance

    # 2. Maximum link capacity sum <= 500
    total_link = network.links.p_nom_opt.sum()
    assert total_link <= 300.01  # Small tolerance
    assert total_link <= 299.9  # Small tolerance

    # 3. Generator ratio: gen1 = 1.2 * gen2
    gen1_cap = network.generators.loc["gen1", "p_nom_opt"]
    gen2_cap = network.generators.loc["gen2", "p_nom_opt"]
    assert abs(gen1_cap - 1.2 * gen2_cap) < 0.01  # Small tolerance
