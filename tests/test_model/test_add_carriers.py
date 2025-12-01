import pandas as pd
import pypsa
import pytest

from ispypsa.pypsa_build.carriers import _add_carriers_to_network


@pytest.fixture
def mock_network():
    """Create a minimal PyPSA network for testing."""
    return pypsa.Network()


def test_add_carriers_to_network_with_data(mock_network, csv_str_to_df):
    """Test adding carriers to network with generator and storage data."""
    # Create test generators DataFrame
    generators_csv = """
    name,     bus,      carrier,   p_nom
    gen1,     bus1,     Wind,      100
    gen2,     bus2,     Solar,     200
    gen3,     bus3,     Gas,       300
    """
    generators = csv_str_to_df(generators_csv)

    # Create test storage DataFrame
    storage_csv = """
    name,     bus,      carrier,   p_nom,  max_hours
    bat1,     bus1,     Battery,   50,     4
    bat2,     bus2,     Battery,   75,     2
    """
    storage = csv_str_to_df(storage_csv)

    # Call function
    _add_carriers_to_network(mock_network, generators, storage)

    # Check all expected carriers are added
    expected_carriers = ["Wind", "Solar", "Gas", "Battery", "AC", "DC"]
    assert set(mock_network.carriers.index) == set(expected_carriers)


def test_add_carriers_to_network_one_input_source_only(mock_network, csv_str_to_df):
    """Test adding carriers to network with only generator data."""
    # Create test generators DataFrame
    generators_csv = """
    name,     bus,      carrier,   p_nom
    gen1,     bus1,     Wind,      100
    gen2,     bus2,     Coal,      200
    """
    generators = csv_str_to_df(generators_csv)

    # Call function with no storage
    _add_carriers_to_network(mock_network, generators, None)

    # Check expected carriers are added
    expected_carriers = ["Wind", "Coal", "AC", "DC"]
    assert set(mock_network.carriers.index) == set(expected_carriers)


def test_add_carriers_to_network_storage_only(mock_network, csv_str_to_df):
    """Test adding carriers to network with only storage data."""
    # Create test storage DataFrame
    storage_csv = """
    name,     bus,      carrier,   p_nom,  max_hours
    bat1,     bus1,     Battery,   50,     4
    bat2,     bus2,     Flow,      75,     6
    """
    storage = csv_str_to_df(storage_csv)

    # Call function with no generators
    _add_carriers_to_network(mock_network, None, storage)

    # Check expected carriers are added
    expected_carriers = ["Battery", "Flow", "AC", "DC"]
    assert set(mock_network.carriers.index) == set(expected_carriers)


def test_add_carriers_to_network_empty_dataframes(mock_network):
    """Test adding carriers to network with empty DataFrames."""
    # Create empty DataFrames
    generators = pd.DataFrame(columns=["carrier"])
    storage = pd.DataFrame(columns=["carrier"])

    # Call function
    _add_carriers_to_network(mock_network, generators, storage)

    # Check only standard carriers are added
    expected_carriers = ["AC", "DC"]
    assert set(mock_network.carriers.index) == set(expected_carriers)


def test_add_carriers_to_network_no_data(mock_network):
    """Test adding carriers to network with no data."""
    # Call function with None for both parameters
    _add_carriers_to_network(mock_network, None, None)

    # Check only standard carriers are added
    expected_carriers = ["AC", "DC"]
    assert set(mock_network.carriers.index) == set(expected_carriers)
