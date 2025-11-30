import pandas as pd
import pypsa
import pytest

from ispypsa.model.storage import (
    _add_batteries_to_network,
    _add_battery_to_network,
)


@pytest.fixture
def mock_network(csv_str_to_df):
    """Create a minimal PyPSA network for testing."""
    network = pypsa.Network()
    network.add("Bus", "test_bus")

    return network


def test_add_battery_to_network(mock_network):
    sample_battery = dict(
        name="test_battery",
        bus="test_bus",
        p_nom=100.0,
        p_nom_extendable=False,
        carrier="Battery",
        max_hours=2,
        capital_cost=0.0,
        build_year=2015,
        lifetime=20,
        efficiency_store=0.92,
        efficiency_dispatch=0.92,
        isp_resource_type="Battery Storage 2h",
        isp_rez_id="",
    )

    _add_battery_to_network(mock_network, sample_battery)

    # Check the battery was added correctly
    assert "test_battery" in mock_network.storage_units.index
    assert "isp_resource_type" not in mock_network.storage_units.columns
    assert mock_network.storage_units.at["test_battery", "max_hours"] == 2

    # Check that some key default values have been set:
    assert mock_network.storage_units.at["test_battery", "p_max_pu"] == 1
    assert mock_network.storage_units.at["test_battery", "p_min_pu"] == -1


def test_add_batteries_to_network(mock_network, csv_str_to_df):
    sample_batteries_csv = """
    name,           bus,        p_nom,  p_nom_extendable,   carrier,    max_hours,  capital_cost,   build_year, lifetime,   efficiency_store,   efficiency_dispatch,    isp_resource_type,      isp_rez_id
    test_battery_1, test_bus,   100.0,  False,              Battery,    2,          0.0,            2015,       20,         0.92,               0.92,                   Battery__Storage__2h,   rez_A
    test_battery_2, test_bus,   100.0,  False,              Battery,    4,          0.0,            2015,       20,         0.91,               0.91,                   Battery__Storage__4h,   rez_B
    new_entrant_1,  test_bus,   0.0,    True,               Battery,    1,          100000.0,       2015,       20,         0.92,               0.92,                   Battery__Storage__1h,
    new_entrant_2,  test_bus,   0.0,    True,               Battery,    8,          400000.0,       2015,       20,         0.93,               0.93,                   Battery__Storage__8h,
    """
    sample_batteries = csv_str_to_df(sample_batteries_csv)

    _add_batteries_to_network(mock_network, sample_batteries)

    # Check the batteries were all added correctly - check each feature:
    assert set(mock_network.storage_units.index) == set(
        [
            "test_battery_1",
            "test_battery_2",
            "new_entrant_1",
            "new_entrant_2",
        ]
    )
    assert (mock_network.storage_units.bus == "test_bus").all()
    assert (mock_network.storage_units.carrier == "Battery").all()

    characteristics_that_should_match_exactly = [
        "bus",
        "p_nom",
        "p_nom_extendable",
        "carrier",
        "max_hours",
        "capital_cost",
        "build_year",
        "lifetime",
        "efficiency_store",
        "efficiency_dispatch",
    ]
    sample_batteries = sample_batteries.set_index("name")
    for test_battery in sample_batteries.index:
        for characteristic in characteristics_that_should_match_exactly:
            assert (
                mock_network.storage_units.at[test_battery, characteristic]
                == sample_batteries.at[test_battery, characteristic]
            )

    # Check that some key default values have been set:
    assert (mock_network.storage_units.p_max_pu == 1).all()
    assert (mock_network.storage_units.p_min_pu == -1).all()
