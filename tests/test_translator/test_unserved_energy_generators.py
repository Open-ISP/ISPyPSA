from pathlib import Path

import pandas as pd

from ispypsa.config import ModelConfig, load_config
from ispypsa.data_fetch import read_csvs
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_inputs,
)
from ispypsa.translator.generators import _create_unserved_energy_generators


def test_unserved_energy_generator_creation(
    sample_ispypsa_tables: dict[str, pd.DataFrame],
    sample_model_config: ModelConfig,
):
    """Test that unserved energy generators are created when cost is specified."""
    # Set unserved energy cost for testing
    sample_model_config.unserved_energy.cost = 10000.0
    sample_model_config.unserved_energy.generator_size_mw = 5000.0

    pypsa_tables = create_pypsa_friendly_inputs(
        sample_model_config, sample_ispypsa_tables
    )

    # Check for unserved energy generators
    generators = pypsa_tables["generators"]
    unserved_generators = generators[generators["carrier"] == "Unserved Energy"]

    # Should be one generator per bus
    # In this specific test data there are only 2 sub_regions
    assert len(unserved_generators) == 2

    # Check properties of unserved generators
    for _, gen in unserved_generators.iterrows():
        assert gen["name"].startswith("unserved_energy_")
        assert gen["p_nom"] == 5000.0
        assert gen["p_nom_extendable"] == False
        assert gen["marginal_cost"].startswith("unserved_energy_")
        assert gen["isp_vom_$/mwh_sent_out"] == 10000.0
        assert gen["bus"] in pypsa_tables["buses"]["name"].values


def test_no_unserved_energy_generators_when_cost_is_none(
    sample_ispypsa_tables: dict[str, pd.DataFrame],
    sample_model_config: ModelConfig,
):
    """Test that no unserved energy generators are created when cost is None."""
    # Ensure unserved energy cost is None
    sample_model_config.unserved_energy.cost = None

    pypsa_tables = create_pypsa_friendly_inputs(
        sample_model_config, sample_ispypsa_tables
    )

    # Check that no unserved energy generators exist
    generators = pypsa_tables["generators"]
    unserved_generators = generators[generators["carrier"] == "Unserved Energy"]

    assert len(unserved_generators) == 0


def test_create_unserved_energy_generators():
    """Test the _create_unserved_energy_generators function directly."""
    buses = pd.DataFrame({"name": ["bus1", "bus2", "bus3"]})

    # Test with cost specified
    unserved_generators = _create_unserved_energy_generators(buses, 5000.0, 1000.0)
    assert len(unserved_generators) == 3
    assert all(unserved_generators["isp_vom_$/mwh_sent_out"] == 5000.0)
    assert all(unserved_generators["p_nom"] == 1000.0)
    assert all(unserved_generators["carrier"] == "Unserved Energy")
