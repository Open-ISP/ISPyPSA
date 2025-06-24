import pandas as pd
import pytest

from ispypsa.translator import create_pypsa_friendly_inputs


def test_sample_ispypsa_tables_fixture(sample_ispypsa_tables):
    """Test that the sample_ispypsa_tables fixture contains all required tables."""
    # Check that all required tables are present
    expected_tables = [
        "sub_regions",
        "nem_regions",
        "renewable_energy_zones",
        "flow_paths",
        "ecaa_generators",
        "custom_constraints_lhs",
        "custom_constraints_rhs",
    ]
    
    for table_name in expected_tables:
        assert table_name in sample_ispypsa_tables
        assert isinstance(sample_ispypsa_tables[table_name], pd.DataFrame)
        assert not sample_ispypsa_tables[table_name].empty
    
    # Check specific table contents
    assert len(sample_ispypsa_tables["sub_regions"]) == 2  # CNSW and NNSW
    assert len(sample_ispypsa_tables["renewable_energy_zones"]) == 2  # 2 REZs
    assert len(sample_ispypsa_tables["ecaa_generators"]) == 6  # 2 coal + 4 REZ generators


def test_create_pypsa_friendly_inputs_with_fixture(sample_ispypsa_tables, sample_model_config):
    """Test that create_pypsa_friendly_inputs runs successfully with the fixture data."""
    # Run create_pypsa_friendly_inputs
    result = create_pypsa_friendly_inputs(sample_model_config, sample_ispypsa_tables)
    
    # Check that all expected outputs are present
    expected_outputs = [
        "snapshots",
        "investment_period_weights",
        "buses",
        "links",
        "generators",
        "custom_constraints_lhs",
        "custom_constraints_rhs",
    ]
    
    for output in expected_outputs:
        assert output in result
        assert isinstance(result[output], pd.DataFrame)
    
    # custom_constraints_generators is only created when rez_transmission_expansion is True
    if sample_model_config.network.rez_transmission_expansion:
        assert "custom_constraints_generators" in result
        assert isinstance(result["custom_constraints_generators"], pd.DataFrame)
    
    # Check specific results
    # Buses should include sub-regions and REZs
    assert len(result["buses"]) == 4  # 2 sub-regions + 2 REZs
    
    # Generators should include ECAA generators + unserved energy generators
    assert len(result["generators"]) >= 8  # 6 ECAA + 2 unserved energy
    
    # Links should include flow paths and REZ connections
    assert len(result["links"]) >= 3  # 1 flow path + 2 REZ connections
    
    # Custom constraints should be translated
    assert not result["custom_constraints_rhs"].empty
    assert not result["custom_constraints_lhs"].empty
    
    # Snapshots should be created based on config
    assert not result["snapshots"].empty
    assert "investment_periods" in result["snapshots"].columns
    
    # Investment period weights should be created
    assert not result["investment_period_weights"].empty
    assert len(result["investment_period_weights"]) == 2  # 2 investment periods