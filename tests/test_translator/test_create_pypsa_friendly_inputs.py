from pathlib import Path

from ispypsa.config import load_config
from ispypsa.data_fetch import read_csvs
from ispypsa.templater import (
    create_ispypsa_inputs_template,
    list_templater_output_files,
    load_manually_extracted_tables,
)
from ispypsa.translator import (
    create_pypsa_friendly_inputs,
    list_translator_output_files,
)


def test_create_pypsa_inputs_template_sub_regions(workbook_table_cache_test_path: Path):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    manual_tables = load_manually_extracted_tables("6.0")
    config = load_config(Path(__file__).parent / Path("ispypsa_config.yaml"))
    template_tables = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manual_tables,
    )
    pypsa_tables = create_pypsa_friendly_inputs(config, template_tables)

    for table in list_translator_output_files():
        assert table in pypsa_tables.keys()

    assert "SQ" in pypsa_tables["buses"]["name"].values
    assert "Q1" in pypsa_tables["buses"]["name"].values


def test_create_pypsa_inputs_template_sub_regions_rezs_not_nodes(
    workbook_table_cache_test_path: Path,
):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    manual_tables = load_manually_extracted_tables("6.0")
    config = load_config(Path(__file__).parent / Path("ispypsa_config.yaml"))
    config.network.nodes.rezs = "attached_to_parent_node"
    template_tables = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manual_tables,
    )
    pypsa_tables = create_pypsa_friendly_inputs(config, template_tables)

    for table in list_translator_output_files():
        assert table in pypsa_tables.keys()

    assert "SQ" in pypsa_tables["buses"]["name"].values
    assert "Q1" not in pypsa_tables["buses"]["name"].values


def test_create_ispypsa_inputs_template_single_regions(
    workbook_table_cache_test_path: Path,
):
    iasr_tables = read_csvs(workbook_table_cache_test_path)
    manual_tables = load_manually_extracted_tables("6.0")
    config = load_config(Path(__file__).parent / Path("ispypsa_config.yaml"))
    config.network.nodes.regional_granularity = "single_region"
    config.network.nodes.rezs = "attached_to_parent_node"
    template_tables = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manual_tables,
    )
    pypsa_tables = create_pypsa_friendly_inputs(config, template_tables)

    for table in list_translator_output_files():
        assert table in pypsa_tables.keys()

    assert "NEM" in pypsa_tables["buses"]["name"].values
    assert pypsa_tables["lines"].empty
