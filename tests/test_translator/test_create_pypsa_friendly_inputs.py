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
    create_pypsa_friendly_snapshots,
    create_pypsa_friendly_timeseries_inputs,
    list_translator_output_files,
)
from ispypsa.translator.generators import (
    _translate_ecaa_generators,
    _translate_new_entrant_generators,
)
from ispypsa.translator.snapshots import (
    _add_investment_periods,
    _create_complete_snapshots_index,
)


class DummyConfigOne:
    """Simple dummy config class for testing."""

    def __init__(self):
        # Default configuration that can be modified by tests
        self.temporal = type(
            "obj",
            (object,),
            {
                "year_type": "fy",
                "range": type("obj", (object,), {"start_year": 2025, "end_year": 2026}),
                "capacity_expansion": type(
                    "obj",
                    (object,),
                    {
                        "resolution_min": 60,  # 60-minute intervals for capacity expansion
                        "investment_periods": [2025, 2026],  # Two investment periods
                        "reference_year_cycle": [2018],
                        "aggregation": type(
                            "obj", (object,), {"representative_weeks": [1]}
                        ),
                    },
                ),
                "operational": type(
                    "obj",
                    (object,),
                    {
                        "resolution_min": 30,  # 30-minute intervals for operational
                        "reference_year_cycle": [2018],
                        "horizon": 336,
                        "overlap": 48,
                        "aggregation": type(
                            "obj", (object,), {"representative_weeks": [1, 2]}
                        ),
                    },
                ),
            },
        )


def test_create_pypsa_friendly_snapshots_capacity_expansion():
    """Test create_pypsa_friendly_snapshots with capacity_expansion model_phase."""

    config = DummyConfigOne()

    # Call the function with capacity_expansion
    snapshots = create_pypsa_friendly_snapshots(config, "capacity_expansion")

    # Basic structure assertions
    assert isinstance(snapshots, pd.DataFrame)
    assert "snapshots" in snapshots.columns
    assert "investment_periods" in snapshots.columns

    # Check investment periods (should have both 2025 and 2026)
    assert set(snapshots["investment_periods"].unique()) == {2025, 2026}

    # Check timestamps (should be from the first week of the financial year 2025 and 2026)
    first_date = snapshots["snapshots"].min()
    assert first_date.year == 2024
    assert first_date.month == 7

    # Verify that capacity expansion parameters were used
    # 1. Check resolution (60-minute intervals)
    timestamps = snapshots["snapshots"].sort_values()
    assert (timestamps.iloc[1] - timestamps.iloc[0]).seconds == 60 * 60

    # 2. Check that we got the right number of snapshots:
    # 1 week per year × 2 years at 60-min intervals:
    # = 2 weeks × 7 days × 24 intervals = 336 snapshots
    assert len(snapshots) == 336


def test_create_pypsa_friendly_snapshots_operational():
    """Test create_pypsa_friendly_snapshots with operational model_phase."""

    config = DummyConfigOne()

    # Call the function with operational
    snapshots = create_pypsa_friendly_snapshots(config, "operational")

    # Basic structure assertions
    assert isinstance(snapshots, pd.DataFrame)
    assert "snapshots" in snapshots.columns
    assert "investment_periods" in snapshots.columns

    # For operational mode, there should only be one investment period (start year)
    # even though there are two investment periods in the capacity expansion config
    assert set(snapshots["investment_periods"].unique()) == {2025}

    # Check timestamps start in the right place
    first_date = snapshots["snapshots"].min()
    assert first_date.year == 2024
    assert first_date.month == 7

    # Verify that operational parameters were used
    # 1. Check resolution (30-minute intervals)
    timestamps = snapshots["snapshots"].sort_values()
    assert (timestamps.iloc[1] - timestamps.iloc[0]).seconds == 30 * 60

    # 2. Check that 2 representative weeks were used
    # 2 week per year × 2 years at 30-min intervals:
    # = 4 weeks × 7 days × 48 intervals = 1344 snapshots
    assert len(snapshots) == 1344


def test_create_pypsa_inputs_template_sub_regions(
    sample_model_config: ModelConfig,
    sample_ispypsa_tables: dict[str, pd.DataFrame],
):
    pypsa_tables = create_pypsa_friendly_inputs(
        sample_model_config, sample_ispypsa_tables
    )

    assert "CNSW" in pypsa_tables["buses"]["name"].values
    assert "Central-West Orana REZ" in pypsa_tables["buses"]["name"].values


def test_create_pypsa_inputs_template_sub_regions_rezs_not_nodes(
    sample_model_config: ModelConfig,
    sample_ispypsa_tables: dict[str, pd.DataFrame],
):
    sample_model_config.network.nodes.rezs = "attached_to_parent_node"
    pypsa_tables = create_pypsa_friendly_inputs(
        sample_model_config, sample_ispypsa_tables
    )

    for table in list_translator_output_files():
        assert table in pypsa_tables.keys()

    assert "CNSW" in pypsa_tables["buses"]["name"].values
    assert "Central-West Orana REZ" not in pypsa_tables["buses"]["name"].values


def test_create_pypsa_inputs_template_single_regions(
    sample_ispypsa_tables: dict[str, pd.DataFrame],
    sample_model_config: ModelConfig,
):
    sample_model_config.network.nodes.regional_granularity = "single_region"
    sample_model_config.network.nodes.rezs = "attached_to_parent_node"
    pypsa_tables = create_pypsa_friendly_inputs(
        sample_model_config, sample_ispypsa_tables
    )

    for table in list_translator_output_files():
        assert table in pypsa_tables.keys()

    assert "NEM" in pypsa_tables["buses"]["name"].values
    assert pypsa_tables["links"].empty


class DummyConfigTwo:
    """Simple dummy config class for testing."""

    def __init__(self):
        # Default configuration that can be modified by tests
        self.scenario = "Step Change"
        self.discount_rate = 0.05
        self.temporal = type(
            "obj",
            (object,),
            {
                "year_type": "fy",
                "range": type("obj", (object,), {"start_year": 2025, "end_year": 2025}),
                "path_to_parsed_traces": None,  # Will be set in the test
                "capacity_expansion": type(
                    "obj",
                    (object,),
                    {
                        "resolution_min": 60,
                        "investment_periods": [2025],
                        "reference_year_cycle": [2011],
                        "aggregation": {"representative_weeks": [1]},
                    },
                ),
                "operational": type(
                    "obj",
                    (object,),
                    {
                        "resolution_min": 30,
                        "reference_year_cycle": [2011],
                        "horizon": 336,
                        "overlap": 48,
                        "aggregation": type(
                            "obj", (object,), {"representative_weeks": [1, 2]}
                        ),
                    },
                ),
            },
        )
        self.network = type(
            "obj",
            (object,),
            {"nodes": type("obj", (object,), {"regional_granularity": "sub_regions"})},
        )


def test_create_pypsa_friendly_timeseries_inputs_capacity_expansion(
    tmp_path,
    sample_ispypsa_tables: dict[str, pd.DataFrame],
):
    """Test create_pypsa_friendly_timeseries_inputs for capacity expansion mode."""

    # Setup
    config = DummyConfigTwo()

    # Use the trace data that ships with the tests
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data")
    config.temporal.path_to_parsed_traces = parsed_trace_path

    # Create snapshots for capacity expansion (hourly)
    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2025,
        temporal_resolution_min=60,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    # only keep the generators that we have test traces for from the fixtures:
    ecaa_generators = sample_ispypsa_tables["ecaa_generators"]
    filtered_ecaa_generators = (
        ecaa_generators.loc[
            ecaa_generators["generator"].isin(
                ["Moree Solar Farm", "Bodangora Wind Farm", "Bayswater", "Eraring"]
            ),
            :,
        ]
        .copy()
        .reset_index()
    )
    sample_ispypsa_tables["ecaa_generators"] = filtered_ecaa_generators

    translated_ecaa_generators = _translate_ecaa_generators(
        sample_ispypsa_tables,
        config.temporal.capacity_expansion.investment_periods,
        config.network.nodes.regional_granularity,
        config.temporal.year_type,
    )

    translated_new_entrant_generators = _translate_new_entrant_generators(
        sample_ispypsa_tables,
        config.temporal.capacity_expansion.investment_periods,
        config.discount_rate,
        config.network.nodes.regional_granularity,
    )

    generators = pd.concat(
        [translated_ecaa_generators, translated_new_entrant_generators],
        axis=0,
        ignore_index=True,
    )

    # Create output directory
    output_dir = tmp_path / "timeseries_output"

    # Call the function
    create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        sample_ispypsa_tables,
        snapshots,
        generators,
        parsed_trace_path,
        output_dir,
    )

    # Verify outputs
    # 1. Check that solar_traces directory was created with the right files
    solar_dir = output_dir / "solar_traces"
    assert solar_dir.exists()
    assert (solar_dir / "Moree Solar Farm.parquet").exists()

    # 2. Check that wind_traces directory was created with the right files
    wind_dir = output_dir / "wind_traces"
    assert wind_dir.exists()
    assert (wind_dir / "Bodangora Wind Farm.parquet").exists()

    # 3. Check that marginal cost timeseries directory was created with the right files
    marginal_cost_dir = output_dir / "marginal_cost_timeseries"
    assert marginal_cost_dir.exists()
    assert (marginal_cost_dir / "ccgt_cnsw.parquet").exists()
    assert (marginal_cost_dir / "ocgt_small_gt_cnsw.parquet").exists()
    assert (marginal_cost_dir / "large_scale_solar_pv_n3_sat.parquet").exists()
    assert (marginal_cost_dir / "wind_n3_wh.parquet").exists()
    assert (marginal_cost_dir / "large_scale_solar_pv_n2_sat.parquet").exists()
    assert (marginal_cost_dir / "wind_n2_wh.parquet").exists()
    assert (marginal_cost_dir / "bayswater.parquet").exists()
    assert (marginal_cost_dir / "eraring.parquet").exists()
    assert (marginal_cost_dir / "bodangora_wind_farm.parquet").exists()
    assert (marginal_cost_dir / "moree_solar_farm.parquet").exists()

    # 4. Check that demand_traces directory was created with the right files
    demand_dir = output_dir / "demand_traces"
    assert demand_dir.exists()
    assert (demand_dir / "CNSW.parquet").exists()
    assert (demand_dir / "NNSW.parquet").exists()

    # 5. Load and check content of one of the generation files to verify basic structure
    solar_trace = pd.read_parquet(solar_dir / "Moree Solar Farm.parquet")

    # Check structure of the output
    assert "snapshots" in solar_trace.columns
    assert "p_max_pu" in solar_trace.columns
    assert "investment_periods" in solar_trace.columns

    # Verify matching of snapshots to investment periods
    assert set(solar_trace["investment_periods"].unique()) == {2025}

    # 6. And do the same for one of the marginal cost files
    marginal_cost_trace = pd.read_parquet(marginal_cost_dir / "bayswater.parquet")

    # Check structure of the output
    assert "snapshots" in marginal_cost_trace.columns
    assert "marginal_cost" in marginal_cost_trace.columns
    assert "investment_periods" in marginal_cost_trace.columns

    # Verify matching of snapshots to investment periods
    assert set(marginal_cost_trace["investment_periods"].unique()) == {2025}


def test_create_pypsa_friendly_timeseries_inputs_operational(
    tmp_path,
    sample_ispypsa_tables: dict[str, pd.DataFrame],
):
    """Test create_pypsa_friendly_timeseries_inputs for operational mode."""

    # Setup
    config = DummyConfigTwo()

    # Use the trace data that ships with the tests
    parsed_trace_path = Path(__file__).parent.parent / Path("trace_data")
    config.temporal.path_to_parsed_traces = parsed_trace_path

    # only keep the generators that we have test traces for from the fixtures:
    ecaa_generators = sample_ispypsa_tables["ecaa_generators"]
    filtered_ecaa_generators = (
        ecaa_generators.loc[
            ecaa_generators["generator"].isin(
                ["Moree Solar Farm", "Bodangora Wind Farm", "Bayswater", "Eraring"]
            ),
            :,
        ]
        .copy()
        .reset_index()
    )
    sample_ispypsa_tables["ecaa_generators"] = filtered_ecaa_generators

    translated_ecaa_generators = _translate_ecaa_generators(
        sample_ispypsa_tables,
        config.temporal.capacity_expansion.investment_periods,
        config.network.nodes.regional_granularity,
        config.temporal.year_type,
    )

    translated_new_entrant_generators = _translate_new_entrant_generators(
        sample_ispypsa_tables,
        config.temporal.capacity_expansion.investment_periods,
        config.discount_rate,
        config.network.nodes.regional_granularity,
    )

    generators = pd.concat(
        [translated_ecaa_generators, translated_new_entrant_generators],
        axis=0,
        ignore_index=True,
    )

    # Create snapshots for operational model (half-hourly)
    snapshots = _create_complete_snapshots_index(
        start_year=2025,
        end_year=2025,  # Just one year for operational
        temporal_resolution_min=30,
        year_type="fy",
    )

    snapshots = _add_investment_periods(snapshots, [2025], "fy")

    # Create output directory
    output_dir = tmp_path / "timeseries_output"

    # Call the function
    create_pypsa_friendly_timeseries_inputs(
        config,
        "operational",
        sample_ispypsa_tables,
        snapshots,
        generators,
        parsed_trace_path,
        output_dir,
    )

    # Verify outputs
    # 1. Check that solar_traces directory was created with the right files
    solar_dir = output_dir / "solar_traces"
    assert solar_dir.exists()
    assert (solar_dir / "Moree Solar Farm.parquet").exists()

    # 2. Check that wind_traces directory was created with the right files
    wind_dir = output_dir / "wind_traces"
    assert wind_dir.exists()
    assert (wind_dir / "Bodangora Wind Farm.parquet").exists()

    # 3. Check that marginal cost timeseries directory was created with the right files
    marginal_cost_dir = output_dir / "marginal_cost_timeseries"
    assert marginal_cost_dir.exists()
    assert (marginal_cost_dir / "ccgt_cnsw.parquet").exists()
    assert (marginal_cost_dir / "ocgt_small_gt_cnsw.parquet").exists()
    assert (marginal_cost_dir / "large_scale_solar_pv_n3_sat.parquet").exists()
    assert (marginal_cost_dir / "wind_n3_wh.parquet").exists()
    assert (marginal_cost_dir / "large_scale_solar_pv_n2_sat.parquet").exists()
    assert (marginal_cost_dir / "wind_n2_wh.parquet").exists()
    assert (marginal_cost_dir / "bayswater.parquet").exists()
    assert (marginal_cost_dir / "eraring.parquet").exists()
    assert (marginal_cost_dir / "bodangora_wind_farm.parquet").exists()
    assert (marginal_cost_dir / "moree_solar_farm.parquet").exists()

    # 4. Check that demand_traces directory was created with the right files
    demand_dir = output_dir / "demand_traces"
    assert demand_dir.exists()
    assert (demand_dir / "CNSW.parquet").exists()
    assert (demand_dir / "NNSW.parquet").exists()

    # 5. Load and check content of one of the files to verify basic structure
    solar_trace = pd.read_parquet(solar_dir / "Moree Solar Farm.parquet")

    # Check structure of the output
    assert "snapshots" in solar_trace.columns
    assert "p_max_pu" in solar_trace.columns
    assert "investment_periods" in solar_trace.columns

    # Verify only one investment period for operational
    assert set(solar_trace["investment_periods"].unique()) == {2025}

    # 6. And do the same for one of the marginal cost files
    marginal_cost_trace = pd.read_parquet(marginal_cost_dir / "bayswater.parquet")

    # Check structure of the output
    assert "snapshots" in marginal_cost_trace.columns
    assert "marginal_cost" in marginal_cost_trace.columns
    assert "investment_periods" in marginal_cost_trace.columns

    # Verify matching of snapshots to investment periods
    assert set(marginal_cost_trace["investment_periods"].unique()) == {2025}
