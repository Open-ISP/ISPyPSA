from pathlib import Path
from typing import Literal

import pandas as pd
from isp_trace_parser import construct_reference_year_mapping

from ispypsa.config import (
    ModelConfig,
)
from ispypsa.translator.buses import (
    _create_single_region_bus,
    _translate_isp_sub_regions_to_buses,
    _translate_nem_regions_to_buses,
    _translate_rezs_to_buses,
    create_pypsa_friendly_bus_demand_timeseries,
)
from ispypsa.translator.custom_constraints import (
    _append_if_not_empty,
    _translate_custom_constraints,
)
from ispypsa.translator.generators import (
    _create_unserved_energy_generators,
    _translate_ecaa_generators,
    _translate_new_entrant_generators,
    create_pypsa_friendly_dynamic_marginal_costs,
    create_pypsa_friendly_ecaa_generator_timeseries,
    create_pypsa_friendly_new_entrant_generator_timeseries,
)
from ispypsa.translator.helpers import convert_to_numeric_if_possible
from ispypsa.translator.links import _translate_flow_paths_to_links
from ispypsa.translator.renewable_energy_zones import (
    _translate_renewable_energy_zone_build_limits_to_links,
)
from ispypsa.translator.snapshots import (
    _create_investment_period_weightings,
    create_pypsa_friendly_snapshots,
)
from ispypsa.translator.temporal_filters import _time_series_filter
from ispypsa.translator.time_series_checker import _check_time_series

_BASE_TRANSLATOR_OUTPUTS = [
    "snapshots",
    "investment_period_weights",
    "buses",
    "links",
    "generators",
    "custom_constraints_lhs",
    "custom_constraints_rhs",
    "custom_constraints_generators",
]


def create_pypsa_friendly_inputs(
    config: ModelConfig, ispypsa_tables: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Creates a set of tables for defining a `PyPSA` network from a set `ISPyPSA` tables.

    Examples:
        Perform requried imports.
        >>> from pathlib import Path
        >>> from ispypsa.config import load_config
        >>> from ispypsa.data_fetch import read_csvs, write_csvs
        >>> from ispypsa.translator import create_pypsa_friendly_inputs

        Load ISPyPSA model config file and input tables.
        >>> config = load_config(Path("ispypsa_config.yaml"))
        >>> ispypsa_input_tables = read_csvs(Path("ispypsa_inputs_directory"))

        Make the PyPSA friendly inputs!
        >>> pypsa_friendly_inputs = create_pypsa_friendly_inputs(
        ... config=config,
        ... ispypsa_tables=ispypsa_input_tables
        ... )

        Write the resulting dataframes to CSVs.
        >>> write_csvs(pypsa_friendly_inputs)

    Args:
        config: `ISPyPSA` `ispypsa.config.ModelConfig` object (add link to config docs).
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).

    Returns: dictionary of dataframes in the `PyPSA` friendly format. (add link to
        pypsa friendly format table docs)
    """
    pypsa_inputs = {}

    pypsa_inputs["investment_period_weights"] = _create_investment_period_weightings(
        config.temporal.capacity_expansion.investment_periods,
        config.temporal.range.end_year,
        config.discount_rate,
    )

    translated_generators = []
    translated_ecaa_generators = _translate_ecaa_generators(
        ispypsa_tables,
        config.temporal.capacity_expansion.investment_periods,
        config.network.nodes.regional_granularity,
        config.network.nodes.rezs,
        config.temporal.year_type,
    )
    _append_if_not_empty(translated_generators, translated_ecaa_generators)

    translated_new_entrant_generators = _translate_new_entrant_generators(
        ispypsa_tables,
        config.temporal.capacity_expansion.investment_periods,
        config.discount_rate,
        config.network.nodes.regional_granularity,
        config.network.nodes.rezs,
    )
    _append_if_not_empty(translated_generators, translated_new_entrant_generators)

    if len(translated_generators) > 0:
        pypsa_inputs["generators"] = pd.concat(
            translated_generators,
            axis=0,
            ignore_index=True,
        )
    else:
        # TODO: Log, improve error message (/ is this the right place for the error?)
        raise ValueError("No generator data returned from translator.")

    buses = []
    links = []

    if config.network.nodes.regional_granularity == "sub_regions":
        buses.append(_translate_isp_sub_regions_to_buses(ispypsa_tables["sub_regions"]))
    elif config.network.nodes.regional_granularity == "nem_regions":
        buses.append(_translate_nem_regions_to_buses(ispypsa_tables["nem_regions"]))
    elif config.network.nodes.regional_granularity == "single_region":
        buses.append(_create_single_region_bus())

    if config.unserved_energy.cost is not None:
        unserved_energy_generators = _create_unserved_energy_generators(
            buses[0],  # create generators for just demand buses not rez buses too.
            config.unserved_energy.cost,
            config.unserved_energy.max_per_node,
        )
        pypsa_inputs["generators"] = pd.concat(
            [pypsa_inputs["generators"], unserved_energy_generators], ignore_index=True
        )

    if config.network.nodes.rezs == "discrete_nodes":
        buses.append(_translate_rezs_to_buses(ispypsa_tables["renewable_energy_zones"]))
        links.append(
            _translate_renewable_energy_zone_build_limits_to_links(
                ispypsa_tables["renewable_energy_zones"],
                ispypsa_tables["rez_transmission_expansion_costs"],
                config,
            )
        )

    if config.network.nodes.regional_granularity != "single_region":
        links.append(_translate_flow_paths_to_links(ispypsa_tables, config))

    pypsa_inputs["buses"] = pd.concat(buses)

    if len(links) > 0:
        pypsa_inputs["links"] = pd.concat(links)
    else:
        pypsa_inputs["links"] = pd.DataFrame()

    pypsa_inputs.update(
        _translate_custom_constraints(
            config, ispypsa_tables, pypsa_inputs["links"], pypsa_inputs["generators"]
        )
    )

    return pypsa_inputs


def create_pypsa_friendly_timeseries_inputs(
    config: ModelConfig,
    model_phase: Literal["capacity_expansion", "operational"],
    ispypsa_tables: dict[str, pd.DataFrame],
    generators: pd.DataFrame,
    parsed_traces_directory: Path,
    pypsa_friendly_timeseries_inputs_location: Path,
    snapshots: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Creates snapshots and timeseries data files in PyPSA friendly format for generation
    and demand.

    - First creates snapshots based on the temporal configuration, optionally using
      named_representative_weeks and/or representative_weeks if configured. If snapshots
      are provided, they are used instead of generating new ones.

    - a time series file is created for each wind and solar generator in the new_entrant_generators
    table (table in ispypsa_tables dict). The time series data is saved in parquet files
    in the 'solar_traces' and 'wind_traces' directories with the columns "snapshots"
    (datetime) and "p_max_pu" (float specifying availability in MW).

    - a time series file is created for each generator in the translated generators table
    (table in pypsa_inputs dict) containing the marginal costs for each generator in each
    snapshot. The time series data is saved in parquet files in the 'marginal_costs' directory
    with the columns "snapshots" (datetime) and "marginal_cost" (float specifying marginal
    cost in $/MWh).

    - a time series file is created for each model region specifying the load in that
    region (regions set by config.network.nodes.regional_granularity). The time series
    data is saved in parquet files in the 'demand_traces' directory with the columns
    "snapshots" (datetime) and "p_set" (float specifying load in MW).

    Examples:

        >>> from pathlib import Path
        >>> from ispypsa.config import load_config
        >>> from ispypsa.data_fetch import read_csvs
        >>> from ispypsa.translator import (
        ...      create_pypsa_friendly_timeseries_inputs
        ... )

        Get a ISPyPSA ModelConfig instance

        >>> config = load_config(Path("path/to/config/file.yaml"))

        Get ISPyPSA inputs (in particular these need to contain the ecaa_generators,
        new_entrant_generators and sub_regions tables).

        >>> ispypsa_tables = read_csvs(Path("path/to/ispypsa/inputs"))

        Define which phase of the modelling we need the time series data for.

        >>> model_phase = "capacity_expansion"

        Now create the complete set of time series files and get the snapshots.

        >>> snapshots = create_pypsa_friendly_timeseries_inputs(
        ...     config,
        ...     model_phase,
        ...     ispypsa_tables,
        ...     Path("path/to/parsed/isp/traces"),
        ...     Path("path/to/write/time/series/inputs/to")
        ... )

    Args:
        config: ispypsa.ModelConfig instance
        model_phase: string defining whether the snapshots are for the operational or
            capacity expansion phase of the modelling. This allows the correct temporal
            config inputs to be used from the ModelConfig instance.
        ispypsa_tables: dict of pd.DataFrames defining the ISPyPSA input tables.
            In particular the dict needs to contain the ecaa_generators, new_entrant_generators
            and sub_regions tables, as well as fuel cost tables for fuel types present in
            either generator table - the other tables aren't required for the time series
            data creation. The ecaa_generators and new_entrant_generators tables need
            the columns 'generator' or 'generator_name' respectively (name or generator
            as str) and 'fuel_type' (str with 'Wind' and 'Solar' fuel types as appropriate).
            The sub_regions table needs to have the columns 'isp_sub_region_id' (str) and
            'nem_region_id' (str) if a 'regional' granularity is used.
        snapshots: a pd.DataFrame with the columns 'period' (int) and 'snapshots'
            (datetime) defining the time intervals and coresponding investment periods
            to be modelled.
        generators: a pd.DataFrame containing PyPSA-friendly generator data, including
            columns 'marginal_cost' (str with snake-case name of generator), 'carrier'
            (str), 'isp_fuel_cost_mapping' (str), 'isp_heat_rate_gj/mwh' (float)
            and 'isp_vom_$/mwh_sent_out' (float). These columns are required for the
            calculation of dynamic marginal costs for each generator.
        parsed_traces_directory: a pathlib.Path defining where the trace data which
            has been parsed using isp-trace-parser is located.
        pypsa_friendly_timeseries_inputs_location: a pathlib.Path defining where the
            time series data which is to be created should be saved.
        snapshots: Optional pd.DataFrame containing pre-defined snapshots to use instead
            of generating them. If provided, must contain columns 'snapshots' (datetime)
            and 'investment_periods' (int). This is useful for testing or when custom
            snapshots are needed.

    Returns: pd.DataFrame containing the snapshots used for filtering the timeseries
    """

    if model_phase == "capacity_expansion":
        reference_year_cycle = config.temporal.capacity_expansion.reference_year_cycle
    else:
        reference_year_cycle = config.temporal.operational.reference_year_cycle

    reference_year_mapping = construct_reference_year_mapping(
        start_year=config.temporal.range.start_year,
        end_year=config.temporal.range.end_year,
        reference_years=reference_year_cycle,
    )

    # Load generator timeseries data (organized by type)
    generator_traces_by_type = create_pypsa_friendly_ecaa_generator_timeseries(
        ispypsa_tables["ecaa_generators"],
        parsed_traces_directory,
        generator_types=["solar", "wind"],
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
    )

    # Load demand timeseries data
    demand_traces = create_pypsa_friendly_bus_demand_timeseries(
        ispypsa_tables["sub_regions"],
        parsed_traces_directory,
        scenario=config.scenario,
        regional_granularity=config.network.nodes.regional_granularity,
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
    )

    # Use provided snapshots or create new ones
    if snapshots is None:
        # Create snapshots, potentially using the loaded data for named_representative_weeks
        # Flatten generator traces for snapshot creation
        all_generator_traces = _flatten_generator_traces(generator_traces_by_type)

        snapshots = create_pypsa_friendly_snapshots(
            config,
            model_phase,
            existing_generators=ispypsa_tables.get("ecaa_generators"),
            demand_traces=demand_traces,
            generator_traces=all_generator_traces,
        )

    if generator_traces_by_type is not None:
        # Filter and save generator timeseries by type
        for gen_type, gen_traces in generator_traces_by_type.items():
            if gen_traces:
                _filter_and_save_timeseries(
                    gen_traces,
                    snapshots,
                    pypsa_friendly_timeseries_inputs_location,
                    f"{gen_type}_traces",
                )

    # Filter and save demand timeseries
    _filter_and_save_timeseries(
        demand_traces,
        snapshots,
        pypsa_friendly_timeseries_inputs_location,
        "demand_traces",
    )

    create_pypsa_friendly_new_entrant_generator_timeseries(
        ispypsa_tables["new_entrant_generators"],
        parsed_traces_directory,
        pypsa_friendly_timeseries_inputs_location,
        generator_types=["solar", "wind"],
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
        snapshots=snapshots,
    )

    # This is needed because numbers can be converted to strings if the data has been saved to a csv.
    generators = convert_to_numeric_if_possible(generators, cols=["marginal_cost"])
    # NOTE - maybe this function needs to be somewhere separate/handled a little
    # different because it currently requires the translated generator table as input?
    create_pypsa_friendly_dynamic_marginal_costs(
        ispypsa_tables,
        generators,
        snapshots,
        pypsa_friendly_timeseries_inputs_location,
    )

    return snapshots


def list_timeseries_files(
    config: ModelConfig, ispypsa_tables: dict[str, pd.DataFrame], output_base_path: Path
) -> list[Path]:
    """List all expected timeseries files based on the generators and demand nodes.

    Args:
        config: ISPyPSA model configuration
        ispypsa_tables: Dictionary of ISPyPSA input tables
        output_base_path: Base path for operational timeseries (e.g., _OPERATIONAL_TIMESERIES_LOCATION)

    Returns:
        List of Path objects for all expected timeseries files

    Raises:
        KeyError: If required ISPyPSA tables are missing
        ValueError: If required columns are missing from tables
    """
    files = []

    # Validate required tables exist
    if "ecaa_generators" not in ispypsa_tables:
        raise KeyError("Missing required ISPyPSA table: 'ecaa_generators'")
    if "sub_regions" not in ispypsa_tables:
        raise KeyError("Missing required ISPyPSA table: 'sub_regions'")

    # Get generators table and validate required columns
    ecaa_generators = ispypsa_tables["ecaa_generators"]
    required_gen_cols = ["generator", "fuel_type"]
    missing_cols = [
        col for col in required_gen_cols if col not in ecaa_generators.columns
    ]
    if missing_cols:
        raise ValueError(
            f"Missing required columns in 'ecaa_generators' table: {missing_cols}"
        )

    # Solar generator files
    solar_gens = ecaa_generators[ecaa_generators["fuel_type"] == "Solar"]["generator"]
    for gen in solar_gens:
        files.append(output_base_path / "solar_traces" / f"{gen}.parquet")

    # Wind generator files
    wind_gens = ecaa_generators[ecaa_generators["fuel_type"] == "Wind"]["generator"]
    for gen in wind_gens:
        files.append(output_base_path / "wind_traces" / f"{gen}.parquet")

    # Get sub_regions table and validate required columns based on granularity
    sub_regions = ispypsa_tables["sub_regions"]
    if config.network.nodes.regional_granularity == "single_region":
        files.append(output_base_path / "demand_traces" / "NEM.parquet")
    elif config.network.nodes.regional_granularity == "nem_regions":
        if "nem_region_id" not in sub_regions.columns:
            raise ValueError(
                "Missing required column 'nem_region_id' in 'sub_regions' table for nem_regions granularity"
            )
        for region in sub_regions["nem_region_id"].unique():
            files.append(output_base_path / "demand_traces" / f"{region}.parquet")
    else:  # sub_regions
        if "isp_sub_region_id" not in sub_regions.columns:
            raise ValueError(
                "Missing required column 'isp_sub_region_id' in 'sub_regions' table for sub_regions granularity"
            )
        for region in sub_regions["isp_sub_region_id"].unique():
            files.append(output_base_path / "demand_traces" / f"{region}.parquet")

    return files


def _flatten_generator_traces(
    generator_traces_by_type: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, pd.DataFrame] | None:
    """Flatten nested generator traces dictionary into a single level dictionary.

    Args:
        generator_traces_by_type: Dictionary with generator types as keys,
            each containing a dictionary with generator names as keys

    Returns:
        dict[str, pd.DataFrame]: Flattened dictionary with generator names as keys
    """
    if generator_traces_by_type is None:
        return None
    flattened_traces = {}
    for gen_type_traces in generator_traces_by_type.values():
        flattened_traces.update(gen_type_traces)
    return flattened_traces


def _filter_and_save_timeseries(
    timeseries_data: dict[str, pd.DataFrame],
    snapshots: pd.DataFrame,
    output_path: Path,
    trace_type: str,
) -> None:
    """Filter timeseries data by snapshots and save to parquet files.

    Args:
        timeseries_data: Dictionary of timeseries dataframes with names as keys.
            Each dataframe must have columns: Datetime, Value
        snapshots: DataFrame containing the expected time series values
        output_path: Path to directory where files will be saved
        trace_type: Type of trace data (e.g., "demand_traces", "solar_traces", "wind_traces")
    """
    output_trace_path = Path(output_path, trace_type)
    if not output_trace_path.exists():
        output_trace_path.mkdir(parents=True)

    # Determine the value column name based on trace type
    if "demand" in trace_type:
        value_column_name = "p_set"
    else:  # solar_traces or wind_traces
        value_column_name = "p_max_pu"

    for name, trace in timeseries_data.items():
        # Rename columns to PyPSA format
        trace = trace.rename(
            columns={"Datetime": "snapshots", "Value": value_column_name}
        )

        # Filter by snapshots
        trace = _time_series_filter(trace, snapshots)

        # Check time series alignment
        _check_time_series(
            trace["snapshots"],
            snapshots["snapshots"],
            trace_type.replace("_traces", " data"),
            name,
        )

        # Merge with snapshots to get investment periods
        trace = pd.merge(trace, snapshots, on="snapshots")

        # Select relevant columns
        trace = trace.loc[:, ["investment_periods", "snapshots", value_column_name]]

        # Save to parquet
        trace.to_parquet(Path(output_trace_path, f"{name}.parquet"), index=False)


def list_translator_output_files(output_path: Path | None = None) -> list[Path]:
    files = _BASE_TRANSLATOR_OUTPUTS
    if output_path is not None:
        files = [output_path / Path(file + ".csv") for file in files]
    return files
