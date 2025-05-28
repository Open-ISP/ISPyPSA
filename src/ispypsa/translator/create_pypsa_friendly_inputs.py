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
    _translate_custom_constraints,
)
from ispypsa.translator.generators import (
    _create_unserved_energy_generators,
    _translate_ecaa_generators,
    create_pypsa_friendly_existing_generator_timeseries,
)
from ispypsa.translator.lines import _translate_flow_paths_to_lines
from ispypsa.translator.renewable_energy_zones import (
    _translate_renewable_energy_zone_build_limits_to_lines,
)
from ispypsa.translator.snapshots import (
    _create_investment_period_weightings,
    create_pypsa_friendly_snapshots,
)

_BASE_TRANSLATOR_OUTPUTS = [
    "snapshots",
    "investment_period_weights",
    "buses",
    "lines",
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

    # Perform requried imports.
    >>> from pathlib import Path
    >>> from ispypsa.config import load_config
    >>> from ispypsa.data_fetch import read_csvs, write_csvs
    >>> from ispypsa.translator import create_pypsa_friendly_inputs

    # Load ISPyPSA model config file and input tables.
    >>> config = load_config(Path("ispypsa_config.yaml"))
    >>> ispypsa_input_tables = read_csvs(Path("ispypsa_inputs_directory"))

    # Make the PyPSA friendly inputs!
    >>> pypsa_friendly_inputs = create_pypsa_friendly_inputs(
    ... config=config,
    ... ispypsa_tables=ispypsa_input_tables
    ... )

    # Write the resulting dataframes to CSVs.
    >>> write_csvs(pypsa_friendly_inputs)

    Args:
        config: `ISPyPSA` `ispypsa.config.ModelConfig` object (add link to config docs).
        ispypsa_tables: dictionary of dataframes providing the `ISPyPSA` input tables.
            (add link to ispypsa input tables docs).

    Returns: dictionary of dataframes in the `PyPSA` friendly format. (add link to
        pypsa friendly format table docs)
    """
    pypsa_inputs = {}

    pypsa_inputs["snapshots"] = create_pypsa_friendly_snapshots(
        config, "capacity_expansion"
    )

    pypsa_inputs["investment_period_weights"] = _create_investment_period_weightings(
        config.temporal.capacity_expansion.investment_periods,
        config.temporal.range.end_year,
        config.discount_rate,
    )

    pypsa_inputs["generators"] = _translate_ecaa_generators(
        ispypsa_tables["ecaa_generators"], config.network.nodes.regional_granularity
    )

    buses = []
    lines = []

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
            config.unserved_energy.generator_size_mw,
        )
        pypsa_inputs["generators"] = pd.concat(
            [pypsa_inputs["generators"], unserved_energy_generators]
        )

    if config.network.nodes.rezs == "discrete_nodes":
        buses.append(_translate_rezs_to_buses(ispypsa_tables["renewable_energy_zones"]))
        lines.append(
            _translate_renewable_energy_zone_build_limits_to_lines(
                ispypsa_tables["renewable_energy_zones"],
                ispypsa_tables["rez_transmission_expansion_costs"],
                config,
            )
        )

    if config.network.nodes.regional_granularity != "single_region":
        lines.append(_translate_flow_paths_to_lines(ispypsa_tables, config))

    pypsa_inputs["buses"] = pd.concat(buses)

    if len(lines) > 0:
        pypsa_inputs["lines"] = pd.concat(lines)
    else:
        pypsa_inputs["lines"] = pd.DataFrame()

    pypsa_inputs.update(_translate_custom_constraints(config, ispypsa_tables))

    return pypsa_inputs


def create_pypsa_friendly_timeseries_inputs(
    config: ModelConfig,
    model_phase: Literal["capacity_expansion", "operational"],
    ispypsa_tables: dict[str, pd.DataFrame],
    snapshots: pd.DataFrame,
    parsed_traces_directory: Path,
    pypsa_friendly_timeseries_inputs_location: Path,
) -> None:
    """Creates on disk the timeseries data files in PyPSA friendly format for generation
    and demand.

    - a time series file is created for each wind and solar generator in the
    ecaa_generators table (table in ispypsa_tables dict). The time series data is saved
    in parquet files in the 'solar_traces' and 'wind_traces' directories with the
    columns "snapshots" (datetime) and "p_max_pu" (float specifying availability in MW).

    - a time series file is created for each model region specifying the load in that
    region (regions set by config.network.nodes.regional_granularity). The time series
    data is saved in parquet files in the 'demand_traces' directory with the columns
    "snapshots" (datetime) and "p_set" (float specifying load in MW).

    Examples:

        >>> from ispypsa.translator import create_pypsa_friendly_snapshots        >>> from pathlib import Path
        >>> from ispypsa.config import load_config
        >>> from ispypsa.data_fetch import read_csvs
        >>> from ispypsa.translator.create_pypsa_friendly_inputs import (
        ...      create_pypsa_friendly_timeseries_inputs
        ... )

        Get a ISPyPSA ModelConfig instance

        >>> config = load_config(Path("path/to/config/file.yaml"))

        Get ISPyPSA inputs (in particular these need to contain the ecaa_generators and
        sub_regions tables).

        >>> ispypsa_tables = read_csvs(Path("path/to/ispypsa/inputs"))

        Define which phase of the modelling we need the time series data for.

        >>> model_phase = "capacity_expansion"

        Create pd.Dataframe defining the set of snapshot (time intervals) to be used.

        >>> snapshots = create_pypsa_friendly_snapshots(config, model_phase)

        Now the complete set of time series files needed to run the PyPSA model can
        be created.

        >>> create_pypsa_friendly_timeseries_inputs(
        ...     config,
        ...     model_phase,
        ...     ispypsa_tables
        ...     snapshots
        ...     Path("path/to/parsed/isp/traces"),
        ...     Path("path/to/write/time/series/inputs/to")
        ... )

    Args:
        config: ispypsa.ModelConfig instance
        model_phase: string defining whether the snapshots are for the operational or
            capacity expansion phase of the modelling. This allows the correct temporal
            config inputs to be used from the ModelConfig instance.
        ispypsa_tables: dict of pd.DataFrames defining the ISPyPSA input tables.
            Inparticular the dict needs to contain the ecaa_generators and
            sub_regions tables, the other tables aren't required for the time series
            data creation. The ecaa_generators table needs the columns 'generator' (name
            or generator as str) and 'fuel_type' (str with 'Wind' and 'Solar' fuel types
            as appropraite). The sub_regions table needs to have the columns
            'isp_sub_region_id' (str) and 'nem_region_id' (str) if a 'regional'
            granuality is used.
        snapshots: a pd.DataFrame with the columns 'period' (int) and 'snapshots'
            (datetime) defining the time intervals and coresponding investment periods
            to be modelled.
        parsed_traces_directory: a pathlib.Path defining where the trace data which
            has been parsed using isp-trace-parser is located.
        pypsa_friendly_timeseries_inputs_location: a pathlib.Path defining where the
            time series data which is to be created should be saved.

    Returns: None
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
    create_pypsa_friendly_existing_generator_timeseries(
        ispypsa_tables["ecaa_generators"],
        parsed_traces_directory,
        pypsa_friendly_timeseries_inputs_location,
        generator_types=["solar", "wind"],
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
        snapshots=snapshots,
    )
    create_pypsa_friendly_bus_demand_timeseries(
        ispypsa_tables["sub_regions"],
        parsed_traces_directory,
        pypsa_friendly_timeseries_inputs_location,
        scenario=config.scenario,
        regional_granularity=config.network.nodes.regional_granularity,
        reference_year_mapping=reference_year_mapping,
        year_type=config.temporal.year_type,
        snapshots=snapshots,
    )


def list_translator_output_files(output_path: Path | None = None) -> list[Path]:
    files = _BASE_TRANSLATOR_OUTPUTS
    if output_path is not None:
        files = [output_path / Path(file + ".csv") for file in files]
    return files
