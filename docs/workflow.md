The ISPyPSA workflow consists of a series of data handling and manipulation steps to
convert input data from the AEMO provided format to a format consistent with the PyPSA
API. Once the PyPSA friendly inputs are created then a PyPSA network object is created
and optimised to run the capacity expansion and operational modelling. The
steps of the workflow are outlined below.

## Input data downloading

ISPyPSA requires two main types of input data: the IASR workbook containing model
parameters and assumptions, and trace data containing wind, solar, and demand time series.
Both can be downloaded using the ISPyPSA CLI or API.

### Workbook

The IASR workbook is downloaded from OpenISP's public archive. The workbook download can be run using
either the ISPyPSA CLI or API:

=== "CLI"

    ```commandline
    uv run ispypsa config=ispypsa_config.yaml download_workbook
    ```

=== "API"

    ```Python
    from ispypsa.data_fetch import fetch_workbook

    fetch_workbook(
        workbook_version="6.0",
        save_path="path/to/save/iasr_workbook.xlsx"
    )
    ```

### Trace data

The trace data contains time series for wind and solar resource availability, and demand.
This data has been preprocessed using [isp-trace-parser](https://github.
com/Open-ISP/isp-trace-parser) and is hosted on OpenISP's
public archive. The dataset type (full or example) and year are specified in the
configuration file. The trace data download can be run using either the ISPyPSA CLI
or API:

=== "CLI"

    ```commandline
    uv run ispypsa config=ispypsa_config.yaml download_trace_data
    ```

=== "API"

    ```Python
    from ispypsa.data_fetch import fetch_trace_data

    fetch_trace_data(
        dataset_type="example",
        dataset_year=2024,
        save_directory="path/to/save/directory"
    )
    ```

## Workbook parsing

AEMO provides many of the inputs for its ISP models in the
Inputs Assumptions Scenarios Report (IASR) MS Excel workbook. The workbook parsing
step of the workflow extracts this data from the workbook and saves the data in a set of
CSV files, with each CSV file corresponding to a table from the workbook. The parsing
is handled by the external package [isp-workbook-parser](https://github.
com/Open-ISP/isp-workbook-parser), but running the parser is integrated directly
into the ISPyPSA workflow via a call to the isp-workbook-parser API. The workbook
parsing can be run using either the ISPyPSA CLI or API:

=== "CLI"

    ```commandline
    uv run ispypsa config=ispypsa_config.yaml cache_required_iasr_workbook_tables
    ```

=== "API"

    ```Python
    from ispypsa.iasr_table_caching import build_local_cache

    build_local_cache(
        cache_path="path/to/cache/location",
        workbook_path="path/to/iasr_workbook.xlsx",
        iasr_workbook_version="6.0"
    )
    ```

## Wind, solar, and demand trace data parsing

AEMO provides time series data for wind and solar resource availability, and demand as
CSVs. The trace parsing performs data format and naming conventions adjustments to
integrate the data smoothly with the rest of the ISPyPSA workflow. The parsing is
handled by the external package [isp-trace-parser](https://github.
com/Open-ISP/isp-trace-parser), the trace parsing is not integrated directly into
the ISPyPSA workflow due to the long computational time required, instead pre-parsed
trace data is provided during the initial input downloading workflow step.

## Templating

The templating step of the workflow extracts the input data
required to run a particular ISP scenario from the parsed workbook tables. The data
is also reformatted to create a more concise table set, while attempting to largely
maintain the core data structures established by AEMO in the IASR workbook. This
step is called templating because it produces a template set of inputs based on
an ISP scenario which can then be used to run an ISPyPSA model. The tables produced
by the templating are referred to as ISPyPSA input tables. The templating step can be
run using either ISPyPSA CLI or API.

=== "CLI"

    ```commandline
    uv run ispypsa config=config.yaml create_ispypsa_inputs
    ```

=== "API"

    ```Python
    from ispypsa.config import load_config
    from ispypsa.data_fetch import read_csvs, write_csvs
    from ispypsa.templater import (
        create_ispypsa_inputs_template,
        load_manually_extracted_tables,
    )

    config_path = Path("ispypsa_config.yaml")
    config = load_config(config_path)

    parsed_workbook_cache = Path(config.paths.parsed_workbook_cache)
    run_directory = Path(config.paths.run_directory)
    ispypsa_input_tables_directory = (
        run_directory / config.paths.ispypsa_run_name /"ispypsa_inputs"
    )

    # Get raw IASR tables
    iasr_tables = read_csvs(parsed_workbook_cache)
    manually_extracted_tables = load_manually_extracted_tables(
        config.iasr_workbook_version
    )

    # Create ISPyPSA inputs from IASR tables.
    ispypsa_tables = create_ispypsa_inputs_template(
        config.scenario,
        config.network.nodes.regional_granularity,
        iasr_tables,
        manually_extracted_tables,
        config.filter_by_nem_regions,
        config.filter_by_isp_sub_regions,
    )
    write_csvs(ispypsa_tables, ispypsa_input_tables_directory)
    ```

## Translating

The translating step of the workflow converts ISPyPSA inputs tables and parsed trace
data into a format consistent with the structure of the PyPSA API, these are referred to
as the PyPSA friendly inputs. This is referred to as the translating step because we
are translating the inputs from ISPyPSA format to PyPSA friendly format. The translating
step can be run using either the ISPyPSA CLI or API.

=== "CLI"

    ```commandline
    uv run ispypsa config=config.yaml create_pypsa_friendly_inputs
    ```

=== "API"

    ```Python
    from ispypsa.config import load_config
    from ispypsa.data_fetch import read_csvs, write_csvs
    from ispypsa.templater import (
        create_ispypsa_inputs_template,
        load_manually_extracted_tables,
    )

    config_path = Path("ispypsa_config.yaml")
    config = load_config(config_path)

    run_directory = Path(config.paths.run_directory)
    ispypsa_input_tables_directory = (
        run_directory / config.paths.ispypsa_run_name /"ispypsa_inputs"
    )
    pypsa_friendly_inputs_location = (
        run_directory / config.paths.ispypsa_run_name /"pypsa_friendly"
    )
    capacity_expansion_timeseries_location = (
        pypsa_friendly_inputs_location / "capacity_expansion_timeseries"
    )

    # Translate ISPyPSA format to a PyPSA friendly format.
    pypsa_friendly_input_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)
    write_csvs(pypsa_friendly_input_tables, pypsa_friendly_inputs_location)

    create_pypsa_friendly_timeseries_inputs(
        config,
        "capacity_expansion",
        ispypsa_tables,
        pypsa_friendly_input_tables["snapshots"],
        parsed_traces_directory,
        capacity_expansion_timeseries_location,
    )
    ```

### Create operational timeseries data

Optionally, if you want to run the operational model after running capacity expansion
you can also create the PyPSA friendly operational timeseries data at this stage.

=== "CLI"

    ```commandline
    uv run ispypsa config=config.yaml create_operational_timeseries
    ```

=== "API"

    ```Python
    from ispypsa.config import load_config
    from ispypsa.data_fetch import read_csvs, write_csvs
    from ispypsa.templater import (
        create_ispypsa_inputs_template,
        load_manually_extracted_tables,
    )

    config_path = Path("ispypsa_config.yaml")
    config = load_config(config_path)

    run_directory = Path(config.paths.run_directory)
    ispypsa_input_tables_directory = (
        run_directory / config.paths.ispypsa_run_name /"ispypsa_inputs"
    )
    pypsa_friendly_inputs_location = (
        run_directory / config.paths.ispypsa_run_name /"pypsa_friendly"
    )
    operational_timeseries_location = (
        pypsa_friendly_inputs_location / "operational_timeseries"
    )

    # Translate ISPyPSA format to a PyPSA friendly format.
    pypsa_friendly_input_tables = create_pypsa_friendly_inputs(config, ispypsa_tables)
    write_csvs(pypsa_friendly_input_tables, pypsa_friendly_inputs_location)

    operational_snapshots = create_pypsa_friendly_snapshots(
        config,
        "operational"
    )

    create_pypsa_friendly_timeseries_inputs(
        config,
        "operational",
        ispypsa_tables,
        operational_snapshots,
        parsed_traces_directory,
        operational_timeseries_location,
    )
    ```

## PyPSA building and run

### Capacity expansion model

Once the PyPSA friendly inputs have been created a PyPSA network object can be created,
inputs loaded into the network object, and the capacity expansion optimisation run. The
PyPSA object needs to be built and run within a single workflow step as custom
constraints are not preserved when the PyPSA network object is saved to disk.

=== "CLI"

    ```commandline
    uv run ispypsa config=config.yaml create_and_run_capacity_expansion_model
    ```

=== "API"

    ```Python

    from ispypsa.data_fetch import read_csvs
    from ispypsa.model import build_pypsa_network, save_results

    # Load model config.
    config_path = Path("ispypsa_runs/development/ispypsa_inputs/ispypsa_config.yaml")
    config = load_config(config_path)

    # Load base paths from config
    run_directory = Path(config.paths.run_directory)

    # Construct full paths from base paths
    pypsa_friendly_inputs_location = (
        run_directory / config.ispypsa_run_name / "pypsa_friendly"
    )
    capacity_expansion_timeseries_location = (
        pypsa_friendly_inputs_location / "capacity_expansion_timeseries"
    )
    pypsa_outputs_directory = run_directory / "outputs"

    # Load the capacity expansion network
    network = pypsa.Network(capacity_expansion_pypsa_file)

    # Build a PyPSA network object.
    network = build_pypsa_network(
        pypsa_friendly_input_tables,
        capacity_expansion_timeseries_location,
    )

    # Solve for least cost operation/expansion
    # Never use network.optimize() as this will remove custom constraints.
    network.optimize.solve_model(solver_name=config.solver)

    # Save results.
    save_results(
        network,
        pypsa_outputs_directory,
        config.paths.ispypsa_run_name +"_capacity_expansion"
    )
    ```

### Operational model

After the capacity expansion model has run, an operational model, usually with a higher
temporal resolution, can be built and run. The operational model build takes the PyPSA
network object used for capacity expansion and fixes generator, storage, and
transmission line capacities at their optimal values. Then the timeseries load and
resource availability timeseries data is updated to match the operational model
temporal resolution, custom constraints are rebuilt, and the optimisation rerun. Note,
the operational optimisation uses rolling horizon to allow for greater temporal
resolution, the fixing of the unit and transmission capacities also help limit
computational complexity.

=== "CLI"

    ```commandline
    uv run ispypsa config=config.yaml create_and_run_operational_model
    ```

=== "API"

    ```Python
    from ispypsa.data_fetch import read_csvs
    from ispypsa.model import update_network_timeseries, save_results

    # Load model config.
    config_path = Path("ispypsa_runs/development/ispypsa_inputs/ispypsa_config.yaml")
    config = load_config(config_path)

    # Load base paths from config
    run_directory = Path(config.paths.run_directory)

    # Construct full paths from base paths
    pypsa_friendly_inputs_location = (
        run_directory / config.ispypsa_run_name / "pypsa_friendly"
    )
    operational_timeseries_location = (
        pypsa_friendly_inputs_location / "operational_timeseries"
    )
    pypsa_outputs_directory = run_directory / "outputs"
    capacity_expansion_pypsa_file = (
        pypsa_outputs_directory / config.paths.ispypsa_run_name +"_capacity_expansion"
    )

    # Get PyPSA freindly inputs as pd.DataFrames
    pypsa_friendly_input_tables = read_csvs(pypsa_friendly_inputs_location)

    # Create operational snapshots (needed for update_network_timeseries)
    operational_snapshots = create_pypsa_friendly_snapshots(config, "operational")

    update_network_timeseries(
        network,
        pypsa_friendly_input_tables,
        operational_snapshots,
        operational_timeseries_location,
    )

    network.optimize.fix_optimal_capacities()

    # Never use network.optimize() as this will remove custom constraints.
    network.optimize.optimize_with_rolling_horizon(
        horizon=config.temporal.operational.horizon,
        overlap=config.temporal.operational.overlap,
    )

    save_results(
        network,
        pypsa_outputs_directory,
        config.paths.ispypsa_run_name +"_operational"
    )
    ```
