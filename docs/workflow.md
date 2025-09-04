The ISPyPSA workflow consists of a series of data handling and manipulation steps to
take input data from AEMO provided formats to formats consistent with the PyPSA API.
Once the PyPSA friendly inputs are created then a PyPSA network object is created
and optimised to run the capacity expansion and operational modelling results. The
steps of the workflow are outlined below.

## Workbook parsing

AEMO provides many of the inputs for its ISP models in the
Inputs Assumptions Scenarios Report (IASR) MS Excel workbook. The workbook parsing
step of the workflow extracts this data from the workbook and saves the data in a set of
CSV files, with each CSV file corresponding to a table from the workbook. The parsing
is handled by the external package [isp-workbook-parser](https://github.
com/Open-ISP/isp-workbook-parser), but running of the parser is integrated directly
into the ISPyPSA workflow via a call to the isp-workbook-parser API. The workbook
parsing can be run using either the ISPyPSA CLI or API:

=== "CLI"

    ```commandline
    uv run ispypsa --config ispypsa_config.yaml cache_required_tables
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

Wind, solar, and demand trace data parsing**: AEMO provides time series data for
wind and solar resource availability, and demand as CSVs. The trace parsing
performs data format and naming conventions adjustments to integrate the data
smoothly with the rest of the ISPyPSA workflow. The parsing is handled by the
external package [isp-trace-parser](https://github.com/Open-ISP/isp-trace-parser),
the trace parsing is not integrated directly into teh ISPyPSA workflow due to the
long computational time required, instead pre-parsed trace data is made available
[here]() which the user can download to their local machine, for ISPyPSA to query as
needed.

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
    uv run ispypsa --config config.yaml create_ispypsa_inputs
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
        run_directory / config.run_name / "ispypsa_inputs" / "tables"
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
    )
    write_csvs(ispypsa_tables, ispypsa_input_tables_directory)
    ```

## Translating

The translating step of the workflow converts ISPyPSA inputs tables and parsed trace
data into a format consistent with the structure of the PyPSA API, these are referred to
as the PyPSA friendly inputs. The translating step is used

=== "CLI"

    ```commandline
    uv run ispypsa --config config.yaml create_pypsa_inputs
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
        run_directory / config.run_name / "ispypsa_inputs" / "tables"
    )
    pypsa_friendly_inputs_location = (
        run_directory / config.run_name / "pypsa_friendly"
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
