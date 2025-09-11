# Getting started

## Installation

### New to Python / need help setting up your environment?

!!! note "Unsure?"

      We recommend using the `uv` software to install and run Python for ISPyPSA.

1.  Install `uv` by following the instructions [here](https://docs.astral.
    sh/uv/getting-started/installation/#__tabbed_1_1)

2. Using the command line install Python 3.12

    ```commandline
    uv python install 3.12
    ```

3. Using the command line create a new directory setup as a `uv` managed project

    ```commandline
    uv init new-ispypsa-project
    ```

4. In the new directory use the command line to install ISPyPSA for the project. Any
additional Python packages you wish to use can be installed using the same commands.

    ```commandline
    uv add ISPyPSA
    uv sync
    ```

5. Any scripts you run inside the new directory using `uv run` will automatically
   use the packages you've installed for the project.

    ```commandline
    uv run main.py
    ```

### I know Python!

pip install or a python package manager can install ISPyPSA

    pip install ispypsa

## Obtaining input data

We need to setup a very simple and convenient place for users to download the
traces and workbooks from.


## Running your first model

1. Create a new directory for storing your ispypsa model files, or if you followed
   the `uv` install process just use the new-ispypsa-project directory you created.

2. Download the default example config file and place in the new directory.

    [⬇ Download ispypsa_config.yaml](examples/ispypsa_config.py)

2. Edit the yaml config file so that the paths section matches your environment:
      - `parsed_traces_directory`: Location of demand, wind, and solar trace data.
      - `workbook_path`: Path to the ISP Excel workbook file (.xlsx)
      - `parsed_workbook_cache`: Where extracted workbook data should be cached. This
         could be anywhere but a subdirectory in your new directory is a good idea.
      - `run_directory`: Base directory where all model inputs and outputs will be
         stored. This could be anywhere but a subdirectory in your new directory is a
        good idea.

     The top of your yaml file should look something like this:

     ```yaml
      # The name of the ISPyPSA model run
      # This name is used to select the output folder within `ispypsa_runs`
      ispypsa_run_name: example_model_run

      # ===== Path configuration =============================================================

      paths:
        # The path to the folder containing parsed demand, wind and solar traces.
        # If set to ENV the path will be retrieved from the environment variable "PATH_TO_PARSED_TRACES"
        parsed_traces_directory: "D:/isp_2024_data/parsed_trace_data"

        # The path to the ISP workbook Excel file
        workbook_path: "2024-isp-inputs-and-assumptions-workbook.xlsx"

        # The path to the workbook table cache directory
        parsed_workbook_cache: "workbook_table_cache"

        # The run directory where all inputs and outputs will be stored
        # Subdirectories will be created automatically:
        #   - {run_directory}/{ispypsa_run_name}/ispypsa_inputs/tables
        #   - {run_directory}/{ispypsa_run_name}/pypsa_friendly
        #   - {run_directory}/{ispypsa_run_name}/pypsa_friendly/capacity_expansion_timeseries
        #   - {run_directory}/{ispypsa_run_name}/pypsa_friendly/operational_timeseries
        #   - {run_directory}/{ispypsa_run_name}/outputs
        run_directory: "ispypsa_runs"
     ```

     See [Obtaining input data](#obtaining-input-data) for where to get the trace data
     and MS Excel workbook.

3. Using the command line inside the project/environment where `ispypsa` is installed
   run the complete modeling workflow to tests everything works correctly:

    === "uv"

        ```commandline
        uv run ispypsa config=ispypsa_config.yaml
        ```

    === "plain python"

        ```commandline
        ispypsa config=ispypsa_config.yaml
        ```

       Once the model run finishes you should have the following directory structure.

       ```
       <run_directory>/
       └── <ispypsa_run_name>/
           ├── ispypsa_inputs/
           │   ├── tables/
           │   │   ├── build_costs.csv
           │   │   ├── ...
           ├── pypsa_friendly/
           │   ├── buses.csv
           │   ├── ...
           │   ├── capacity_expansion_timeseries/
           │   │   ├── demand_traces/
           │   │   │   ├── CNSW.parquet
           │   │   │   └── ...
           │   │   ├── solar_traces/
           │   │   │   ├── Bomen Solar Farm.parquet
           │   │   │   └── ...
           │   │   └── wind_traces/
           │   │       ├── Ararat Wind Farm.parquet
           │   │       └── ...
           │   └── operational_timeseries/
           │       └── (same structure as capacity_expansion)
           └── outputs/
               ├── development_capacity_expansion.h5
               └── development_operational.h5
       ```

4. The previous model run used the default inputs for the ISP scenario specified in the
   yaml file. To run model with different inputs you can edit the csv files in
   `<run_directory>/<ispypsa_run_name>/ispypsa_inputs/tables/`. To rerun the model
   with the new inputs, use the same commands as before to rerun the
   model. By default, `ispypsa` will detect that the inputs have changed and rerun only
   modelling workflow steps which depend on the changed inputs, but not workflow steps
   used to create the ispypsa input files.


    === "uv"

        ```commandline
        uv run ispypsa config=ispypsa_config.yaml
        ```

    === "plain python"

        ```commandline
        ispypsa config=ispypsa_config.yaml
        ```

5. If you want to create the ispypsa inputs, so you can edit them, but not run the
   complete model, use the following command:

    === "uv"

        ```commandline
        uv run ispypsa config=ispypsa_config.yaml create_ispypsa_inputs
        ```

    === "plain python"

        ```commandline
        ispypsa config=ispypsa_config.yaml create_ispypsa_inputs
        ```

!!! note "Mastering the ispysa command line tool (CLI)"

      This example has shown you how to start using ISPyPSA through its command line
      interface or CLI. To view the complete CLI documentation go to [CLI](cli.md)

!!! note "The API alternative"

      The alternaive to using the CLI is to use the API, which offers more control than
      the CLI but takes a little extra work.  To view the complete API documentation go
      to [API](api.md)

!!! note "Understanding what's happening under the hood!"

      When ISPyPSA is run from the command line the workflow executed is the same as in
      [example_workflow.py](examples/example_workflow.py). If you want to understand
      the workflow by tracing the API calls you can follow along in example_workflow.py
      script. Alternatively, the [Workflow](workflow.md) section provides a
      high level explanation.
