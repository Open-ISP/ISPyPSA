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

## Running your first model

1. Create a new directory for storing your ispypsa model files, or if you followed
   the `uv` install process just use the new-ispypsa-project directory you created.

2. Download the default example config file and place in the new directory.

    [⬇ Download ispypsa_config.yaml](downloads/ispypsa_config.yaml)

3. Edit the yaml config file so that the paths section matches your environment:
      - `parsed_traces_directory`: Base directory where trace data will be downloaded.
         Choose a directory on a drive with at least 30 GB of free space.
      - `workbook_path`: Path where the ISP Excel workbook file will be downloaded (must end with `.xlsx`).
         You need to give a complete file path and name, not just a path to a directory.
      - `parsed_workbook_cache`: Where extracted workbook data should be cached. This
         could be anywhere but a subdirectory in your new directory is a good idea.
      - `run_directory`: Base directory where all model inputs and outputs will be
         stored. This could be anywhere but a subdirectory in your new directory is a
        good idea.

     The top of your yaml file should look something like this:

     ```yaml
      # ===== Path configuration =============================================================

      paths:
        # The name of the ISPyPSA model run
        # This name is used to select the output folder within `ispypsa_runs`
        ispypsa_run_name: example_model_run

        # Base directory where trace data will be downloaded
        # The download task will create isp_2024 subdirectory automatically
        parsed_traces_directory: "data/trace_data"

        # Path where the ISP Excel workbook will be downloaded
        workbook_path: "data/2024-isp-inputs-and-assumptions-workbook.xlsx"

        # The path to the workbook table cache directory
        parsed_workbook_cache: "data/workbook_table_cache"

        # The run directory where all inputs and outputs will be stored
        run_directory: "ispypsa_runs"
     ```

!!! Important "Relative paths"

      In this example the paths in the config should be relative to the directory where you will use the command line
      to run the model. If you are using a new `uv` project then this will be the directory you just created, and
      `data`, `data/trace_data`, `data/workbook_table_cache`, and `ispypsa_runs` should be new directories you have
       created.

4. Using the command line inside the project/environment where `ispypsa` is installed download the ISP workbook:

    === "uv"

        ```commandline
        uv run ispypsa config=ispypsa_config.yaml download_workbook
        ```

    === "plain python"

        ```commandline
        ispypsa config=ispypsa_config.yaml download_workbook
        ```

    This will download the workbook to the path specified in your config file
    (`paths.workbook_path`). The workbook version is automatically determined from
    your config (`iasr_workbook_version`).

5. Using the command line inside the project/environment where `ispypsa` is installed download the trace data:

    === "uv"

        ```commandline
        uv run ispypsa config=ispypsa_config.yaml download_trace_data
        ```

    === "plain python"

        ```commandline
        ispypsa config=ispypsa_config.yaml download_trace_data
        ```

    This will download trace data to your configured directory (`paths.parsed_traces_directory`).
    By default, the example dataset is downloaded, which is a smaller subset suitable for
    testing. The dataset type can be configured in your config file (`trace_data.dataset_type`)
    or overridden on the command line:

    === "uv"

        ```commandline
        # Download the full dataset instead of example
        uv run ispypsa config=ispypsa_config.yaml trace_dataset_type=full download_trace_data
        ```

    === "plain python"

        ```commandline
        # Download the full dataset instead of example
        ispypsa config=ispypsa_config.yaml trace_dataset_type=full download_trace_data
        ```

6. Using the command line inside the project/environment where `ispypsa` is installed
   run the complete modeling workflow to test everything works correctly:

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
           │   ├── build_costs.csv
           │   ├── ...
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

7. The previous model run used the default inputs for the ISP scenario specified in the
   yaml file. To run model with different inputs you can edit the csv files in
   `<run_directory>/<ispypsa_run_name>/ispypsa_inputs/`. To rerun the model
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

8. If you want to create the ispypsa inputs, so you can edit them, but not run the
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
      [example_workflow.py](examples/example_api_workflow.md). If you want to understand
      the workflow by tracing the API calls you can follow along in example_workflow.py
      script. Alternatively, the [Workflow](workflow.md) section provides a
      high level explanation.
