# ISPyPSA API

The ISPyPSA API is the set of Python functions used to carry out the ISPyPSA modelling workflow. When the
[CLI](cli.md) is used the API runs in the background to carry out the workflow.

Advanced users wanting more control or flexibility from the modelling workflow might want to use ISPyPSA directly
through the API. An example of how the default ISPyPSA workflow is implemented using the API is provided
[below](#api-default-workflow). We suggest API users start by running and understanding the default workflow
and then adapting it for their use case.

Each API function is also documented individually after the workflow example.

## API default workflow

Below is an example of the default ISPyPSA workflow implemented using the Python API.
This is the same workflow which the CLI follows. To use this workflow you simply need to
edit the code to point at an ispypsa_config.yaml file of your choice, then run the
Python script.

```Python
--8<-- "example_workflow.py"
```

## Configuration & Logging

::: ispypsa.config.load_config

::: ispypsa.logging.configure_logging

## Data Fetching & Caching

::: ispypsa.iasr_table_caching.build_local_cache

::: ispypsa.data_fetch.read_csvs

::: ispypsa.data_fetch.write_csvs

::: ispypsa.data_fetch.fetch_workbook

## Templating (ISPyPSA Input Creation)

::: ispypsa.templater.create_ispypsa_inputs_template

::: ispypsa.templater.load_manually_extracted_tables

## Translation (PyPSA-Friendly Format)

::: ispypsa.translator.create_pypsa_friendly_inputs

::: ispypsa.translator.create_pypsa_friendly_snapshots

::: ispypsa.translator.create_pypsa_friendly_timeseries_inputs


## Model Building & Execution

::: ispypsa.pypsa_build.build_pypsa_network

::: ispypsa.pypas_build.update_network_timeseries

::: ispypsa.pypas_build.save_pypsa_network

## Tabular Results Extraction

::: ispypsa.results.extract_tabular_results

## Plotting

::: ispypsa.plotting.create_plot_suite

::: ispypsa.plotting.save_plots

::: ispypsa.plotting.generate_results_website
