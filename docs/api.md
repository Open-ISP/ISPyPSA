# ISPyPSA API

The ISPyPSA API is the set of Python functions used to carrier out the ISPyPSA modelling workflow. When the
[CLI](cli.md) is used the API runs in the background to carrier out the workflow.

Advanced users wanting more control or flexibility from the modelling workflow might want to use ISPyPSA directly
through the API. An example of how the default ISPyPSA workflow is implement using the API is provided
[here](examples/example_api_workflow.md). We suggest API users start by running and understanding the default workflow
and

## Configuration & Logging

::: ispypsa.config.load_config

::: ispypsa.logging.configure_logging

## Data Fetching & Caching

::: ispypsa.iasr_table_caching.build_local_cache

::: ispypsa.data_fetch.read_csvs

::: ispypsa.data_fetch.write_csvs

## Templating (ISPyPSA Input Creation)

::: ispypsa.templater.create_ispypsa_inputs_template

::: ispypsa.templater.load_manually_extracted_tables

## Translation (PyPSA-Friendly Format)

::: ispypsa.translator.create_pypsa_friendly_inputs

::: ispypsa.translator.create_pypsa_friendly_snapshots

::: ispypsa.translator.create_pypsa_friendly_timeseries_inputs


## Model Building & Execution

::: ispypsa.model.build_pypsa_network

::: ispypsa.model.update_network_timeseries

::: ispypsa.model.save_results
