The config is a yaml file used to specify all the modelling settings or
inputs that are not otherwise provided in the ISPyPSA input tables. Each of the settings
is documented on this page and an example config file can be found
[here](examples/example_config.md).

## Paths

The paths settings specify the locations of model input and output data. Either relative
or absolute paths can be used. Relative paths assume the run directory (CLI or API) is the
root directory of the relative path.

### paths.parsed_traces_directory

The path to the folder containing parsed demand, wind and solar traces.  If set to
ENV the path will be retrieved from the environment variable "PATH_TO_PARSED_TRACES".

Examples:

```parsed_traces_directory: ENV```

```parsed_traces_directory: "path/to/traces"```

### paths.workbook_path

The path to the ISP workbook Excel file.

Examples:

```workbook_path: "path/to/workbook"```

### paths.parsed_workbook_cache

The path to the workbook table cache directory.

Examples:

```parsed_workbook_cache: path/to/workbook/cache```

### paths.run_directory

The run directory where all inputs and outputs will be stored. Subdirectories will be created automatically:

- {run_directory}/{ispypsa_run_name}/ispypsa_inputs
- {run_directory}/{ispypsa_run_name}/pypsa_friendly
- {run_directory}/{ispypsa_run_name}/pypsa_friendly/capacity_expansion_timeseries
- {run_directory}/{ispypsa_run_name}/pypsa_friendly/operational_timeseries
- {run_directory}/{ispypsa_run_name}/outputs

Examples:

```run_directory: "path/to/directory"```

### paths.ispypsa_run_name

The name of the ISPyPSA model run. This name is used to select the output folder within `run_directory`.

Examples:

```ispypsa_run_name: modelling_test_run```

## Trace Data

### trace_data.dataset_type

The type of trace dataset to download when using CLI download tasks. This setting is only used by the
`download_trace_data` CLI task.

Options:

- "example": Smaller dataset suitable for testing and development
- "full": Complete dataset for production runs

Default: "example"

Examples:

```dataset_type: example```

### trace_data.dataset_year

The year of trace dataset to download when using CLI download tasks. This setting is used by the `download_trace_data`
CLI task and by other modelling tasks to select the correct data set from the trace data directory. Currently only 2024
is supported.

Default: 2024

Examples:

```dataset_year: 2024```

## ISPyPSA Templating

### iasr_workbook_version

The version of IASR workbook that the template inputs are generated from. The workbook
version is used to retrieve IASR data which has been manually extracted (rather than via
isp-workbook-parser) and packaged with ISPyPSA. Some data needs to be manually extracted
because of the formatting of the IASR workbook.

Examples:

```iasr_workbook_version: "6.0"```

### scenario

The ISP scenario for which to generate ISPyPSA inputs.

Options (descriptions lifted from the 2024 ISP):

- "Progressive Change": Reflects slower economic growth and energy investment with economic and international factors placing industrial demands at greater risk and slower decarbonisation action beyond current commitments
- "Step Change": Fulfils Australia's emission reduction commitments in a growing economy
- "Green Energy Exports": Sees very strong industrial decarbonisation and low-emission energy exports

Examples:

```scenario: Step Change```

## Financial/Economic

### wacc

Weighted average cost of capital for annuitisation of generation and transmission costs,
as a fraction, i.e. 0.07 is 7%.

Examples:

```wacc: 0.07```

### discount_rate

Discount rate applied to model objective function, as a fraction, i.e. 0.07 is 7%.

Examples:

```discount_rate: 0.05```

## Unserved Energy

Unserved energy can be allowed at each node in the network with energy demand to prevent
model infeasibility.

### unserved_energy.cost

Cost of unserved energy in $/MWh.

Set to 'None' to disable unserved energy generators.

Examples:

```cost: 10000.0```

### unserved_energy.max_per_node

Maximum allowed unserved demand MW per demand network node. Defaults to 1e5
(100,000 MW). Larger values may cause problems for optimisation solvers.

Examples:

```max_per_node: 100000.0```

## Network

### network.transmission_expansion

Does the model consider the expansion of sub-region to sub-region transmission capacity.

Examples:

```transmission_expansion: True```

### network.rez_transmission_expansion

Does the model consider the expansion of renewable energy zone transmission capacity.

Examples:

```rez_transmission_expansion: True```

### network.annuitisation_lifetime

Years to annuitise transmission project capital costs over.

Examples:

```annuitisation_lifetime: 30```

### network.nodes.regional_granularity

The regional granularity of the nodes in the modelled network. Only "sub_regions" is
implemented and allowed currently.

Options:

- "sub_regions": ISP sub-regions are added as network nodes (12 nodes)
- "nem_regions": NEM regions are added as network nodes (5 nodes)
- "single_region": A single node, the Victorian sub-region, is added as a network node (1 node)

Examples:

```regional_granularity: sub_regions```


### network.nodes.rezs

Whether Renewable Energy Zones (REZs) are modelled as distinct nodes.

Options:

- "discrete_nodes": REZs are added as network nodes to model REZ transmission limits
- "attached_to_parent_node": REZ resources are attached to their parent node (sub-region or NEM region)

Examples:

```rezs: discrete_nodes```

### network.rez_to_sub_region_transmission_default_limit

Link capacity limit for rez to node connections that have their limit's modelled
through custom constraint (MW).

The export limits for some REZs are modelled via custom constraints which
incorporate multiple transmission line flows and/or generator dispatch levels. For these
REZs a PyPSA link object between the REZ and sub region node is still created but with a
very high capacity limit such the that the custom constraint always sets the actual REZ
export limit.

Examples:

```rez_to_sub_region_transmission_default_limit: 1e5```

## Temporal

### temporal.year_type

Whether the model uses financial ("fy") or calendar ("calendar") years.

Examples:

```year_type: fy```

### temporal.range.start_year

Model begins at the start of the start year. E.g. the first time interval for a
financial year model starting in 2025 would be 2024-07-01 00:30:00.

Examples:

```start_year: 2025```

### temporal.range.end_year

Model ends at the end of the start year. E.g. the last time interval for a
financial year model ending in 2028 would be 2028-06-01 23:30:00.

Examples:

```end_year: 2035```

### temporal.capacity_expansion

The temporal settings for the capacity expansion phase of the modelling.

#### temporal.capacity_expansion.resolution_min

The temporal resolution in minutes. Currently, only the 30 min resolution is
implemented/allowed. Value should be provided as an integer.

Examples:

```resolution_min: 30```

#### temporal.capacity_expansion.reference_year_cycle

The order in which different weather reference years are used in the model. If the
number of reference years is less than the number of model years the reference years
will be reused in the order provided.

Examples:

```reference_year_cycle: [2018, 2021, 2023]```

#### temporal.capacity_expansion.investment_periods

List of investment period start years. An investment period runs from the beginning of
the year (financial or calendar) until the next the period begins.

Examples:

```investment_periods: [2025, 2030]```

#### temporal.capacity_expansion.aggregation.representative_weeks

Representative weeks to use instead of full yearly temporal representation.

Options:

- "None": Full yearly temporal representation is used or another aggregation.
- list[int]: a list of integers specifying weeks of year to use as representative. Weeks
  of year are defined as full weeks (Monday-Sunday) falling within the year. For
  example, if the list is "[1]" the model will only use the first full week of each
  modelled year.

Examples:

```representative_weeks: [12, 25, 40]```

#### temporal.capacity_expansion.aggregation.named_representative_weeks

Named representative weeks to use instead of full yearly temporal representation.

Options:

- "None": Full yearly temporal representation is used or another aggregation.
-  list[str]: A list of strings from the following options: peak-demand, residual-peak-demand, minimum-demand,
   residual-minimum-demand, peak-consumption, residual-peak-consumption. Only weeks which fall fully within a model
   calendar or financial year are considered for selection.
``

Examples:

```named_representative_weeks: [residual-peak-demand, minimum-demand]```

### temporal.operational

The temporal settings for the operational phase of the modelling.

#### temporal.operational.resolution_min

The temporal resolution in minutes. Currently, only the 30 min resolution is
implemented/allowed. Value should be provided as an integer.

Examples:

```resolution_min: 30```

#### temporal.operational.reference_year_cycle

The order in which different weather reference years are used in the model. If the
number of reference years is less than the number of model years the reference years
will be reused in the order provided.

Examples:

```reference_year_cycle: [2018, 2021, 2023]```

#### temporal.operational.horizon

The number of time intervals to optimise over per iteration of the operational rolling
horizon optimisation.

Examples:

```horizon: 96```

#### temporal.operational.overlap

The number of time intervals to overlap between the current and previous iterations
of the rolling horizon optimisation.

If a horizon of 96 (48*2) and overlap of 48 was used, with a 30 min resolution, this
would be equivalent to daily rolling horizon with a one day look ahead.

Examples:

```overlap: 48```

#### temporal.operational.aggregation.representative_weeks

Representative weeks to use instead of full yearly temporal representation.

Options:

- "None": Full yearly temporal representation is used or another aggregation.
- list[int]: a list of integers specifying weeks of year to use as representative. Weeks
  of year are defined as full weeks (Monday-Sunday) falling within the year. For
  example, if the list is "[1]" the model will only use the first full week of each
  modelled year.

Examples:

```representative_weeks: [12, 25, 40]```

#### temporal.operational.aggregation.named_representative_weeks

Named representative weeks to use instead of full yearly temporal representation.

Options:

- "None": Full yearly temporal representation is used or another aggregation.
-  list[str]: A list of strings from the following options: peak-demand, residual-peak-demand, minimum-demand,
   residual-minimum-demand, peak-consumption, residual-peak-consumption. Only weeks which fall fully within a model
   calendar or financial year are considered for selection.

Examples:

```named_representative_weeks: [residual-peak-demand, minimum-demand]```

## Solver

### solver

External solver to use.

Options (refer to https://pypsa.readthedocs.io/en/latest/getting-started/installation.html):

Free, and by default, installed with ISPyPSA:

- "highs"

Free, but must be installed by the user:

- "cbc"
- "glpk"
- "scip"

Not free and must be installed by the user:

- "cplex"
- "gurobi"
- "xpress"
- "mosek"
- "copt"
- "mindopt"
- "pips"

Examples:

```solver: highs```
