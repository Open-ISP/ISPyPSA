The method description provided here is intended as a plain english
explanation that links the ISPyPSA inputs and model config to the formulation of the
optimisation model in PyPSA.

## Overview

Mix-integer linear programing is used to represent the National Electricity Market's
generation and transmission infrastructure, future costs, resource
availability, demand for electricity, policy objectives, and operational and investment
decisions. Optimisation can than be used to determine which combination of operational
and investment decisions would result in the lowest total cost. The methods description
which follows is a plain english description of how various aspects of the NEM are
represented within the mix-integer linear model.

## Nodal representation

Each sub region and REZ is represented as a separate node within the model:

- Generators, storages, and loads are located at particular nodes, with energy able to
flow freely, without constraint or losses between any generator, storage, or load
connected to the same node.

- The sub region nodes are defined by the `isp_sub_region_id` column in the
ISPyPSA [sub_regions](tables/ispypsa.md#sub_regions) input table.
- The REZ nodes are defined by the `rez_id` column in the
ISPyPSA [renewable_energy_zones](tables/ispypsa.md#renewable_energy_zone) input table.

## Demand

Demand is represented as a time varying quantity at each sub region node.

- Local supply at the node must equal demand, plus net transmission outflow, minus the
  quantity of unserved energy at the node.
- The model config parameters [unserved_energy.cost](config.md#unserved_energycost) and
[unserved_energy.max_per_node](config.md#unserved_energymax_per_node) can be used to
configure cost and the allowable quantity of unserved energy. The default cost of unserved
energy is set very high (10,000 $/MWh) to incentivise the optimisation to allow unserved
energy only as a last resort. However, allowing some unserved energy helps prevent model
infeasibility.
- The sub region demand data used is the demand data published by AEMO.
- The historical weather years, or reference years, used as a basis for deriving the time vary
demand data are defined using the `reference_year_cycle` options in the config. [More detail on
reference years](#reference-years).
- The time varying quantity of demand at each node is also dependent on the model year.
AEMO publishes demand data for every year in modelling horizon for each reference year, with demand
changing over time due to economic growth, CER uptake, energy efficiency etc.

## Transmission

The transmission network is represented as the ability for energy to flow from node
to node subject to a power constraint:

- Currently, transmission losses are not implemented in the model.

### Sub-region to sub-region

Transmission between sub regions is implemented with static constraints on power
flow in each direction:

- The column `flow_path` in the ISPyPSA input table [flow_paths](tables/ispypsa.md#flow_paths)
defines the names of the model flow paths and the columns `node_from` and `node_to` define the
nodes linked by the flow path.
- The two values `forward_direction_mw_summer_typical` and
`reverse_direction_mw_summer_typical` from the ISPyPSA inputs table `flow_paths`
set the limits on power flow in the forward and reverse flow directions.
- TODO: Implement time varying transmission limits making use of the peak demand and
winter limits provided in the IASR workbook.

### REZ exports

REZ exports are implemented with a single static limit on power flow that applies in
both directions of flow:

- The value `rez_transmission_network_limit_summer_typical` from the ISPyPSA input table
`renewable_energy_zones` is used to set power flow limit.
- TODO: Implement time varying export limits making use of the peak demand and
winter limits provided in the IASR workbook.
- If a NA or blank value is provided then the `rez_to_sub_region_transmission_default_limit`
from the config file is used to set the limit. This is typically set to high value (1e5). Using
the default limit is done for REZs where [custom contraints](#custom-constraints) are used to
model REZ export limits, such that the static limit will not influence the optimisation.

### Custom constraints

Custom constraints create arbitrary linear constraints linking the capacity or output of model
components:

- In the base model implemented by the default work flow, custom constraints are used to
represent complex network dynamics which link REZ exports to transmission flow between ISP
sub regions and the dispatch of existing generators.
- The ISPyPSA tables [custom_constraints_rhs](tables/ispypsa.md#custom_constraints_rhs) and
[custom_constraint_lhs](tables/ispypsa.md#custom_constraints_rhs) are used to define
the custom linear constraints applied to the model. See the docs for these tables for
further information on custom constraint implementation.

## Generation

## Storage

## Reference years

Weather reference years are used ensure weather correlations are consistent between demand
and renewable energy availability, and to ensure the modelling considers diverse weather
conditions:

- For each model year between the model start year and model end year, a historical reference year
is chosen from which to derive both demand and renewable energy availability.
- In the model config the inputs [temporal.capacity_expansion.reference_year_cycle](config.md#temporalcapacity_expansionreference_year_cycle)
and [temporal.operational.reference_year_cycle](config.md#temporaloperationalreference_year_cycle)
are used to specify the ordering of reference years.
- If the `reference_year_cycle` is shorter than the model horizon then the cycle is repeated as many
times as needed.

## Temporal aggregation

Temporal aggregation is used condense the temporal representation of time varying quantities
within the model to improve computational tractability while retaining good representation of
their characteristics:

- If no temporal aggregation is specified then a full half hourly representation is retained for
each year.
- If temporal aggregation is used, the contribution of each time interval in the model are scaled
up so that operational costs, fuel use, and emissions are equivalent to a fully time resolved model.
See [Investment periodisation and discounting](#investment-periodisation-and-discounting) for
further detail on interval weighting.

### Representative weeks

The `representative_weeks` input in the model config can be used to specify particular weeks
to include in the temporal representation:

- A list of integers is provided for `representative_weeks` and these number weeks with in the
year will retained in the time sequence used by the model. For example, if `[1, 25]` are provided
then the first and twenty fifth weeks in each model year will be retained.
- Counting of weeks within the year starts from the first whole week (Monday to Sunday) which
falls within the year.
- Separate sets of representative weeks can be specified for capacity expansion and operational
modelling using the [temporal.capacity_expansion.aggregation.representative_weeks](config.md#temporalcapacity_expansionaggregationrepresentative_weeks)
and [temporal.operational.aggregation.representative_weeks](config.md#temporaloperationalaggregationrepresentative_weeks)
config inputs.
- If both `representative_weeks` and `named_representative_weeks` are given, then weeks from both aggregations are used.
  If the same week is selected twice only one instance is kept.
- TODO: Clarify that representative weeks treated sequential by PyPSA.

### Named representative weeks

The `named_representative_weeks` input in the model config can be used to specify particular weeks
to include in the temporal representation:

- A list of strings is provided for `named_representative_weeks` specifying the weeks to be
extracted from the yearly data. For example, ["residual-peak-demand", "residual-minimum-demand"].
- The named representative week options are:

    - peak-demand: Week with highest instantaneous demand
    - minimum-demand: Week with lowest instantaneous demand
    - peak-consumption: Week with highest average demand (energy consumption)
    - residual-peak-demand: Week with highest demand net of renewables
    - residual-minimum-demand: Week with lowest demand net of renewables
    - residual-peak-consumption: Week with highest average demand net of renewables

- Only whole weeks which fall entirely within a model year are consider for selection.
- Separate sets of named representative weeks can be specified for capacity expansion and operational
modelling using the [temporal.capacity_expansion.aggregation.named_representative_weeks](config.md#temporalcapacity_expansionaggregationnamed_representative_weeks)
and [temporal.operational.aggregation.named_representative_weeks](config.md#temporaloperationalaggregationnamed_representative_weeks)
config inputs.
- If both `representative_weeks` and `named_representative_weeks` are given, then weeks from both aggregations are used.
  If the same week is selected twice only one instance is kept.

## Capacity expansion

Capacity expansion is the first modelling phase. In this phase investment in generation and capacity expansion are
co-optimised with operational dispatch decisions. The optimisation assumes perfect foresight.

### Investment periodisation and discounting

Capacity expansion decisions are periodised with investment decisions made at the start of each
multiyear period:

- Instead of investment decisions being available every year, they are only available at the being
of each investment period. This reduces computational complexity.
- The config input [temporal.capacity_expansion.investment_periods](config.md#temporalcapacity_expansioninvestment_periods)
can be used to specify the investment periodisation. For example, specify `[2025, 2030]` will create
two investment periods in the model, one starting at the beginning of 2025 and ending at the
beginning of 2030, and a second period starting at the beginning of 2030 and lasting to the end of
the modelling horizon.
- The periodisation is not myopic. The optimisation considers investment across all periods
simultaneously.

The periodisation is also used to structure the application of discount rates to the model:

- An objective function weighting is calculated for each investment period and applied to both capacity expansion and
operational costs accrued during an investment period.
- The weighting for a period is calculated as the sum of discount factors used to adjust costs to Net Present Value on
yearly basis over the duration of an investment period:

$$
w_{p}=\sum_{i=t_{period\_start}}^{t_{period\_end}}\frac{1}{(1 + r)^i}
$$

  Where $r$ is the [discount_rate](config.md#discount_rate) specified in the model config.

- The sum of discount factors rather than the average is used such that capital cost which are applied once per
investment period are scaled up by the number of years in the investment period. This is equivalent to taking the
average discount factor for the period and multiplying by the number of years.
- Note: because the period weighting is also applied to operational costs, the interval level weighting/scaling used
to account for temporal aggregation is calculated so that all intervals in an investment period have the equivalent
weight of one year of full time resolved intervals. Then, when the period weighting is applied the costs are scaled back
up to match the number of years in the investment period.

### Transmission

The model considers the expansion of sub region to sub region and REZ export transmission capacity at the start of every
[investment period](#investment-periodisation-and-discounting):

- The capital costs in $/MW for transmission expansion are provided in the ISPyPSA tables
[flow_path_expansion_costs](tables/ispypsa.md#flow_path_expansion_costs) and
[rez_transmission_expansion_costs](tables/ispypsa.md#rez_transmission_expansion_costs). The costs used are those in cost
columns corresponding to the start years of each investment period.
- Costs are annuitised according to the following formula:

$$
c_{a} =\frac{c_{o} \times r }{1 - (1 + r)^{-t}}
$$

Where $c_{a}$ is the annuitised cost, $c_{o}$ is the overnight build cost, $r$ is the [WACC](config.md#wacc), and $t$ is
the network cost [annuitisation_lifetime](config.md#networkannuitisation_lifetime).

- If a transmission flow path or REZ connection is expanded in one investment period it's annuitised cost multiplied by
the size of the expansion is incurred in all subsequent investment periods. Transmission expansions are not considered
to retire.
- The value in column `additional_network_capacity_mw` in
[flow_path_expansion_costs](tables/ispypsa.md#flow_path_expansion_costs) and
[rez_transmission_expansion_costs](tables/ispypsa.md#rez_transmission_expansion_costs) sets a limit on the total
expansion allowed for each flow path and REZ connection.
- Where REZ export limits are set by [custom constraints](#custom-constraints) the expansion of the REZ connection is
modelled as relaxing the custom constraint limit.

### Generation

## Operational

Operational is the second modelling phase. In this modelling phase capacity expansion decisions are taken as fixed,
at the values found during the capacity expansion phase, and only operational decisions are optimised:

- Operational optimisation is done using a rolling horizon, breaking up the optimisation into many smaller problems
which can be solved sequentially.
- The rolling horizon formulation has two additional config parameters [horizon](config.md#temporaloperationalhorizon)
and [overlap](config.md#temporaloperationaloverlap). `horizon` controls the number of intervals in each sub problem and
`overlap` controls the number of intervals overlapping between each sub problem. If `overlap` is greater than zero then
the second optimisations decisions are recorded as the model dispatch values. The overlap gives each sub problem a look
ahead so it is not completely naive to future conditions.
- Fixing investment decisions and using rolling horizons reduce model complexity and speed up run time, allowing for
operation to be modelled with greater temporal resolution than capacity expansion.

