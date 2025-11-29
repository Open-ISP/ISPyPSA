## Getting started

The [Getting started](getting_started.md) section walks the user through:

- installation process
- obtaining the required data
- and running your first model

## Workflow

The [Workflow](workflow.md) section explains the ISPyPSA data handling
workflow, which converts AEMO inputs and data into a PyPSA model.

## Modelling method

The [Method](method.md) section explains the conceptual details of the model ISPyPSA
builds in PyPSA, and how this is controlled through user settings (config). By
*conceptual*, we mean the descriptions are intended to describe the model in terms any
modeller or analyst can understand, rather than explaining the Python code or the data
processing used to achieve the intended method.

Topics covered include:

- Transmission representation
- Generation representation
- Policy constraints
- Temporal resolution reduction
- Investment periodisation
- Custom constraints

## Config

The [Config](config.md) explains the config file which can be used to control the
modelling process.

## Tables

!!! note "Coming soon"

Most of the inputs in ISPyPSA are specified through CSV files or Pandas
DataFrames. The Tables section will provide detailed descriptions of each table, detailing
the required columns, their units, and their effect on the model.

- [ISPyPSA input tables](tables/ispypsa.md) describe the set of tables taken as inputs
  to the [translator](workflow.md#translating), these tables are the recommended inputs
  for most users to edit and create custom ISP scenarios.
- [PyPSA friendly inputs](tables/ispypsa.md) describe the set of tables taken as inputs
  to the [model](workflow.md#capacity-expansion-model), these tables are provided as audit
  points, but may also be useful for advanced users looking for fine tuned model control.

## API

The [API](api.md) section explains the different ISPyPSA functions used to implement
the modelling workflow. This documentation is helpful for users creating custom
workflows, or looking to reuse individual elements of the ISPyPSA functionality within
their own projects.

## Examples

The Examples section provides different examples of how ISPyPSA can be used. Currently,
this just documents the default API and CLI workflow. In the future we will add a wider
range of examples with various model configuration including both full scale examples,
which show realistic NEM level models and simplified examples intended to help explain
how ISPyPSA works.
