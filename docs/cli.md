# ISPyPSA Command Line Interface

The `ispypsa` command provides a user-friendly interface for running ISPyPSA workflows.
It's the simplest and quickest way to run the predefined workflows. However, if more
flexibility is required the [API](api.md) might be a better option.

## Overview

The `ispypsa` command allows you to:

  - Extract data from ISP workbooks
  - Generate model input files
  - Run capacity expansion and operational models
  - Manage workflow outputs

All commands require a configuration file that specifies paths and model parameters.
See the [Workflow overview](workflow/workflow.md) section for a high-level overview of
the default ISPyPSA workflow.

## Installation

See the [Installation section](getting_started.md#installation) in the Getting Started guide.

## Basic Usage

The basic command structure is:

=== "uv"

    ```commandline
    uv run ispypsa --config <config_file> <task>
    ```

=== "plain python"

    ```commandline
    ispypsa --config <config_file> <task>
    ```

The `--config` argument is required and must point to a valid ISPyPSA configuration YAML file.

!!! important

    For the sake of brevity the commands on the rest of this page are given in plain
    Python, but can be run with `uv run` as well.

### Examples

```bash
# Run the complete up to and including capacity expansion
ispypsa --config my_config.yaml run_capacity_expansion

# List available tasks
ispypsa --config my_config.yaml list

# Clean output files
ispypsa --config my_config.yaml clean
```

## Tasks

The ISPyPSA CLI workflow consists of a series of tasks, each dependent on the
outputs of previous tasks. The following diagram shows the task dependency
graph:

```
cache_required_iasr_workbook_tables
│
└──> create_ispypsa_inputs
     │
     ├──> create_pypsa_friendly_inputs
     │    │
     │    └──> create_pypsa_network_object_for_capacity_expansion_modelling
     │         │
     │         └──> run_capacity_expansion
     │                                  │
     └──> create_operational_timeseries │
          │                             │
          └───────────────────────────────┴──> create_pypsa_object_for_operational_modelling
                                            │
                                            └──> run_operational_model
```

Note: `create_pypsa_object_for_operational_modelling` depends on:

- `run_capacity_expansion` (for the optimized capacity expansion network)
- `create_operational_timeseries` (for the operational time series data)
- `create_pypsa_friendly_inputs` (for the PyPSA-friendly tables)

!!! important

    Each task depends on the outputs of previous tasks. If a particular task is run, but the
    previous tasks on which it depends haven't been run yet, then the CLI will detect this
    and also run the previous tasks. The detection of a previous task's completeness is
    based on if the files it outputs have been created. Deleting task output files will
    trigger reruns of a task, but editing files will not.

### cache_required_iasr_workbook_tables

Extracts data from the ISP Excel workbook and caches it as CSV files.

```bash
ispypsa --config config.yaml cache_required_iasr_workbook_tables
```

**Inputs:**

- IASR Excel workbook (specified in config `paths.workbook_path`)

**Outputs:**

- CSV files in the workbook cache directory (location specified in config `paths.
  parsed_workbook_cache`)

### create_ispypsa_inputs

Generates ISPyPSA format input tables from the cached workbook data.

```bash
ispypsa --config config.yaml create_ispypsa_inputs
```

**Inputs:**

- Cached workbook CSV files (location specified in config `paths.
  parsed_workbook_cache`)

**Outputs:**

- ISPyPSA input tables in `{run_directory}/{run_name}/ispypsa_inputs/tables/`
  (run_directory and run_name specified in config)

### create_pypsa_friendly_inputs

Converts ISPyPSA format tables to PyPSA-compatible format and generates time series data for capacity expansion.

```bash
ispypsa --config config.yaml create_pypsa_friendly_inputs
```

**Inputs:**

- ISPyPSA input tables in `{run_directory}/{run_name}/ispypsa_inputs/tables/`
  (run_directory and run_name specified in config)
- Trace data (demand, wind, solar) (location specified in config `paths.
  parsed_traces_directory`)

**Outputs:**

- PyPSA-friendly tables in `{run_directory}/{run_name}/pypsa_friendly/` (run_directory
  and run_name specified in config)
- Time series data in `{run_directory}/{run_name}/pypsa_friendly/capacity_expansion_timeseries/`
  (run_directory and run_name specified in config)

### create_pypsa_network_object_for_capacity_expansion_modelling

Creates the PyPSA network object for capacity expansion modeling.

```bash
ispypsa --config config.yaml create_pypsa_network_object_for_capacity_expansion_modelling
```

**Inputs:**

- PyPSA-friendly tables in `{run_directory}/{run_name}/pypsa_friendly/` (run_directory
  and run_name specified in config)
- Time series data in `{run_directory}/{run_name}/pypsa_friendly/capacity_expansion_timeseries/`
  (run_directory and run_name specified in config)

**Outputs:**

- PyPSA network object in `{run_directory}/{run_name}/outputs/{run_name}_capacity_expansion.h5`

### run_capacity_expansion

Runs the capacity expansion optimization model on the prepared network.

```bash
ispypsa --config config.yaml run_capacity_expansion
```

**Inputs:**

- PyPSA network object in `{run_directory}/{run_name}/outputs/{run_name}_capacity_expansion.h5`

**Outputs:**

- Optimized model results in `{run_directory}/{run_name}/outputs/{run_name}_capacity_expansion.h5`

### create_operational_timeseries

Creates time series data for operational modeling.

```bash
ispypsa --config config.yaml create_operational_timeseries
```

**Inputs:**

- ISPyPSA input tables in `{run_directory}/{run_name}/ispypsa_inputs/tables/`
  (run_directory and run_name specified in config)
- Trace data (demand, wind, solar) (location specified in config `paths.
  parsed_traces_directory`)

**Outputs:**

- Operational time series data in `{run_directory}/{run_name}/pypsa_friendly/operational_timeseries/`

### create_pypsa_object_for_operational_modelling

Prepares the PyPSA network object for operational modeling using fixed capacities from capacity expansion.

```bash
ispypsa --config config.yaml create_pypsa_object_for_operational_modelling
```

**Inputs:**

- Capacity expansion results in `{run_directory}/{run_name}/outputs/{run_name}_capacity_expansion.h5`
- PyPSA-friendly tables in `{run_directory}/{run_name}/pypsa_friendly/`
- Operational time series data in `{run_directory}/{run_name}/pypsa_friendly/operational_timeseries/`

**Outputs:**

- Operational network object in `{run_directory}/{run_name}/outputs/{run_name}_operational.h5`

### run_operational_model

Runs the operational optimization with rolling horizon on the prepared operational network.

```bash
ispypsa --config config.yaml run_operational_model
```

**Inputs:**

- Operational network object in `{run_directory}/{run_name}/outputs/{run_name}_operational.h5`

**Outputs:**
- Operational results in `{run_directory}/{run_name}/outputs/{run_name}_operational.h5`

### list

Shows all available tasks and their status.

```bash
ispypsa --config config.yaml list
```

### clean

Removes all generated files (targets) from all tasks. This is a built-in doit command.

```bash
ispypsa --config config.yaml clean
```

**Note:** This will delete all files that were created as targets by the tasks, including:

- Cached workbook tables
- ISPyPSA input tables
- PyPSA-friendly tables
- Time series data
- Model output files

## Configuration

The `--config` argument accepts either absolute or relative paths:

```bash
# Absolute path
ispypsa --config /home/user/projects/my_config.yaml list

# Relative path (from current directory)
ispypsa --config ../configs/my_config.yaml list

# File in current directory
ispypsa --config my_config.yaml list
```

See the [example configuration file](examples/ispypsa_config.yaml) for the required format.

## Examples

### Complete Workflow

Run all tasks one by one to generate model results:

```bash
# Extract workbook data
ispypsa --config config.yaml cache_required_iasr_workbook_tables

# Generate ISPyPSA inputs
ispypsa --config config.yaml create_ispypsa_inputs

# At this stage the ISPyPSA inputs could be edited to adjust build cost or any other
# inputs set out in {run_directory}/{run_name}/ispypsa_inputs/tables/

# Convert to PyPSA format
ispypsa --config config.yaml create_pypsa_friendly_inputs

# At this stage the PyPSA format inputs could also be edited to achieve fine grained
# control of the model formulation. However, for most use case we recommend editing the
# ISPyPSA inputs.

# Create capacity expansion network
ispypsa --config config.yaml create_pypsa_network_object_for_capacity_expansion_modelling

# Run capacity expansion optimization
ispypsa --config config.yaml run_capacity_expansion

# Create operational time series
ispypsa --config config.yaml create_operational_timeseries

# Create operational network
ispypsa --config config.yaml create_pypsa_object_for_operational_modelling

# Run operational optimization
ispypsa --config config.yaml run_operational_model
```

### Simplified Complete Workflow

The complete workflow can be run in two stages, the intermediate step run
automatically with CLI detecting that they are required. Running in two steps allows the
ISPyPSA inputs to be edited before capacity expansion and operational modelling are run.

```bash
# Generate ISPyPSA inputs
ispypsa --config config.yaml create_ispypsa_inputs

# At this stage the ISPyPSA inputs could be edited to adjust build cost or any other
# inputs set out in {run_directory}/{run_name}/ispypsa_inputs/tables/

# Optionally run operational model
ispypsa --config config.yaml run_operational_model
```

### Running from Different Directories

The CLI correctly handles relative paths when run from different directories:

```bash
# From project root
ispypsa --config ispypsa_runs/development/ispypsa_inputs/ispypsa_config.yaml list

# From a subdirectory
cd analysis
ispypsa --config ../ispypsa_runs/development/ispypsa_inputs/ispypsa_config.yaml list

# Using an absolute path (works from anywhere)
ispypsa --config /home/user/ispypsa/config.yaml list
```

### Using Different Configurations

You can easily switch between different model configurations:

```bash
# Development configuration
ispypsa --config configs/dev_config.yaml run_capacity_expansion

# Production configuration with different parameters
ispypsa --config configs/prod_config.yaml run_capacity_expansion

# Test configuration with smaller dataset
ispypsa --config configs/test_config.yaml run_capacity_expansion
```

### Debug Mode

Use the `--debug` flag to see detailed information about path resolution and command execution:

```bash
ispypsa --debug --config config.yaml list
```

Debug output includes:
- Where dodo.py was found
- Working directory information
- Resolved config file path
- Environment variables being set

### Path Issues

If you encounter path-related errors:

1. Use `--debug` to see how paths are being resolved
2. Try using absolute paths in your config file
3. Ensure all directories exist before running tasks
4. Check file permissions for input/output directories

## Advanced Usage

### How ispypsa Works

The `ispypsa` command is a wrapper around the `doit` task automation tool. When you run `ispypsa`, it:

1. Locates the dodo.py file (either in current directory or package installation)
2. Changes to the directory containing dodo.py
3. Sets environment variables for path resolution
4. Executes `doit` with your specified task

### Passing Additional doit Options

You can pass any doit option after the task name:

```bash
# Run with verbose output
ispypsa --config config.yaml run_capacity_expansion -v

# Show task details without running
ispypsa --config config.yaml run_capacity_expansion -n

# Run specific task ignoring dependencies
ispypsa --config config.yaml run_capacity_expansion -a
```

Common doit options:
- `-v` / `--verbosity`: Set verbosity level (0-2)
- `-n` / `--dry-run`: Show tasks without executing
- `-a` / `--always-execute`: Ignore task dependencies
- `--continue`: Continue executing tasks even after failure

### Environment Variables

The CLI uses these environment variables internally:

- `ISPYPSA_CONFIG_PATH`: Resolved path to the config file
- `ISPYPSA_ORIGINAL_CWD`: Original working directory (for relative path resolution)

These are set automatically and should not be modified directly.

### Working Directory Behavior

The CLI handles working directory changes transparently:
- Commands are always executed in the directory containing dodo.py
- Relative paths in configs are resolved from where you run the command
- Output files are created according to paths in your config file
