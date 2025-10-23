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
See the [Workflow overview](workflow.md) section for a high-level overview of
the default ISPyPSA workflow.

## Installation

See the [Installation section](getting_started.md#installation) in the Getting Started guide.

## Basic Usage

The basic command structure is:

=== "uv"

    ```commandline
    uv run ispypsa config=<config_file> [task]
    ```

=== "plain python"

    ```commandline
    ispypsa config=<config_file> [task]
    ```

The `config=<config_file>` argument is required for task execution and must point to a valid ISPyPSA configuration YAML file.

!!! important

    For the sake of brevity the commands on the rest of this page are given in plain
    Python, but can be run with `uv run` as well.

### Examples

```bash
# List available tasks (no config required)
ispypsa list

# Run a specific task with config
ispypsa config=my_config.yaml create_and_run_capacity_expansion_model

# Run all tasks with config
ispypsa config=my_config.yaml

```

## Tasks

The ISPyPSA CLI workflow consists of a series of tasks, each dependent on the
outputs of previous tasks. The following diagram shows the task dependency
graph:

```
save_config
│
└──> cache_required_iasr_workbook_tables
     │
     └──> create_ispypsa_inputs
          │
          ├──> create_pypsa_friendly_inputs
          │    │
          │    ├──> create_and_run_capacity_expansion_model
          │    │    │
          └────┼────┼──> create_operational_timeseries
               │    │    │
               └────┴────┴──> create_and_run_operational_model

```

!!! important

    Each task depends on the outputs of previous tasks. If a particular task is run, but the
    previous tasks' runs on which it depends isn't up to date, then the CLI will
    detect this and also run the previous tasks. The detection of a previous task's
    being 'up to date' is based on two checks 1) its input files haven't been modified
    since it last ran and 2) its output files exist. If either 1) or 2) aren't true then
    a task is not up to date and will be rerun. This applies to both the primary target
    task and all of its dependencies.

### cache_required_iasr_workbook_tables

Extracts data from the ISP Excel workbook and caches it as CSV files.

```bash
ispypsa config=config.yaml cache_required_iasr_workbook_tables
```

**Inputs:**

- IASR Excel workbook (specified in config `paths.workbook_path`)

**Outputs:**

- CSV files in the workbook cache directory (location specified in config `paths.
  parsed_workbook_cache`)

### create_ispypsa_inputs

Generates ISPyPSA format input tables from the cached workbook data.

```bash
ispypsa config=config.yaml create_ispypsa_inputs
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
ispypsa config=config.yaml create_pypsa_friendly_inputs
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

### create_and_run_capacity_expansion_model

Creates the PyPSA network object and runs the capacity expansion optimization.

```bash
ispypsa config=config.yaml create_and_run_capacity_expansion_model
```

**Inputs:**

- PyPSA-friendly tables in `{run_directory}/{run_name}/pypsa_friendly/` (run_directory
  and run_name specified in config)
- Time series data in `{run_directory}/{run_name}/pypsa_friendly/capacity_expansion_timeseries/`
  (run_directory and run_name specified in config)

**Outputs:**

- Optimized capacity expansion results in `{run_directory}/{run_name}/outputs/capacity_expansion.nc`

**Skip Optimization Option:**

You can skip the optimization step and only build the network using the `dont_run_capacity_expansion` flag:

```bash
ispypsa config=config.yaml dont_run_capacity_expansion=True create_and_run_capacity_expansion_model
```

This is particularly useful for:
- Testing that your model configuration is valid
- Verifying network construction without waiting for optimization
- Debugging model setup issues
- Creating a network file for manual inspection or custom optimization

When this flag is set to `True`, the task will:
1. Build the complete PyPSA network with all components and constraints
2. Save the unoptimized network to the output file
3. Skip the potentially time-consuming optimization step

### create_operational_timeseries

Creates time series data for operational modeling.

```bash
ispypsa config=config.yaml create_operational_timeseries
```

**Inputs:**

- ISPyPSA input tables in `{run_directory}/{run_name}/ispypsa_inputs/tables/`
  (run_directory and run_name specified in config)
- Trace data (demand, wind, solar) (location specified in config `paths.
  parsed_traces_directory`)

**Outputs:**

- Operational time series data in `{run_directory}/{run_name}/pypsa_friendly/operational_timeseries/`

### create_and_run_operational_model

Prepares the PyPSA network object for operational modeling using fixed capacities from capacity expansion and runs the operational optimization.

```bash
ispypsa config=config.yaml create_and_run_operational_model
```

**Inputs:**

- Capacity expansion results in `{run_directory}/{run_name}/outputs/capacity_expansion.nc`
- PyPSA-friendly tables in `{run_directory}/{run_name}/pypsa_friendly/`
- Operational time series data in `{run_directory}/{run_name}/pypsa_friendly/operational_timeseries/`

**Outputs:**

- Operational optimization results in `{run_directory}/{run_name}/outputs/operational.h5`

!!! note "Running Without Capacity Expansion"

    The operational model can be built and run even if the capacity expansion optimization was
    skipped (using `dont_run_capacity_expansion=True`). However, this will significantly affect
    the operational model outputs because:

    - The network will use initial capacities rather than optimized capacities
    - No new generation or transmission capacity will have been added
    - The operational model may be infeasible if initial capacities are insufficient
    - Results will not reflect least-cost capacity investment decisions

    This can be useful for testing but should not be used for final analysis.

**Skip Optimization Option:**

You can skip the optimization step and only prepare the network using the `dont_run_operational` flag:

```bash
ispypsa config=config.yaml dont_run_operational=True create_and_run_operational_model
```

This is particularly useful for:

- Testing operational model setup without running the full optimization
- Verifying that capacity expansion results load correctly
- Debugging time series data integration
- Creating a prepared network for custom operational analysis

When this flag is set to `True`, the task will:

1. Load the capacity expansion results
2. Update the network with operational time series data
3. Fix the optimal capacities from capacity expansion
4. Save the prepared network without running the rolling horizon optimization
5. Skip the potentially very long operational optimization process

### list

Shows all available tasks and their status. No config file required.

```bash
ispypsa list
```

### save_config

Saves a copy of the configuration file to the run directory. This task runs automatically as a dependency of other tasks to preserve the exact configuration used for reproducibility.

```bash
ispypsa config=config.yaml save_config
```

**Inputs:**

- Configuration YAML file (specified with `config=`)

**Outputs:**

- Copy of the configuration file in `{run_directory}/{run_name}/`
  (preserves the exact config used for the run)

**Note:** This task always runs (never considers itself up-to-date) to ensure the config file is always current.

## Configuration

The `config=` argument is required for task execution. It accepts either absolute or relative
paths:

```bash
# Absolute path
ispypsa config=/home/user/projects/my_config.yaml task_name

# Relative path (from current directory)
ispypsa config=../configs/my_config.yaml task_name

# File in current directory
ispypsa config=my_config.yaml task_name
```

See the [example configuration file](examples/ispypsa_config.yaml) for the required format.

## Examples

### Complete Workflow

Run all tasks one by one to generate model results:

```bash
# Extract workbook data
ispypsa config=config.yaml cache_required_iasr_workbook_tables

# Generate ISPyPSA inputs
ispypsa config=config.yaml create_ispypsa_inputs

# At this stage the ISPyPSA inputs could be edited to adjust build cost or any other
# inputs set out in {run_directory}/{run_name}/ispypsa_inputs/tables/

# Convert to PyPSA format and run capacity expansion
ispypsa config=config.yaml create_and_run_capacity_expansion_model

# Create operational time series
ispypsa config=config.yaml create_operational_timeseries

# Run operational optimization
ispypsa config=config.yaml create_and_run_operational_model
```

### Simplified Complete Workflow

The complete workflow can be run in fewer steps, with intermediate tasks run
automatically when their outputs are required.

```bash
# Generate ISPyPSA inputs
ispypsa config=config.yaml create_ispypsa_inputs

# At this stage the ISPyPSA inputs could be edited to adjust build cost or any other
# inputs set out in {run_directory}/{run_name}/ispypsa_inputs/tables/

# Run complete workflow (all remaining tasks)
ispypsa config=config.yaml create_and_run_operational_model
```

### Running from Different Directories

The CLI works correctly from any directory and handles relative paths appropriately:

```bash
# From project root - no config needed for list
ispypsa list

# From project root - config for task execution
ispypsa config=ispypsa_config.yaml create_ispypsa_inputs

# From a subdirectory
cd analysis
ispypsa config=../ispypsa_config.yaml create_ispypsa_inputs

# Using an absolute path (works from anywhere)
ispypsa config=/home/user/ispypsa/config.yaml create_ispypsa_inputs
```

### Using Different Configurations

You can easily switch between different model configurations:

```bash
# Development configuration
ispypsa config=configs/dev_config.yaml create_and_run_capacity_expansion_model

# Production configuration with different parameters
ispypsa config=configs/prod_config.yaml create_and_run_capacity_expansion_model

# Test configuration with smaller dataset
ispypsa config=configs/test_config.yaml create_and_run_capacity_expansion_model
```

### Debug Mode

Use the `debug=True` flag to see detailed information about config file resolution:

```bash
ispypsa config=config.yaml debug=True create_ispypsa_inputs
```

Debug output includes:

- Resolved config file path
- Working directory information

### Skip Optimization Flags

You can skip the optimization step in modeling tasks using these flags:

```bash
# Skip capacity expansion optimization (only build the network)
ispypsa config=config.yaml dont_run_capacity_expansion=True create_and_run_capacity_expansion_model

# Skip operational optimization (only prepare the network)
ispypsa config=config.yaml dont_run_operational=True create_and_run_operational_model

# Run both optimizations normally (default behavior)
ispypsa config=config.yaml dont_run_capacity_expansion=False dont_run_operational=False create_and_run_operational_model
```

These flags are useful for:

- Debugging network construction without waiting for optimization
- Testing model setup and configuration
- Preparing networks for manual optimization or analysis

### Path Issues

If you encounter path-related errors:

1. Use `debug=True` to see how paths are being resolved
2. Try using absolute paths in your config file
3. Ensure all directories exist before running tasks
4. Check file permissions for input/output directories

## Advanced Usage

### How ispypsa Works

The `ispypsa` command is built on the `doit` task automation tool with ISPyPSA-specific enhancements:

1. **run command**: Uses doit's native run command with `config=value` parameter support
2. **Other commands**: Uses doit's built-in commands (list, help, etc.) directly
3. **Lazy loading**: Configuration is only loaded when tasks actually execute
4. **Path resolution**: All paths work relative to where you run the command

### Passing Additional doit Options

You can pass any doit option to the run command:

```bash
# Run with verbose output
ispypsa config=config.yaml -v create_and_run_capacity_expansion_model

# Always execute task ignoring up-to-date checks
ispypsa config=config.yaml -a create_ispypsa_inputs

# Continue executing even after task failure
ispypsa config=config.yaml --continue create_and_run_operational_model
```

Common doit options for the run command:

- `-v` / `--verbosity`: Set verbosity level (0-2)
- `-a` / `--always-execute`: Always execute tasks even if up-to-date
- `--continue`: Continue executing tasks even after failure
- `-s` / `--single`: Execute only specified tasks ignoring their dependencies

### Commands Without Config

Some commands work without requiring a config file:

```bash
# List all available tasks
ispypsa list

# Show help
ispypsa help

# Show help for specific command
ispypsa help run
```

### Working Directory Behavior

The CLI maintains your current working directory:

- All paths in config files work relative to where you run the command
- No directory changes occur during execution
- Output files are created according to paths in your config file
