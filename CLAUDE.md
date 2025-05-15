# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ISPyPSA is an open-source capacity expansion modelling tool based on the methodology and assumptions used by the Australian Energy Market Operator (AEMO) to produce their Integrated System Plan. It leverages the capabilities of PyPSA (Python for Power System Analysis), an open source toolbox for simulating and optimising modern power and energy systems.

## Common Commands

### Development Setup

```bash
# Install uv first (see https://github.com/astral-sh/uv for instructions)
uv sync  # Install dependencies
uv run pre-commit install  # Set up git hooks
```

### Running Tests

```bash
# Run all tests with coverage reporting
uv run --frozen pytest

# Run a specific test file
uv run --frozen pytest tests/test_model/test_initialise.py

# Run a specific test function
uv run --frozen pytest tests/test_model/test_initialise.py::test_network_initialisation

# Run tests with verbose output
uv run --frozen pytest -v
```

### Code Formatting and Linting

```bash
# Use ruff through uv for formatting and linting
uvx ruff check --fix
uvx ruff format
```

## Project Architecture

### Key Components

1. **Config** (`src/ispypsa/config/`)
   - Handles loading and validation of model configuration from YAML files

2. **Data Fetching** (`src/ispypsa/data_fetch/`)
   - Handles reading and writing CSV files

3. **Templater** (`src/ispypsa/templater/`)
   - Creates ISPyPSA inputs from the AEMO IASR workbook data
   - Includes handling for renewable energy zones, nodes, generators, and other components

4. **Translator** (`src/ispypsa/translator/`)
   - Transforms ISPyPSA format inputs into PyPSA-friendly inputs
   - Handles buses, generators, lines, snapshots, and timeseries

5. **Model** (`src/ispypsa/model/`)
   - Builds and runs the PyPSA network
   - Includes modules for initializing the network, building components, and saving results

### Workflow

The typical model workflow consists of these stages:
1. Cache required tables from ISP workbooks
2. Create ISPyPSA inputs from cached tables (templating stage)
3. Translate ISPyPSA inputs to PyPSA-friendly format
4. Build and solve the PyPSA model
5. Optionally perform operational modelling with fixed capacities

The workflow can be executed using doit tasks (see dodo.py) or programmatically (see example_workflow.py).

### Configuration

Model configuration is specified in YAML files (see `ispypsa_runs/development/ispypsa_inputs/ispypsa_config.yaml` for an example). Key configuration options include:
- Scenario selection (Progressive Change, Step Change, Green Energy Exports)
- Network configuration (nodes, REZs, transmission expansion)
- Temporal settings (years, resolution, representative periods)
- Solver selection

## Working with the Codebase

When making changes:
1. Understand how data flows through the system (IASR workbooks → templater → translator → PyPSA model)
2. Follow existing code patterns and naming conventions
3. All code changes should pass tests and linting (enforced by pre-commit hooks)
4. Update tests for any new functionality

## Testing

- Test functions with dataframe outputs by comparing to hardcoded expected dataframe
  definitions.
- Prefer simple testing infrastructure similar to what is already in use.
