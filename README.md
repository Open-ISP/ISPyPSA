# ISPyPSA
[![Continuous Integration and Deployment](https://github.com/Open-ISP/ISPyPSA/actions/workflows/cicd.yml/badge.svg)](https://github.com/Open-ISP/ISPyPSA/actions/workflows/cicd.yml)
[![codecov](https://codecov.io/gh/Open-ISP/ISPyPSA/graph/badge.svg?token=rcEXuQgfOJ)](https://codecov.io/gh/Open-ISP/ISPyPSA)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/Open-ISP/ISPyPSA/main.svg)](https://results.pre-commit.ci/latest/github/Open-ISP/ISPyPSA/main)
[![UV](https://camo.githubusercontent.com/4ab8b0cb96c66d58f1763826bbaa0002c7e4aea0c91721bdda3395b986fe30f2/68747470733a2f2f696d672e736869656c64732e696f2f656e64706f696e743f75726c3d68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f61737472616c2d73682f75762f6d61696e2f6173736574732f62616467652f76302e6a736f6e)](https://github.com/astral-sh/uv)

An open-source capacity expansion modelling tool based on the methodology and assumptions used by the Australian Energy Market Operator (AEMO) to produce their Integrated System Plan (ISP). Built on [PyPSA](https://github.com/pypsa/pypsa).

**This README is a quick reference.** For detailed instructions, tutorials, and API documentation, see the [full documentation](https://open-isp.github.io/ISPyPSA/):

- [Getting Started](https://open-isp.github.io/ISPyPSA/getting_started/) - Installation and first model run
- [Configuration Reference](https://open-isp.github.io/ISPyPSA/config/) - All configuration options
- [CLI Guide](https://open-isp.github.io/ISPyPSA/cli/) - Command line interface details
- [API Reference](https://open-isp.github.io/ISPyPSA/api/) - Python API for custom workflows
- [Workflow Overview](https://open-isp.github.io/ISPyPSA/workflow/) - How the modelling pipeline works

## Installation

```bash
pip install ispypsa
```

Or with uv:

```bash
uv add ispypsa
```

## Quick Start

1. Download the [example config](ispypsa_config.yaml) and edit paths for your environment
2. Run:

```bash
# Download ISP workbook and trace data
ispypsa config=ispypsa_config.yaml download_workbook
ispypsa config=ispypsa_config.yaml download_trace_data

# Run complete workflow (capacity expansion + operational model)
ispypsa config=ispypsa_config.yaml
```

## CLI Reference

| Task | Description |
|------|-------------|
| `download_workbook` | Download IASR Excel workbook |
| `download_trace_data` | Download wind/solar/demand traces |
| `cache_required_iasr_workbook_tables` | Extract workbook data to CSV cache |
| `create_ispypsa_inputs` | Generate ISPyPSA input tables |
| `create_pypsa_friendly_inputs` | Convert to PyPSA format |
| `create_and_run_capacity_expansion_model` | Build and solve capacity expansion |
| `create_and_run_operational_model` | Build and solve operational model |
| `create_capacity_expansion_plots` | Generate result plots |
| `create_operational_plots` | Generate operational plots |
| `list` | Show available tasks |

## Common Options

```bash
# Override config values on command line
ispypsa config=config.yaml create_plots=True create_and_run_capacity_expansion_model

# Skip optimisation (build network only)
ispypsa config=config.yaml run_optimisation=False create_and_run_capacity_expansion_model

# Force re-run even if up-to-date
ispypsa config=config.yaml -a create_ispypsa_inputs
```

## Output Structure

```
<run_directory>/
└── <ispypsa_run_name>/
   ├── ispypsa_inputs/
   │   ├── build_costs.csv
   │   └── ...
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
   │       └── (same structure as capacity_expansion_timeseries)
   └── outputs/
       ├── capacity_expansion.nc
       ├── capacity_expansion_results_viewer.html
       ├── capacity_expansion_tables
       │   └── ...
       ├── capacity_expansion_plots
       │   └── ...
       ├── operational.nc
       ├── operational_results_viewer.html
       ├── operational_tables
       │   └── ...
       └── operational_plots
           └── ...
```


## Related Projects

- [isp-workbook-parser](https://github.com/Open-ISP/isp-workbook-parser) - Extract data from IASR workbooks
- [isp-trace-parser](https://github.com/Open-ISP/isp-trace-parser) - Process wind/solar/demand trace data

## Contributing

Interested in contributing to the source code or adding table configurations? Check out the [contributing instructions](./CONTRIBUTING.md), which also includes steps to install `ispypsa` for development.

Please note that this project is released with a [Code of Conduct](./CONDUCT.md). By contributing to this project, you agree to abide by its terms.

## License

`ispypsa` was created as a part of the [OpenISP project](https://github.com/Open-ISP). It is licensed under the terms of [GNU GPL-3.0-or-later](LICENSE) licences.
