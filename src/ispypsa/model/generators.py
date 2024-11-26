from pathlib import Path

import pandas as pd
import pypsa


def _get_trace_data(
    generator_name: str, path_to_solar_traces: Path, path_to_wind_traces: Path
):
    """Fetches trace data for a generator from directories contain solar and wind traces.

    If a trace for the generator cannot be found, the function returns None.

    Args:
        generator_name: String defining the generator's name
        path_to_solar_traces: `pathlib.Path` for directory containing solar traces
        path_to_demand_traces: `pathlib.Path` for directory containing solar traces

    Returns:
        DataFrame with demand trace data or None value.
    """
    filename = Path(f"{generator_name}.parquet")
    solar_trace_filepath = path_to_solar_traces / filename
    wind_trace_filepath = path_to_wind_traces / filename
    if solar_trace_filepath.exists():
        trace_data = pd.read_parquet(solar_trace_filepath)
    elif wind_trace_filepath.exists():
        trace_data = pd.read_parquet(wind_trace_filepath)
    else:
        trace_data = None
    return trace_data


def _add_ecaa_generator_to_network(
    generator_definition: dict,
    network: pypsa.Network,
    path_to_solar_traces: Path,
    path_to_wind_traces: Path,
) -> None:
    """Adds a generator to a pypsa.Network based on a dict containing PyPSA Generator
    attributes.

    If trace data for the generator is available, then a dynamic maximum availability
    for the generator is applied (via `p_max_pu`). Otherwise, the nominal capacity of the
    generator is used to apply a static maximum availability.

    """
    generator_definition["class_name"] = "Generator"

    trace_data = _get_trace_data(
        generator_definition["name"], path_to_solar_traces, path_to_wind_traces
    )

    if trace_data is not None:
        trace_data["Datetime"] = trace_data["Datetime"].astype("datetime64[ns]")
        generator_definition["p_max_pu"] = trace_data.set_index("Datetime")["Value"]

    network.add(**generator_definition)


def add_ecaa_generators_to_network(
    network: pypsa.Network, path_pypsa_inputs: Path
) -> None:
    """Adds the generators in `ecaa_generators.csv` table (located in the
    `path_pypsa_inputs` directory) to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        path_pypsa_inputs: `pathlib.Path` that points to the directory containing
            PyPSA inputs

    Returns: None
    """
    ecaa_generators = pd.read_csv(path_pypsa_inputs / Path("generators.csv"))
    path_to_solar_traces = path_pypsa_inputs / Path("solar_traces")
    path_to_wind_traces = path_pypsa_inputs / Path("wind_traces")
    ecaa_generators.apply(
        lambda row: _add_ecaa_generator_to_network(
            row.to_dict(), network, path_to_solar_traces, path_to_wind_traces
        ),
        axis=1,
    )
