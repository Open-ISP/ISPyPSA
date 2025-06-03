from pathlib import Path

import pandas as pd
import pypsa


def _get_trace_data(generator_name: str, path_to_traces: Path):
    """Fetches trace data for a generator from directories containing traces.

    Args:
        generator_name: String defining the generator's name
        path_to_traces: `pathlib.Path` for directory containing traces

    Returns:
        DataFrame with resource trace data.
    """
    filename = Path(f"{generator_name}.parquet")
    trace_filepath = path_to_traces / filename
    trace_data = pd.read_parquet(trace_filepath)
    return trace_data


def _add_generator_to_network(
    generator_definition: dict,
    network: pypsa.Network,
    path_to_solar_traces: Path,
    path_to_wind_traces: Path,
) -> None:
    """Adds a generator to a pypsa.Network based on a dict containing PyPSA Generator
    attributes.

    If the carrier of a generator is Wind or Solar then a dynamic maximum availability
    for the generator is applied (via `p_max_pu`). Otherwise, the nominal capacity of the
    generator is used to apply a static maximum availability.

    Args:
        generator_definition: dict containing pypsa Generator parameters
        network: The `pypsa.Network` object
        path_to_solar_traces: `pathlib.Path` for directory containing solar traces
        path_to_wind_traces: `pathlib.Path` for directory containing wind traces

    Returns: None
    """
    generator_definition["class_name"] = "Generator"

    if generator_definition["carrier"] == "Wind":
        trace_data = _get_trace_data(generator_definition["name"], path_to_wind_traces)
    elif generator_definition["carrier"] == "Solar":
        trace_data = _get_trace_data(generator_definition["name"], path_to_solar_traces)
    else:
        trace_data = None

    if trace_data is not None:
        trace_data = trace_data.set_index(["investment_periods", "snapshots"])
        generator_definition["p_max_pu"] = trace_data["p_max_pu"]

    network.add(**generator_definition)


def _add_generators_to_network(
    network: pypsa.Network,
    generators: pd.DataFrame,
    path_to_timeseries_data: Path,
) -> None:
    """Adds the generators in a pypsa-friendly `pd.DataFrame` to the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        generators:  `pd.DataFrame` with `PyPSA` style `Generator` attributes.
        path_to_timeseries_data: `pathlib.Path` that points to the directory containing
            timeseries data
    Returns: None
    """
    path_to_solar_traces = path_to_timeseries_data / Path("solar_traces")
    path_to_wind_traces = path_to_timeseries_data / Path("wind_traces")
    generators.apply(
        lambda row: _add_generator_to_network(
            row.to_dict(), network, path_to_solar_traces, path_to_wind_traces
        ),
        axis=1,
    )


def _add_custom_constraint_generators_to_network(
    network: pypsa.Network, generators: pd.DataFrame
) -> None:
    """Adds the Generators defined in `custom_constraint_generators.csv` in the `path_pypsa_inputs` directory to the
    `pypsa.Network` object. These are generators that connect to a dummy bus, not part of the rest of the network,
    the generators are used to model custom constraint investment by referencing the p_nom of the generators in the
    custom constraints.

    Args:
        network: The `pypsa.Network` object
        generators:  `pd.DataFrame` with `PyPSA` style `Generator` attributes.

    Returns: None
    """
    generators["class_name"] = "Generator"
    generators.apply(lambda row: network.add(**row.to_dict()), axis=1)


def _update_generator_availability_timeseries(
    name: str,
    carrier: str,
    network: pypsa.Network,
    path_to_solar_traces: Path,
    path_to_wind_traces: Path,
) -> None:
    """Updates the timeseries availability of the generator in the `pypsa.Network`.

    The function is used to set up the model for operational modelling following
    capacity expansion optimisation. Once the model snapshots are updated then the
    generator time series also need to be updated to match.

    Args:
        name: str specifying the generators name
        carrier: the generator fuel type
        network: The `pypsa.Network` object
        path_to_solar_traces: `pathlib.Path` for directory containing solar traces
        path_to_wind_traces: `pathlib.Path` for directory containing wind traces

    Returns: None
    """

    if carrier == "Wind":
        trace_data = _get_trace_data(name, path_to_wind_traces)
    elif carrier == "Solar":
        trace_data = _get_trace_data(name, path_to_solar_traces)
    else:
        trace_data = None

    if trace_data is not None:
        trace_data = trace_data.set_index(["investment_periods", "snapshots"])
        network.generators_t.p_max_pu[name] = trace_data.loc[:, ["p_max_pu"]]


def _update_generators_availability_timeseries(
    network: pypsa.Network,
    generators: pd.DataFrame,
    path_to_timeseries_data: Path,
) -> None:
    """Updates the timeseries availability of the generators in the pypsa-friendly `
    pd.DataFrame` in the `pypsa.Network`.

    Args:
        network: The `pypsa.Network` object
        generators:  `pd.DataFrame` with `PyPSA` style `Generator` attributes.
        path_to_timeseries_data: `pathlib.Path` that points to the directory containing
            timeseries data
    Returns: None
    """
    path_to_solar_traces = path_to_timeseries_data / Path("solar_traces")
    path_to_wind_traces = path_to_timeseries_data / Path("wind_traces")
    generators.apply(
        lambda row: _update_generator_availability_timeseries(
            row["name"],
            row["carrier"],
            network,
            path_to_solar_traces,
            path_to_wind_traces,
        ),
        axis=1,
    )
