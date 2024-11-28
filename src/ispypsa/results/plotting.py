import matplotlib
import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import pandas as pd
import pypsa
from matplotlib.patches import Patch
import cartopy.crs as ccrs

from ispypsa.config.validators import ModelConfig

_CARRIER_COLOUR_MAPPING = {
    # corresponds to distillate in OpenElectricity
    "Liquid Fuel": "#E46E56",
    "Black Coal": "#251C00",
    "Brown Coal": "#675B42",
    "Gas": "#E78114",
    "Water": "#ACE9FE",
    "Solar": "#FECE00",
    "Wind": "#2A7E3F",
    # corresponds to gas_hydrogen in OpenElectricity
    "Hyblend": "#C75338",
}
"""Colour mapping based on mapping from OpenElectricity"""


def plot_map_of_energy_generation_by_carrier(
    network: pypsa.Network,
    config: ModelConfig,
    figure_size_inches: tuple[float, float],
    bus_size_scaling_factor: float | None = None,
    flow_colormap: str = "inferno",
    flow_arrow_size_scaling_factor: float | None = None,
    min_max_latitudes: tuple[float, float] = (-44.0, -15.0),
    min_max_longitudes: tuple[float, float] = (137.5, 156.0),
    pypsa_plot_kwargs: dict = dict(),
    figure_kwargs: dict = dict(),
    subplot_kwargs: dict = dict(),
) -> tuple[matplotlib.figure.Figure, matplotlib.axes.Axes]:
    total_gen = _sum_generation_by_bus_and_carrier(network)
    # size of ~1.0 appears to work well from trial-and-error
    if bus_size_scaling_factor is None:
        bus_size_scaling_factor = min(1.0 / total_gen.groupby("bus").sum())
    # size of ~300.0 appears to work well from trial-and-error
    if flow_arrow_size_scaling_factor is None:
        flow_arrow_size_scaling_factor = min(300.0 / network.lines_t.p0.mean().abs())
    title = _create_plot_title(config)
    fig, ax = plt.subplots(
        1,
        1,
        subplot_kw=_consolidate_plot_kwargs(
            dict(projection=ccrs.PlateCarree()), subplot_kwargs
        ),
        **_consolidate_plot_kwargs(
            dict(
                figsize=figure_size_inches,
            ),
            figure_kwargs,
        ),
    )
    pyspsa_plot_kwgs = _consolidate_plot_kwargs(
        dict(
            geomap=True,
            color_geomap=True,
            ax=ax,
            bus_colors=_CARRIER_COLOUR_MAPPING,
            bus_sizes=total_gen * bus_size_scaling_factor,
            flow="mean",
            line_colors=network.lines_t.p0.mean().abs(),
            line_cmap=flow_colormap,
            line_widths=flow_arrow_size_scaling_factor,
            boundaries=[
                min_max_longitudes[0],
                min_max_longitudes[1],
                min_max_latitudes[0],
                min_max_latitudes[1],
            ],
            title=title,
        ),
        pypsa_plot_kwargs,
    )
    collection = network.plot(**pyspsa_plot_kwgs)
    plt.colorbar(collection[2], fraction=0.04, pad=0.04, label="Mean flow (MW)")
    _add_fuel_type_legend(ax, _CARRIER_COLOUR_MAPPING)
    return fig, ax


def _sum_generation_by_bus_and_carrier(network: pypsa.Network) -> pd.Series:
    generators_with_total_generation = network.generators.assign(
        total_generation=network.generators_t.p.sum()
    )
    return generators_with_total_generation.groupby(
        ["bus", "carrier"]
    ).total_generation.sum()


def _consolidate_plot_kwargs(
    predefined_kwargs: dict, user_specified_kwargs: dict
) -> dict:
    kwargs = predefined_kwargs
    kwargs.update(user_specified_kwargs)
    return kwargs


def _create_plot_title(config: ModelConfig) -> str:
    run_name = config.ispypsa_run_name
    (start, end) = (config.traces.start_year, config.traces.end_year)
    year_type = config.traces.year_type
    if year_type == "fy" and start != end:
        year_range = f"FY{str(start)[-2:]}-{str(end)[-2:]}"
    elif start == end:
        year_range = f"{start}"
    else:
        year_range = f"{start}-{end}"
    title = (
        f"ISPyPSA run '{run_name}'\nTotal energy generation by fuel type, {year_range}"
    )
    return title


def _add_fuel_type_legend(
    ax: matplotlib.axes.Axes, carrier_colour_mapping: dict[str, str]
) -> None:
    legend_patches = [
        Patch(color=color, label=carrier)
        for carrier, color in carrier_colour_mapping.items()
    ]
    ax.legend(handles=legend_patches, title="Fuel Type", loc="upper right")
