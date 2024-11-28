import cartopy.crs as ccrs
import matplotlib
import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import pandas as pd
import pypsa

from ispypsa.config.validators import ModelConfig
from ispypsa.results.plot_helpers import (
    _DEFAULT_CARRIER_COLOUR_MAPPING,
    _DEFAULT_FACECOLOR,
    _DEFAULT_GEOMAP_COLOURS,
    _add_figure_fuel_type_legend,
    _consolidate_plot_kwargs,
    _determine_title_year_range,
)


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
    # Bus pie size determined by total generation at the bus
    total_gen = _sum_generation_by_bus_and_carrier(network)
    ## size of ~1.0 appears to work well from trial-and-error
    if bus_size_scaling_factor is None:
        bus_size_scaling_factor = min(1.0 / total_gen.groupby("bus").sum())
    ## size of ~300.0 appears to work well from trial-and-error
    if flow_arrow_size_scaling_factor is None:
        flow_arrow_size_scaling_factor = min(300.0 / network.lines_t.p0.mean().abs())
    (main_title, sub_title) = (_create_main_title(config), _create_sub_title(config))
    fig, ax = plt.subplots(
        1,
        1,
        subplot_kw=_consolidate_plot_kwargs(
            dict(projection=ccrs.PlateCarree()), subplot_kwargs
        ),
        **_consolidate_plot_kwargs(
            dict(figsize=figure_size_inches, facecolor=_DEFAULT_FACECOLOR),
            figure_kwargs,
        ),
    )
    pyspsa_plot_kwgs = _consolidate_plot_kwargs(
        dict(
            geomap=True,
            color_geomap=_DEFAULT_GEOMAP_COLOURS,
            ax=ax,
            bus_colors=_DEFAULT_CARRIER_COLOUR_MAPPING,
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
            title=sub_title,
        ),
        pypsa_plot_kwargs,
    )
    collection = network.plot(**pyspsa_plot_kwgs)
    plt.colorbar(collection[2], fraction=0.04, pad=0.04, label="Mean flow (MW)")
    _add_figure_fuel_type_legend(fig, _DEFAULT_CARRIER_COLOUR_MAPPING)
    fig.suptitle(main_title, fontsize=18, x=0.6)
    return fig, ax


def _sum_generation_by_bus_and_carrier(network: pypsa.Network) -> pd.Series:
    generators_with_total_generation = network.generators.assign(
        total_generation=network.generators_t.p.sum()
    )
    return generators_with_total_generation.groupby(
        ["bus", "carrier"]
    ).total_generation.sum()


def _create_main_title(config: ModelConfig) -> str:
    year_range = _determine_title_year_range(config)
    title = f"Total energy generation by fuel type, {year_range}"
    return title


def _create_sub_title(config: ModelConfig) -> str:
    run_name = config.ispypsa_run_name
    title = f"ISPyPSA run: '{run_name}'"
    return title
