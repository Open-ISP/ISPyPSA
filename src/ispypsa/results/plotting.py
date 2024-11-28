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
    total_gen_by_bus_and_carrier = _sum_generation_by_bus_and_carrier(network)
    max_bus_gen = max(total_gen_by_bus_and_carrier.groupby("bus").sum())
    ## size of ~1.0 appears to work well from trial-and-error
    if bus_size_scaling_factor is None:
        bus_size_scaling_factor = 1.0 / max_bus_gen
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
            bus_sizes=total_gen_by_bus_and_carrier * bus_size_scaling_factor,
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
    _add_bus_name_labels(ax, network, bus_size_scaling_factor)
    plt.colorbar(collection[2], fraction=0.04, pad=0.04, label="Mean flow (MW)")
    _add_figure_fuel_type_legend(fig, _DEFAULT_CARRIER_COLOUR_MAPPING)
    _add_generation_circle_reference_legend(ax, max_bus_gen)
    fig.suptitle(main_title, fontsize=16, x=0.6)
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
    title = f"Total energy generation by fuel type\nand mean line flows, {year_range}"
    return title


def _create_sub_title(config: ModelConfig) -> str:
    run_name = config.ispypsa_run_name
    title = f"ISPyPSA run: '{run_name}'"
    return title


def _add_generation_circle_reference_legend(
    ax: matplotlib.axes.Axes,
    max_bus_gen: float,
    patch_kwargs=dict(),
    legend_kwargs=dict(),
) -> None:
    pypsa.plot.add_legend_circles(
        ax,
        [1 / max_bus_gen * factor for factor in (1e6, 1e7, 1e8)],
        ["1 TWh", "10 TWh", "100 TWh"],
        patch_kw=_consolidate_plot_kwargs(
            dict(edgecolor="black", facecolor=("black", 0.0)), patch_kwargs
        ),
        legend_kw=_consolidate_plot_kwargs(
            dict(labelspacing=1.5, frameon=False, title="Total generation"),
            legend_kwargs,
        ),
    )


def _add_bus_name_labels(
    ax: matplotlib.axes.Axes,
    network: pypsa.Network,
    bus_size_scaling_factor: float,
    x_offset: float = 0.15,
    y_offset: float = -0.2,
    label_kwargs: dict = dict(),
) -> None:
    plotted_buses = network.buses[network.buses.x != 0.0]
    label_offsets = _calculate_label_offsets(
        network, bus_size_scaling_factor, x_offset, y_offset
    )
    plotted_buses = plotted_buses.merge(
        label_offsets, how="left", left_index=True, right_index=True
    )
    bus_name_xy = plotted_buses[["x", "y", "x_offset", "y_offset"]].T.to_dict()
    for bus, attrs in bus_name_xy.items():
        ax.text(
            attrs["x"] + attrs["x_offset"],
            attrs["y"] + attrs["y_offset"],
            bus,
            **_consolidate_plot_kwargs(dict(horizontalalignment="left"), label_kwargs),
        )


def _calculate_label_offsets(
    network: pypsa.Network,
    bus_size_scaling_factor: float,
    x_offset: float,
    y_offset: float,
) -> pd.DataFrame:
    total_gen_by_bus = _sum_generation_by_bus_and_carrier(network).groupby("bus").sum()
    rough_bus_radii = (total_gen_by_bus * bus_size_scaling_factor).pow(0.5)
    offsets = pd.DataFrame(index=rough_bus_radii.index)
    if x_offset > 0:
        # offset from right end of pie
        offsets["x_offset"] = rough_bus_radii + x_offset
    else:
        # offset from left end of pie
        offsets["x_offset"] = -1 * rough_bus_radii - x_offset
    offsets["y_offset"] = y_offset
    return offsets
