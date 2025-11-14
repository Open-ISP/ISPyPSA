import pandas as pd
import pygal


def plot_aggregate_transmission_capacity(results: dict[str, pd.DataFrame]) -> pygal.Bar:
    transmission_expansion = results["transmission_expansion"]

    transmission_expansion = transmission_expansion.sort_values("build_year")

    transmission_expansion = transmission_expansion[
        transmission_expansion["isp_type"] != "rez_no_limit"
    ]

    # Calculate cumulative sum of forward_direction_nominal_capacity_mw
    transmission_expansion = (
        transmission_expansion.groupby("build_year")[
            "forward_direction_nominal_capacity_mw"
        ]
        .sum()
        .cumsum()
        .reset_index()
    )

    # Convert to GW
    transmission_expansion["forward_direction_nominal_capacity_mw"] = (
        transmission_expansion["forward_direction_nominal_capacity_mw"] / 1000
    )

    chart = pygal.Bar()
    chart.title = "Aggregate Transmission Capacity"

    chart.y_title = "Forward Direction Nominal Capacity (GW)"

    # Turn legend off.
    chart.show_legend = False

    chart.x_labels = map(str, transmission_expansion["build_year"].unique())

    chart.add(
        "Forward Direction Nominal Capacity (GW)",
        transmission_expansion["forward_direction_nominal_capacity_mw"],
    )

    return chart
