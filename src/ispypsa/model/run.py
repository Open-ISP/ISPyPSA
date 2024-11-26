import pypsa


def run(network: pypsa.Network) -> None:
    """Runs the model by calling `optimize()` on the `pypsa.Network`

    Args:
        network: The `pypsa.Network` object
    """
    network.optimize()
