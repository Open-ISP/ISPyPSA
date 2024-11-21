import pypsa


def run(network: pypsa.Network):
    """Runs optimise for the pypsa.Network

    Args:
        network: The pypsa.Network object


    """
    network.optimize()
