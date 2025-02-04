import pypsa


def run(network: pypsa.Network, solver_name="highs", solver_options={}) -> None:
    """Runs the model by calling `optimize()` on the `pypsa.Network`

    Args:
        network: The `pypsa.Network` object
        solver_name: PyPSA/linopy-compatible solver. See
            https://pypsa.readthedocs.io/en/latest/getting-started/installation.html
        solver_options: Options to pass to the solver.
    """
    network.optimize.solve_model(solver_name=solver_name, solver_options=solver_options)
