from pathlib import Path
import pypsa


def save_results(network: pypsa.Network, pypsa_outputs_location: Path) -> None:
    network.generators_t.p.to_parquet(
        Path(pypsa_outputs_location, "generator_dispatch.parquet")
    )
    network.lines_t.p0.to_parquet(Path(pypsa_outputs_location, "line_flows_p0.parquet"))
    network.lines_t.p0.to_parquet(Path(pypsa_outputs_location, "line_flows_p1.parquet"))
