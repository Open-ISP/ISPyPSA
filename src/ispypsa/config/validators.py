from typing import Literal

from pydantic import BaseModel, field_validator

from ..templater.lists import _ISP_SCENARIOS


class NodesConfig(BaseModel):
    regional_granularity: Literal["sub_regions", "nem_regions", "single_region"]
    rezs: Literal["discrete_nodes", "attached_to_parent_node"]


class NetworkConfig(BaseModel):
    nodes: NodesConfig


class TraceConfig(BaseModel):
    year_type: Literal["fy", "calendar"]
    start_year: int
    end_year: int
    reference_year_cycle: list[int]

    @field_validator("end_year")
    @classmethod
    def validate_end_year(cls, end_year: float, info):
        if end_year < info.data.get("start_year"):
            raise ValueError(
                "config end_year must be greater than or equal to start_year"
            )
        return end_year


class ModelConfig(BaseModel):
    scenario: Literal[tuple(_ISP_SCENARIOS)]
    temporal_resolution: Literal["30min"]
    network: NetworkConfig
    traces: TraceConfig
    solver: Literal[
        "highs",
        "cbc",
        "glpk",
        "scip",
        "cplex",
        "gurobi",
        "xpress",
        "mosek",
        "copt",
        "mindopt",
        "pips",
    ]
