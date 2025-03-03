import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, field_validator

from ..templater.lists import _ISP_SCENARIOS


class NodesConfig(BaseModel):
    regional_granularity: Literal["sub_regions", "nem_regions", "single_region"]
    rezs: Literal["discrete_nodes", "attached_to_parent_node"]


class NetworkConfig(BaseModel):
    nodes: NodesConfig
    annuitisation_lifetime: int
    transmission_expansion: bool
    rez_transmission_expansion: bool
    rez_to_sub_region_transmission_default_limit: float


class TemporalAggregationConfig(BaseModel):
    representative_weeks: list[int] | None


class TemporalConfig(BaseModel):
    operational_temporal_resolution_min: int
    path_to_parsed_traces: str
    year_type: Literal["fy", "calendar"]
    start_year: int
    end_year: int
    reference_year_cycle: list[int]
    aggregation: TemporalAggregationConfig

    @field_validator("operational_temporal_resolution_min")
    @classmethod
    def validate_temporal_resolution_min(cls, operational_temporal_resolution_min: int):
        # TODO properly implement temporal aggregation so this first check can be removed.
        if operational_temporal_resolution_min != 30:
            raise ValueError(
                "config operational_temporal_resolution_min must equal 30 min"
            )
        if operational_temporal_resolution_min < 30:
            raise ValueError(
                "config operational_temporal_resolution_min must be greater than or equal to 30 min"
            )
        if (operational_temporal_resolution_min % 30) != 0:
            raise ValueError(
                "config operational_temporal_resolution_min must be multiple of 30 min"
            )
        return operational_temporal_resolution_min

    @field_validator("path_to_parsed_traces")
    @classmethod
    def validate_path_to_parsed_traces(cls, path_to_parsed_traces: str):
        if path_to_parsed_traces == "NOT_SET_FOR_TESTING":
            return path_to_parsed_traces

        if path_to_parsed_traces == "ENV":
            path_to_parsed_traces = os.environ.get("PATH_TO_PARSED_TRACES")
            if path_to_parsed_traces is None:
                raise ValueError("Environment variable PATH_TO_PARSED_TRACES not set")

        trace_path = Path(path_to_parsed_traces)
        if not trace_path.exists():
            raise NotADirectoryError(
                f"The parsed traces directory specified in the config ({trace_path})"
                + " does not exist"
            )
        # check this folder contains sub-folders named solar, wind and demand
        child_folders = set([folder.parts[-1] for folder in trace_path.iterdir()])
        if child_folders != set(("demand", "wind", "solar")):
            raise ValueError(
                "The parsed traces directory must contain the following sub-folders"
                + " with parsed trace data: 'demand', 'solar', 'wind'"
            )
        return path_to_parsed_traces

    @field_validator("end_year")
    @classmethod
    def validate_end_year(cls, end_year: float, info):
        if end_year < info.data.get("start_year"):
            raise ValueError(
                "config end_year must be greater than or equal to start_year"
            )
        return end_year


class ModelConfig(BaseModel):
    ispypsa_run_name: str
    scenario: Literal[tuple(_ISP_SCENARIOS)]
    wacc: float
    network: NetworkConfig
    temporal: TemporalConfig
    iasr_workbook_version: str
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
