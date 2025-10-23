import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, field_validator, model_validator

from ..templater.lists import _ISP_SCENARIOS


class PathsConfig(BaseModel):
    ispypsa_run_name: str
    parsed_traces_directory: str
    parsed_workbook_cache: str
    workbook_path: str | None
    run_directory: str

    @field_validator("parsed_traces_directory")
    @classmethod
    def validate_parsed_traces_directory(cls, parsed_traces_directory: str):
        if parsed_traces_directory == "NOT_SET_FOR_TESTING":
            return parsed_traces_directory

        if parsed_traces_directory == "ENV":
            parsed_traces_directory = os.environ.get("PATH_TO_PARSED_TRACES")
            if parsed_traces_directory is None:
                raise ValueError("Environment variable PATH_TO_PARSED_TRACES not set")

        trace_path = Path(parsed_traces_directory)
        if not trace_path.exists():
            raise NotADirectoryError(
                f"The parsed traces directory specified in the config1 ({parsed_traces_directory})"
                + " does not exist"
            )
        # check this folder contains sub-folders named solar, wind and demand
        child_folders = set([folder.parts[-1] for folder in trace_path.iterdir()])
        if child_folders != set(("demand", "wind", "solar")):
            raise ValueError(
                "The parsed traces directory must contain the following sub-folders"
                + " with parsed trace data: 'demand', 'solar', 'wind'"
            )
        return parsed_traces_directory


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


class TemporalRangeConfig(BaseModel):
    start_year: int
    end_year: int

    @model_validator(mode="after")
    def validate_end_year(self):
        if self.end_year < self.start_year:
            raise ValueError(
                "config end_year must be greater than or equal to start_year"
            )
        return self


class TemporalDetailedConfig(BaseModel):
    reference_year_cycle: list[int]
    resolution_min: int
    aggregation: TemporalAggregationConfig

    @field_validator("resolution_min")
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


class TemporalOperationalConfig(TemporalDetailedConfig):
    horizon: int
    overlap: int


class TemporalCapacityInvestmentConfig(TemporalDetailedConfig):
    investment_periods: list[int]


class TemporalConfig(BaseModel):
    year_type: Literal["fy", "calendar"]
    range: TemporalRangeConfig
    capacity_expansion: TemporalCapacityInvestmentConfig
    operational: TemporalOperationalConfig = None

    @model_validator(mode="after")
    def validate_investment_periods(self):
        if min(self.capacity_expansion.investment_periods) != self.range.start_year:
            raise ValueError(
                "config first investment period must be equal to start_year"
            )
        if len(self.capacity_expansion.investment_periods) != len(
            set(self.capacity_expansion.investment_periods)
        ):
            raise ValueError("config all years in investment_periods must be unique")
        if (
            sorted(self.capacity_expansion.investment_periods)
            != self.capacity_expansion.investment_periods
        ):
            raise ValueError(
                "config investment_periods must be provided in sequential order"
            )
        return self


class UnservedEnergyConfig(BaseModel):
    cost: float = None
    max_per_node: float = 1e5  # Default to a very large value (100,000 MW)


class ModelConfig(BaseModel):
    paths: PathsConfig
    scenario: Literal[tuple(_ISP_SCENARIOS)]
    wacc: float
    discount_rate: float
    network: NetworkConfig
    temporal: TemporalConfig
    iasr_workbook_version: str
    unserved_energy: UnservedEnergyConfig
    filter_by_nem_regions: list[str] | None = None
    filter_by_isp_sub_regions: list[str] | None = None
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

    @model_validator(mode="after")
    def validate_region_filters(self):
        if (
            self.filter_by_nem_regions is not None
            and self.filter_by_isp_sub_regions is not None
        ):
            raise ValueError(
                "Cannot specify both filter_by_nem_regions and filter_by_isp_sub_regions"
            )
        return self

