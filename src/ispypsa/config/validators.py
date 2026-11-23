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
                f"The parsed traces directory specified in the config ({parsed_traces_directory})"
                + " does not exist"
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
    transmission_default_limit: float


class TsamSegmentsConfig(BaseModel):
    n_segments_per_year: int | None = None
    n_segments_per_week: int | None = None
    n_segments_per_day: int | None = None

    @model_validator(mode="after")
    def validate_exactly_one_option_is_set(self):
        options = [
            self.n_segments_per_year,
            self.n_segments_per_week,
            self.n_segments_per_day,
        ]
        set_options = [option for option in options if option is not None]
        if len(set_options) != 1:
            raise ValueError(
                "config tsam segments must set exactly one of n_segments_per_year, "
                "n_segments_per_week and n_segments_per_day"
            )
        if set_options[0] < 1:
            raise ValueError("config tsam segments must be at least 1")
        return self


class TsamConfig(BaseModel):
    features: Literal["demand", "all-traces"]
    segments: TsamSegmentsConfig


class TemporalAggregationConfig(BaseModel):
    representative_weeks: list[int] | None
    named_representative_weeks: (
        list[
            Literal[
                "peak-demand",
                "residual-peak-demand",
                "minimum-demand",
                "residual-minimum-demand",
                "peak-consumption",
                "residual-peak-consumption",
            ]
        ]
        | None
    ) = None
    tsam: TsamConfig | None = None

    @model_validator(mode="after")
    def validate_yearly_segments_are_not_combined_with_weeks(self):
        # Representative weeks leave gaps in the year, and a segment that spanned
        # a gap would average half-hours that aren't adjacent in time. Weekly and
        # daily segments can't span a gap, so they can be combined with weeks.
        uses_weeks = (
            self.representative_weeks is not None
            or self.named_representative_weeks is not None
        )
        if (
            self.tsam is not None
            and self.tsam.segments.n_segments_per_year is not None
            and uses_weeks
        ):
            raise ValueError(
                "config tsam n_segments_per_year cannot be combined with "
                "representative_weeks or named_representative_weeks, use "
                "n_segments_per_week or n_segments_per_day instead"
            )
        return self


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

    @model_validator(mode="after")
    def validate_tsam_is_not_used(self):
        # The rolling horizon counts its windows in snapshots, which assumes
        # every snapshot is the same length. Segments are not.
        if self.aggregation.tsam is not None:
            raise ValueError(
                "config tsam aggregation is only available for capacity_expansion"
            )
        return self


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


class TraceDataConfig(BaseModel):
    dataset_type: Literal["full", "example"] = "example"
    dataset_year: int = 2024


class ModelConfig(BaseModel):
    paths: PathsConfig
    scenario: Literal[tuple(_ISP_SCENARIOS)]
    wacc: float
    discount_rate: float
    network: NetworkConfig
    temporal: TemporalConfig
    iasr_workbook_version: str
    unserved_energy: UnservedEnergyConfig
    trace_data: TraceDataConfig = TraceDataConfig()
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
    create_plots: bool = False

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
