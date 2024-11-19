from typing import Literal

from pydantic import BaseModel, field_validator

from ..templater.lists import _ISP_SCENARIOS


class NetworkConfig(BaseModel):
    granularity: Literal["sub_regional", "regional", "single_region"]


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
    network: NetworkConfig
    traces: TraceConfig
