from typing import Literal

from ..templater.lists import _ISP_SCENARIOS
from pydantic import BaseModel


class NetworkConfig(BaseModel):
    granularity: Literal["sub_regional", "regional", "single_region"]


class ModelConfig(BaseModel):
    scenario: Literal[tuple(_ISP_SCENARIOS)]
    network: NetworkConfig
