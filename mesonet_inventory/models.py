from __future__ import annotations

from pydantic import BaseModel
from typing_extensions import Annotated
from typing import Any



class Measurement(BaseModel):
    measurement: str
    us_unit: str
    si_unit: str
    other: dict[str, Any]


class Element(BaseModel):
    element: str
    measurement: Measurement
    description: str
    step_size: float
    persistence_delta: float
    spatial_sd: float
    flag_min: float
    shared_element: list[str]
    like_element: list[str]

