from datetime import datetime
from uuid import UUID, uuid4
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_serializer, model_validator

from .enums import (
    AidType,
    EPFDType,
    ManeuverIndicator,
    NavigationStatus,
    PositionAccuracy,
    ShipType,
)

# ============== SUB-MODELS ==============


class Position(BaseModel):
    """Position and motion data (msg 1,2,3,18,19)"""

    lat: float = Field(ge=-90.0, le=90.0, description="Latitude in decimal degrees")
    lon: float = Field(ge=-180.0, le=180.0, description="Longitude in decimal degrees")
    speed: float = Field(ge=0.0, le=102.3, description="Speed over ground in knots")
    course: float = Field(ge=0.0, le=360.0, description="Course over ground in degrees")
    heading: int = Field(ge=0, le=511, description="True heading in degrees (511 = N/A)")
    accuracy: PositionAccuracy | None = Field(None, description="Position accuracy (0=low >10m, 1=high ≤10m)")
    timestamp: int | None = Field(None, ge=0, le=63, description="UTC second when report was generated")
    epfd: EPFDType | None = Field(None, description="Type of position fixing device")


class ClassA(BaseModel):
    """Class A dynamic data (msg 1,2,3)"""

    status: NavigationStatus = Field(description="Navigational status")
    rot: float = Field(ge=-128.0, le=127.0, description="Rate of turn in degrees/min")
    maneuver: ManeuverIndicator | None = Field(None, description="Special maneuver indicator")
    raim: bool | None = Field(None, description="RAIM flag")


class ClassB(BaseModel):
    """Class B specific flags (msg 18)"""

    cs: bool | None = Field(None, description="Class B CS unit flag")
    display: bool | None = Field(None, description="Has display capability")
    dsc: bool | None = Field(None, description="DSC equipped")
    band: bool | None = Field(None, description="Can operate over entire marine band")
    msg22: bool | None = Field(None, description="Can accept channel assignment via msg 22")
    assigned: bool | None = Field(None, description="Assigned mode flag")


class Static(BaseModel):
    """Static vessel data (msg 5,19,24)"""

    shipname: str = Field(max_length=20, description="Vessel name")
    shiptype: ShipType = Field(description="Type of ship and cargo")
    callsign: str | None = Field(None, max_length=7, description="International radio call sign")
    imo: int | None = Field(None, ge=1000000, le=9999999, description="IMO ship identification number")
    a: int | None = Field(None, ge=0, le=511, description="Distance from bow to reference point (m)")
    b: int | None = Field(None, ge=0, le=511, description="Distance from stern to reference point (m)")
    c: int | None = Field(None, ge=0, le=63, description="Distance from port to reference point (m)")
    d: int | None = Field(None, ge=0, le=63, description="Distance from starboard to reference point (m)")


class Voyage(BaseModel):
    """Voyage data (msg 5)"""

    destination: str = Field(max_length=20, description="Destination port")
    draught: float = Field(ge=0.0, le=25.5, description="Maximum present static draught in meters")
    eta: str | None = Field(None, description="Estimated time of arrival (MM-DD HH:MM)")


class AidToNavigation(BaseModel):
    """Aid to Navigation data (msg 21)"""

    aid_type: AidType = Field(description="Type of aid to navigation")
    name: str = Field(max_length=34, description="Name of aid to navigation")
    off_position: bool = Field(description="Off position indicator")
    virtual_aid: bool = Field(description="Virtual aid flag")


class BaseStation(BaseModel):
    """Base station report data (msg 4)"""

    year: int = Field(ge=0, le=9999, description="UTC year")
    month: int = Field(ge=0, le=12, description="UTC month (0 = N/A)")
    day: int = Field(ge=0, le=31, description="UTC day (0 = N/A)")
    hour: int = Field(ge=0, le=24, description="UTC hour (24 = N/A)")
    minute: int = Field(ge=0, le=60, description="UTC minute (60 = N/A)")
    second: int = Field(ge=0, le=60, description="UTC second (60 = N/A)")


# ============== MAIN MODEL ==============


class AisMessage(BaseModel, extra="ignore"):
    """
    Aggregated AIS message with optional sub-objects.
    Accepts flat dict input; sub-objects auto-populated if valid.
    """

    msg_uuid: UUID = Field(..., default_factory=uuid4, description="UUID of the message.")
    msg_time: datetime = Field(
        ..., default_factory=datetime.now, description="Timestamp of the message creation.", init=False
    )
    mmsi: int = Field(
        ge=100000000,
        le=999999999,
        description="Maritime Mobile Service Identity (9 digits)",
    )
    extra_fields: dict[str, Any] = Field(
        ..., default_factory=dict, description="Métadonnées pour un usage délégué aux autres modules"
    )

    position: Position | None = Field(None, description="Position and motion data")
    class_a: ClassA | None = Field(None, description="Class A specific dynamic data")
    class_b: ClassB | None = Field(None, description="Class B specific flags")
    static: Static | None = Field(None, description="Static vessel identification data")
    voyage: Voyage | None = Field(None, description="Voyage related data")
    aton: AidToNavigation | None = Field(None, description="Aid to Navigation data")
    base_station: BaseStation | None = Field(None, description="Base station report data")

    @field_serializer("msg_uuid")
    def _serialize_msg_uuid(self, v: UUID) -> str:
        return str(v)

    @model_validator(mode="before")
    @classmethod
    def build_from_flat(cls, data: dict) -> dict:
        """Attempt to build each sub-model from flat input."""
        extensions = {
            "position": Position,
            "class_a": ClassA,
            "class_b": ClassB,
            "static": Static,
            "voyage": Voyage,
            "aton": AidToNavigation,
            "base_station": BaseStation,
        }
        for key, model in extensions.items():
            if data.get(key) is None:
                try:
                    candidate = model.model_validate(data)
                    if any(v is not None for v in candidate.model_dump().values()):
                        data[key] = candidate
                except ValidationError:
                    pass
        return data

    # def model_dump(self, *args, **kwargs):
    #     """Dump a flat dict by default.

    #     By default, nested sub-models are flattened into the top-level output.
    #     To keep nested objects, pass `flat=False`.
    #     """

    #     flat = kwargs.pop("flat", True)
    #     dumped = super().model_dump(*args, **kwargs)
    #     if not flat:
    #         return dumped

    #     for key in (
    #         "position",
    #         "class_a",
    #         "class_b",
    #         "static",
    #         "voyage",
    #         "aton",
    #         "base_station",
    #     ):
    #         sub = dumped.pop(key, None)
    #         if isinstance(sub, dict):
    #             dumped.update(sub)
    #     return dumped


# ============== ENRICHED MODEL ==============

class EnrichedPosition(BaseModel):
    delta_speed: float
    delta_lat: float
    delta_lon: float
    delta_time: float | None
    delta_course: float
    acceleration: float | None
    distance_haversine: float
    theoretical_distance: float | None
    speed_correlation: float | None

class EnrichedMessage(BaseModel):
    msg_uuid: UUID = Field(..., default_factory=uuid4, description="UUID of the message.")
    msg_time: datetime = Field(
        ..., default_factory=datetime.now, description="Timestamp of the message creation.", init=False
    )
    mmsi: int = Field(
        ge=100000000,
        le=999999999,
        description="Maritime Mobile Service Identity (9 digits)",
    )

    enr_position: EnrichedPosition
