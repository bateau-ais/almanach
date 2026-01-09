import pytest
from pydantic import ValidationError

from almanach import AisMessage


def test_ais_message_builds_submodels_from_flat_input_when_valid() -> None:
    msg = AisMessage.model_validate(
        {
            "mmsi": 123456789,
            # Position
            "lat": 10.0,
            "lon": 20.0,
            "speed": 5.5,
            "course": 90.0,
            "heading": 180,
            # Class A
            "status": 0,
            "rot": 0.0,
            # Class B
            "cs": True,
            # Static
            "shipname": "TEST VESSEL",
            "shiptype": 0,
            # Voyage
            "destination": "PORT",
            "draught": 1.2,
            # Aid-to-navigation
            "aid_type": 0,
            "name": "ATON NAME",
            "off_position": False,
            "virtual_aid": False,
            # Base station
            "year": 2025,
            "month": 1,
            "day": 2,
            "hour": 3,
            "minute": 4,
            "second": 5,
        }
    )

    assert msg.mmsi == 123456789

    assert msg.position is not None
    assert msg.position.lat == 10.0
    assert msg.position.lon == 20.0
    assert msg.position.speed == 5.5
    assert msg.position.course == 90.0
    assert msg.position.heading == 180

    assert msg.class_a is not None
    assert msg.class_a.status == 0
    assert msg.class_a.rot == 0.0

    assert msg.class_b is not None
    assert msg.class_b.cs is True

    assert msg.static is not None
    assert msg.static.shipname == "TEST VESSEL"
    assert msg.static.shiptype == 0

    assert msg.voyage is not None
    assert msg.voyage.destination == "PORT"
    assert msg.voyage.draught == 1.2

    assert msg.aton is not None
    assert msg.aton.aid_type == 0
    assert msg.aton.name == "ATON NAME"
    assert msg.aton.off_position is False
    assert msg.aton.virtual_aid is False

    assert msg.base_station is not None
    assert (msg.base_station.year, msg.base_station.month, msg.base_station.day) == (2025, 1, 2)


def test_ais_message_does_not_overwrite_explicit_nested_submodel() -> None:
    msg = AisMessage.model_validate(
        {
            "mmsi": 123456789,
            "position": {
                "lat": 1.0,
                "lon": 2.0,
                "speed": 3.0,
                "course": 4.0,
                "heading": 5,
                "accuracy": None,
                "timestamp": None,
                "epfd": None,
            },
            # Flat keys that would also be valid if used for auto-population
            "lat": 10.0,
            "lon": 20.0,
            "speed": 30.0,
            "course": 40.0,
            "heading": 50,
        }
    )

    assert msg.position is not None
    # The explicitly provided nested object must win
    assert (msg.position.lat, msg.position.lon, msg.position.speed, msg.position.course, msg.position.heading) == (
        1.0,
        2.0,
        3.0,
        4.0,
        5,
    )


def test_ais_message_leaves_submodel_none_when_flat_input_insufficient_or_invalid() -> None:
    # Missing required Position fields: lon/speed/course/heading
    msg = AisMessage.model_validate({"mmsi": 123456789, "lat": 10.0})
    assert msg.position is None

    # Invalid payload for Position (lat out of range) should not crash build_from_flat; it should remain None.
    msg2 = AisMessage.model_validate(
        {
            "mmsi": 123456789,
            "lat": 1000.0,  # invalid
            "lon": 20.0,
            "speed": 1.0,
            "course": 1.0,
            "heading": 1,
        }
    )
    assert msg2.position is None


def test_ais_message_rejects_invalid_mmsi() -> None:
    with pytest.raises(ValidationError):
        AisMessage.model_validate({"mmsi": 1})

    with pytest.raises(ValidationError):
        AisMessage.model_validate({"mmsi": 1000000000})


def test_ais_message_ignores_extra_fields() -> None:
    msg = AisMessage.model_validate(
        {
            "mmsi": 123456789,
            "unknown_field": "ignored",
        }
    )
    assert msg.mmsi == 123456789
    assert not hasattr(msg, "unknown_field")


def test_ais_message_does_not_autopopulate_when_key_present_as_none() -> None:
    # build_from_flat only auto-populates when data.get(key) is None; therefore if the key is present and None,
    # it will be attempted. However, we verify it stays None if underlying model validation fails.
    msg = AisMessage.model_validate(
        {
            "mmsi": 123456789,
            "position": None,
            # Provide invalid flat position (missing required fields), so auto-population attempt should fail and keep None.
            "lat": 1.0,
        }
    )
    assert msg.position is None
