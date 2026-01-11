import msgpack
from pydantic import BaseModel


def to_msgpack(model: BaseModel) -> bytes:
    """
    Serialize a Pydantic model to MessagePack bytes.
    """
    # Convert the model to a dict first
    model_dict = model.model_dump()
    return msgpack.packb(model_dict, use_bin_type=True)


def from_msgpack(model_cls, packed: bytes) -> BaseModel:
    """
    Deserialize MessagePack bytes to a Pydantic model.
    """
    # Unpack to dict, then parse with the model class
    model_dict = msgpack.unpackb(packed, raw=False)
    return model_cls.model_validate(model_dict)
