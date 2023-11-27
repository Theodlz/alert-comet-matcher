from pydantic import BaseModel, validator, root_validator
from typing import Union


class KowalskiCredentials(BaseModel):
    protocol: str
    host: str
    port: int
    token: str

    @validator("protocol")
    def protocol_one_of(cls, v):
        if v not in ("http", "https"):
            raise ValueError("protocol must be either http or https")
        return v

    @validator("host")
    def host_is_not_empty(cls, v):
        if not v:
            raise ValueError("host must not be empty")
        return v

    @validator("port")
    def port_is_positive_integer(cls, v):
        if not isinstance(v, int) or v <= 0:
            raise ValueError("port must be a positive integer")
        return v

    @validator("token")
    def token_is_not_empty(cls, v):
        if not isinstance(v, str) or not v:
            raise ValueError("token must not be empty")
        return v

    # method that returns a dict with protocol, host, port, and token
    def to_dict(self):
        return {
            "protocol": self.protocol,
            "host": self.host,
            "port": self.port,
            "token": self.token,
        }


class ObjectWithPosition(BaseModel):
    name: str
    ra: float
    dec: float

    @validator("name")
    def name_is_not_empty(cls, v):
        if not isinstance(v, str) or not v:
            raise ValueError("name must not be empty")
        return v

    @validator("ra")
    def ra_is_float(cls, v):
        if not isinstance(v, float):
            raise ValueError("ra must be a float")
        return v

    @validator("dec")
    def dec_is_float(cls, v):
        if not isinstance(v, float):
            raise ValueError("dec must be a float")
        return v

    # method that returns a dict with name, ra, and dec
    def to_dict(self):
        return {"name": self.name, "ra": self.ra, "dec": self.dec}

    def __hash__(self):
        return hash((self.name, self.ra, self.dec))


class ObjectsWithPosition(BaseModel):
    objects_with_position: Union[list[ObjectWithPosition], list[dict]]

    @validator("objects_with_position")
    def objects_with_position_is_not_empty(cls, v):
        if not isinstance(v, list) or not v:
            raise ValueError("objects_with_position must not be empty")
        return v

    @root_validator()
    def objects_with_position_are_objects_with_position(cls, v):
        # for the elements that are dicts, convert them to ObjectWithPosition
        for i, obj in enumerate(v["objects_with_position"]):
            if isinstance(obj, dict):
                try:
                    v["objects_with_position"][i] = ObjectWithPosition(**obj)
                except Exception as e:
                    raise ValueError(
                        f"object_with_position at index {i} is not a valid ObjectWithPosition: {str(e)}"
                    )
        return v

    @root_validator()
    def objects_with_position_are_unique(cls, v):
        if len(v["objects_with_position"]) != len(set(v["objects_with_position"])):
            raise ValueError("objects_with_position must be unique")
        names = [obj.name for obj in v["objects_with_position"]]
        if len(names) != len(set(names)):
            raise ValueError("objects_with_position must have unique names")
        return v

    # method that returns a dict with name, ra, and dec
    def to_dict(self, coordinates_as_list=False):
        if coordinates_as_list:
            return {obj.name: [obj.ra, obj.dec] for obj in self.objects_with_position}
        return {
            obj.name: {"ra": obj.ra, "dec": obj.dec}
            for obj in self.objects_with_position
        }


class MovingObject(BaseModel):
    name: str
    ra: list[float]
    dec: list[float]
    jd: list[float]

    @validator("name")
    def name_is_not_empty(cls, v):
        if not isinstance(v, str) or not v:
            raise ValueError("name must not be empty")
        return v

    @validator("ra")
    def ra_is_not_empty(cls, v):
        if not isinstance(v, list) or not v:
            raise ValueError("ra must not be empty")
        return v

    @validator("dec")
    def dec_is_not_empty(cls, v):
        if not isinstance(v, list) or not v:
            raise ValueError("dec must not be empty")
        return v

    @validator("jd")
    def jd_is_not_empty(cls, v):
        if not isinstance(v, list) or not v:
            raise ValueError("jd must not be empty")
        return v

    @validator("ra")
    def ra_is_float(cls, v):
        if not all(isinstance(x, float) for x in v):
            raise ValueError("ra must be a list of floats")
        return v

    @validator("dec")
    def dec_is_float(cls, v):
        if not all(isinstance(x, float) for x in v):
            raise ValueError("dec must be a list of floats")
        return v

    @validator("jd")
    def jd_is_float(cls, v):
        if not all(isinstance(x, float) for x in v):
            raise ValueError("jd must be a list of floats")
        return v

    # validate that ra, dec, and jd have the same length
    @root_validator()
    def ra_dec_jd_same_length(cls, v):
        print(v)
        if not len(v["ra"]) == len(v["dec"]) == len(v["jd"]):
            raise ValueError("ra, dec, and jd must have the same length")
        return v

    # method that returns a dict with name, ra, dec, and jd
    def to_dict(self):
        return {"name": self.name, "ra": self.ra, "dec": self.dec, "jd": self.jd}
