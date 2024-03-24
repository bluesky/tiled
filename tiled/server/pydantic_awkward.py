import pydantic


class AwkwardStructure(pydantic.BaseModel):
    length: int
    form: dict

    class Config:
        extra = "forbid"

    @classmethod
    def from_json(cls, structure):
        return cls(**structure)
