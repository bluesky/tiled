import pydantic


class AwkwardStructure(pydantic.BaseModel):
    length: int
    form: dict

    model_config = pydantic.ConfigDict(extra="forbid")

    @classmethod
    def from_json(cls, structure):
        return cls(**structure)
