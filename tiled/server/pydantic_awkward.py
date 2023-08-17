import pydantic


class AwkwardStructure(pydantic.BaseModel):
    length: int
    form: dict

    @classmethod
    def from_json(cls, structure):
        return cls(**structure)
