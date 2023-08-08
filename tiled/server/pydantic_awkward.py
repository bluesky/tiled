import pydantic


class AwkwardStructure(pydantic.BaseModel):
    length: int
    form: dict
    suffixed_form_keys: list[str]

    @classmethod
    def from_json(cls, structure):
        return cls(**structure)
