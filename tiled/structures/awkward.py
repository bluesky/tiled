from dataclasses import dataclass
from typing import List

import awkward


@dataclass
class AwkwardStructure:
    length: int
    form: dict
    suffixed_form_keys: List[str]

    @classmethod
    def from_json(cls, structure):
        return cls(**structure)


def project_form(form, form_keys_touched):
    # See https://github.com/bluesky/tiled/issues/450
    if isinstance(form, awkward.forms.RecordForm):
        if form.fields is None:
            original_fields = [None] * len(form.contents)
        else:
            original_fields = form.fields

        fields = []
        contents = []
        for field, content in zip(original_fields, form.contents):
            projected = project_form(content, form_keys_touched)
            if projected is not None:
                fields.append(field)
                contents.append(content)

        if form.fields is None:
            fields = None

        return form.copy(fields=fields, contents=contents)

    elif isinstance(form, awkward.forms.UnionForm):
        raise NotImplementedError

    elif isinstance(form, (awkward.forms.NumpyForm, awkward.forms.EmptyForm)):
        if form.form_key in form_keys_touched:
            return form.copy()
        else:
            return None

    else:
        if form.form_key in form_keys_touched:
            return form.copy(content=project_form(form.content, form_keys_touched))
        else:
            return None
