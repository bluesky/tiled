from dataclasses import dataclass

import awkward


@dataclass
class AwkwardStructure:
    length: int
    form: dict

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


ATTRIBUTES_BY_CLASS = {
    "NumpyArray": ["data"],
    "EmptyArray": [],
    "ListOffsetArray": ["offsets"],
    "ListArray": ["starts", "stops"],
    "RegularArray": [],
    "IndexedArray": ["index"],
    "IndexedOptionArray": ["index"],
    "ByteMaskedArray": ["mask"],
    "BitMaskedArray": ["mask"],
    "UnmaskedArray": [],
    "RecordArray": [],
    "UnionArray": ["tags", "index"],
}


def suffixed_form_keys(form):
    "Given a form, extract suffixed form keys like 'node0-data'."
    result = []
    attributes = ATTRIBUTES_BY_CLASS[form["class"]]
    for attribute in attributes:
        result.append(f"{form['form_key']}-{attribute}")
    if "content" in form:
        result.extend(suffixed_form_keys(form["content"]))
    elif "contents" in form:
        for content in form["contents"]:
            result.extend(suffixed_form_keys(content))
    return result
