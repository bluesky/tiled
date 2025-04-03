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
                contents.append(projected)

        if form.fields is None:
            fields = None

        return form.copy(fields=fields, contents=contents)

    elif isinstance(form, awkward.forms.UnionForm):
        step1 = [project_form(x, form_keys_touched) for x in form.contents]
        step2 = [x for x in step1 if x is not None]
        if len(step2) == 0:
            return None
        elif len(step2) == 1:
            return step2[0]
        elif step2 == form.contents:
            return form
        else:
            raise NotImplementedError(
                "Certain UnionForms are not yet supported. "
                "See https://github.com/scikit-hep/awkward/issues/2666"
            )
            return awkward.forms.UnionForm.simplified(
                form.tags,
                form.index,
                step2,
                parameters=form.parameters,
                form_key=form.form_key,
            )

    elif isinstance(form, awkward.forms.NumpyForm):
        if form.form_key in form_keys_touched:
            return form.copy()
        else:
            return None

    elif isinstance(form, (awkward.forms.RegularForm, awkward.forms.UnmaskedForm)):
        return form.copy(content=project_form(form.content, form_keys_touched))

    elif isinstance(form, awkward.forms.EmptyForm):
        return form.copy()

    else:
        if form.form_key in form_keys_touched:
            return form.copy(content=project_form(form.content, form_keys_touched))
        else:
            return None
