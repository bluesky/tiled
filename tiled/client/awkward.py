import awkward

from ..serialization.awkward import from_zipped_buffers, to_zipped_buffers
from ..structures.awkward import project_form
from .base import BaseClient
from .utils import export_util, handle_error, retry_context


class AwkwardClient(BaseClient):
    def __repr__(self):
        # TODO Include some summary of the structure. Probably
        # lift __repr__ code from awkward itself here.
        form = awkward.forms.from_dict(self.structure().form)
        if isinstance(form, awkward.forms.recordform.RecordForm):
            out = f"<{type(self).__name__} {{"
            # Always show at least one field
            if form.fields:
                out += form.fields[0]
            # And then show as many more as we can fit on one line.
            counter = 1
            for sample_repr in form.fields[1:]:
                if len(out) + len(sample_repr) > 60:  # character count
                    break
                out += ", " + sample_repr
                counter += 1
            # Are there more fields that what we displayed above?
            if len(form.fields) > counter:
                out += f", ...}} ~{len(form.fields)} field"
                out += "s," if len(form.fields) > 1 else ","
            else:
                out += "}"
            out += f" {self.structure().length} item"
            out += "s>" if self.structure().length > 1 else ">"
            return out

        return f"<{type(self).__name__}>"

    def write(self, container):
        structure = self.structure()
        components = (structure.form, structure.length, container)
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=bytes(
                            to_zipped_buffers("application/zip", components, {})
                        ),
                        headers={"Content-Type": "application/zip"},
                    )
                )

    def read(self, slice=...):
        structure = self.structure()
        form = awkward.forms.from_dict(structure.form)
        typetracer, report = awkward.typetracer.typetracer_with_report(form)
        proxy_array = awkward.Array(typetracer)
        awkward.typetracer.touch_data(proxy_array[slice])
        form_keys_touched = set(report.data_touched)
        projected_form = project_form(form, form_keys_touched)
        # The order is not important, but sort so that the request is deterministic.
        form_keys = sorted(form_keys_touched)
        for attempt in retry_context():
            with attempt:
                content = handle_error(
                    self.context.http_client.post(
                        self.item["links"]["buffers"],
                        headers={"Accept": "application/zip"},
                        json=form_keys,
                    )
                ).read()
        container = from_zipped_buffers(content, projected_form, structure.length)
        projected_array = awkward.from_buffers(
            projected_form, structure.length, container, allow_noncanonical_form=True
        )
        return projected_array[slice]

    def __getitem__(self, slice):
        return self.read(slice=slice)

    def export(self, filepath, *, format=None):
        """
        Download data in some format and write to a file.

        Parameters
        ----------
        file: str or buffer
            Filepath or writeable buffer.
        format : str, optional
            If format is None and `file` is a filepath, the format is inferred
            from the name, like 'table.csv' implies format="text/csv". The format
            may be given as a file extension ("csv") or a media type ("text/csv").

        Examples
        --------

        Export as Parquet.

        >>> a.export("awkward.parquet")

        Export as JSON.

        >>> a.export("awkward.json")

        """
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params={},
        )
