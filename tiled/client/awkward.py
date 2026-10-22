import awkward

from ._protocol.awkward import AwkwardReadRecipe, draft_request_write_awkward_full
from .base import BaseClient
from .utils import export_util


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
        draft = draft_request_write_awkward_full(
            self.item["links"]["full"], self.structure(), container
        )
        self._send(draft)

    def read(self, slice=...):
        recipe = AwkwardReadRecipe.for_buffers(
            self.item["links"]["buffers"], self.structure(), slice
        )
        content = self._send(recipe.draft).read()
        projected_array = recipe.parse(content)
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
            self.context,
            self.item["links"]["full"],
            params={},
        )
