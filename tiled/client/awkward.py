import awkward

from ..serialization.awkward import from_zipped_buffers, to_zipped_buffers
from ..structures.awkward import project_form
from .base import BaseClient
from .utils import handle_error


class AwkwardArrayClient(BaseClient):
    def __repr__(self):
        # TODO Include some summary of the structure. Probably
        # lift __repr__ code from awkward itself here.
        return f"<{type(self).__name__}>"

    def write(self, container):
        handle_error(
            self.context.http_client.put(
                self.item["links"]["full"],
                content=bytes(to_zipped_buffers(container, {})),
                headers={"Content-Type": "application/zip"},
            )
        )

    def read(self, slice=...):
        structure = self.structure()
        form = awkward.forms.from_dict(structure.form)
        typetracer, report = awkward.typetracer.typetracer_with_report(
            form,
        )
        proxy_array = awkward.Array(typetracer)
        # TODO Ask awkward to promote _touch_data to a public method.
        ak.typetracer.touch_data(proxy_array[slice])
        form_keys_touched = set(report.data_touched)
        projected_form = project_form(form, form_keys_touched)
        # The order is not important, but sort so that the request is deterministic.
        params = {"form_key": sorted(list(form_keys_touched))}
        content = handle_error(
            self.context.http_client.get(
                self.item["links"]["full"],
                headers={"Accept": "application/zip"},
                params=params,
            )
        ).read()
        container = from_zipped_buffers(content)
        projected_array = awkward.from_buffers(
            projected_form, structure.length, container
        )
        return projected_array[slice]

    def __getitem__(self, slice):
        return self.read(slice=slice)
