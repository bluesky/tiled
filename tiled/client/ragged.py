from typing import TYPE_CHECKING, Any, Dict, Union, cast
from urllib.parse import parse_qs, urlparse

import awkward as ak
import ragged

from tiled.client.base import BaseClient
from tiled.client.utils import export_util, handle_error, retry_context
from tiled.ndslice import NDSlice
from tiled.serialization.ragged import to_flattened_octet_stream, to_json
from tiled.structures.awkward import project_form

if TYPE_CHECKING:
    from tiled.structures.ragged import RaggedStructure


class RaggedClient(BaseClient):
    def write(self, array: Union[ragged.array, ak.Array]):
        for attempt in retry_context():
            with attempt:
                handle_error(
                    self.context.http_client.put(
                        self.item["links"]["full"],
                        content=bytes(
                            # to_zipped_buffers("application/zip", ragged.asarray(array), {})
                            # to_json("application/json", ragged.asarray(array), {})
                            to_flattened_octet_stream(
                                "application/octet-stream", ragged.asarray(array), {}
                            )
                        ),
                        headers={"Content-Type": "application/octet-stream"},
                    )
                )

    def read(self, slice: NDSlice = ...) -> ragged.array:
        structure = cast("RaggedStructure", self.structure())

        # form = ak.forms.from_dict(structure.form)
        # typetracer, report = ak.typetracer.typetracer_with_report(form)
        # proxy_array = ak.Array(typetracer)
        # ak.typetracer.touch_data(proxy_array[slice])
        # form_keys_touched = set(report.data_touched)
        # projected_form = project_form(form, form_keys_touched)
        # url_path = self.item["links"]["full"]
        # url_params: Dict[str, Any] = {**parse_qs(urlparse(url_path).query)}
        # if isinstance(slice, NDSlice):
        #     url_params["slice"] = slice.to_numpy_str()
        # for attempt in retry_context():
        #     with attempt:
        #         content = handle_error(
        #             self.context.http_client.get(
        #                 url_path,
        #                 headers={"Accept": "application/zip"},
        #                 params=url_params,
        #             )
        #         ).read()
        # container = from_zipped_buffers(content, projected_form, structure.length)
        # projected_array = ragged.array(
        #     ak.from_buffers(
        #         projected_form,
        #         structure.length,
        #         container,
        #         allow_noncanonical_form=True,
        #     )
        # )
        # return projected_array[slice]

    def __getitem__(
        self, slice: NDSlice
    ) -> ragged.array:  # this is true even when slicing to return a single item
        # TODO: should we be smarter, and return the scalar rather a singular array
        return self.read(slice=slice)

    def export(self, filepath, *, format=None):
        return export_util(
            filepath,
            format,
            self.context.http_client.get,
            self.item["links"]["full"],
            params={},
        )
