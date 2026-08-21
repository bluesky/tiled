"""
Tests for the built-in XDI serializer (tiled/serialization/xdi.py).

The serializer is registered under the spec name "xdi" and operates on container
nodes (MapAdapter) tagged with Spec("xdi", ...). It walks the container to find
the first table child, reads it, and formats the XDI file using the container's
metadata.
"""

import io
import re

import pandas
import pytest

from tiled.adapters.dataframe import DataFrameAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context
from tiled.client.utils import ClientError
from tiled.server.app import build_app
from tiled.structures.core import Spec

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

XDI_METADATA = {
    "xdi_version": "1.0",
    "extra_version": "GSE/1.0",
    "fields": {
        "Element": {"symbol": "Cu", "edge": "K"},
        "Mono": {"d_spacing": "3.13553", "name": "Si 111"},
        "Beamline": {"name": "13ID", "collimation": "none", "focusing": "yes"},
        "Facility": {
            "name": "APS",
            "energy": "7.00 GeV",
            "xray_source": "APS Undulator A",
        },
        "Scan": {"start_time": "2001-06-26T22:27:31", "edge_energy": "8980.0"},
        "Detector": {"I0": "10cm N2", "I1": "10cm N2"},
        "Sample": {"name": "Cu", "prep": "Cu metal foil"},
        "Column": {"1": "energy eV", "2": "i0", "3": "itrans", "4": "mutrans"},
        # extension namespace
        "GSE": {"EXTRA": "config 1"},
    },
    "comments": "# Cu foil Room Temperature\n# measured at beamline 13-ID\n",
}

XDI_DF = pandas.DataFrame(
    {
        "energy": [8779.0, 8789.0, 8799.0],
        "i0": [149013.7, 144864.7, 132978.7],
        "itrans": [550643.089065, 531876.119084, 489591.10592],
        "mutrans": [-1.3070486, -1.3006104, -1.3033816],
    }
)

# Metadata without a "comments" key — tests the no-comment path
XDI_METADATA_NO_COMMENTS = {
    "xdi_version": "1.0",
    "extra_version": "",
    "fields": {
        "Element": {"symbol": "Ti", "edge": "K"},
        "Mono": {"d_spacing": "3.1353241"},
        "Column": {"1": "energy eV"},
    },
}


def _make_xdi_container(df, meta, extra_specs=None):
    """Wrap a DataFrame in a MapAdapter container tagged with Spec('xdi')."""
    specs = [Spec("xdi", version="1.0")] + (extra_specs or [])
    return MapAdapter(
        {"primary": DataFrameAdapter.from_pandas(df.copy(), npartitions=1)},
        metadata=meta,
        specs=specs,
    )


def _build_tree():
    return MapAdapter(
        {
            "xdi_scan": _make_xdi_container(XDI_DF, XDI_METADATA),
            "xdi_no_comments": _make_xdi_container(
                XDI_DF[["energy"]].copy(), XDI_METADATA_NO_COMMENTS
            ),
            # A plain container with no Spec("xdi") — must not be exportable as XDI
            "plain_container": MapAdapter(
                {"primary": DataFrameAdapter.from_pandas(XDI_DF.copy(), npartitions=1)},
            ),
        }
    )


@pytest.fixture(scope="module")
def client():
    app = build_app(_build_tree())
    with Context.from_app(app) as context:
        yield from_context(context)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _export_to_string(client_node, **kwargs):
    """Export a node to an in-memory buffer and return the decoded string."""
    buf = io.BytesIO()
    client_node.export(buf, **kwargs)
    buf.seek(0)
    return buf.read().decode()


# ---------------------------------------------------------------------------
# Tests: file export
# ---------------------------------------------------------------------------


def test_export_xdi_to_file(client, tmp_path):
    """Export to a file with a .xdi extension using the file-extension alias."""
    out = tmp_path / "output.xdi"
    client["xdi_scan"].export(out)
    assert out.exists()
    content = out.read_text()
    assert content.startswith("# XDI/")


# ---------------------------------------------------------------------------
# Tests: buffer export and output structure
# ---------------------------------------------------------------------------


def test_export_xdi_to_buffer_explicit_mimetype(client):
    """Export with explicit MIME type produces non-empty bytes output."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert len(content) > 0


def test_xdi_output_version_line(client):
    """First line must be the XDI version line."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    first_line = content.splitlines()[0]
    assert first_line == "# XDI/1.0 GSE/1.0"


def test_xdi_output_version_line_no_extra_version(client):
    """Version line with empty extra_version should have no trailing space."""
    content = _export_to_string(client["xdi_no_comments"], format="application/x-xdi")
    first_line = content.splitlines()[0]
    assert first_line == "# XDI/1.0"
    assert not first_line.endswith(" ")


def test_xdi_output_required_element_fields(client):
    """Element.symbol and Element.edge must be present in the header."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert "# Element.symbol: Cu" in content
    assert "# Element.edge: K" in content


def test_xdi_output_required_mono_field(client):
    """Mono.d_spacing must be present in the header."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert "# Mono.d_spacing: 3.13553" in content


def test_xdi_output_extension_namespace(client):
    """Extension namespace fields (e.g. GSE.EXTRA) must appear in the header."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert "# GSE.EXTRA: config 1" in content


def test_xdi_output_defined_namespaces_before_extensions(client):
    """Defined namespaces (Element, Mono, ...) must precede extension namespaces."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    element_pos = content.find("# Element.")
    gse_pos = content.find("# GSE.")
    assert (
        element_pos < gse_pos
    ), "Defined namespaces should appear before extension namespaces"


def test_xdi_output_field_end_separator(client):
    """The field-end separator '# /////' must be present."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert re.search(r"^# /{3,}", content, re.MULTILINE)


def test_xdi_output_header_end_separator(client):
    """The header-end separator '# -----' must be present."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert re.search(r"^# -{3,}", content, re.MULTILINE)


def test_xdi_output_generated_by_tiled_comment(client):
    """The '# generated by tiled' provenance comment must be present."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert "# generated by tiled" in content


def test_xdi_output_user_comments_preserved(client):
    """User comments from metadata must be written between the separators."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert "# Cu foil Room Temperature" in content
    assert "# measured at beamline 13-ID" in content


def test_xdi_output_separators_always_present_without_comments(client):
    """Field-end and header-end separators must appear even when there are no user comments."""
    content = _export_to_string(client["xdi_no_comments"], format="application/x-xdi")
    assert re.search(r"^# /{3,}", content, re.MULTILINE)
    assert re.search(r"^# -{3,}", content, re.MULTILINE)


def test_xdi_output_column_labels_line(client):
    """The column label line must list all DataFrame column names, prefixed with '#'."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert "# energy i0 itrans mutrans" in content


def test_xdi_output_column_labels_after_header_end(client):
    """Column labels must appear after the header-end separator."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    header_end_pos = content.rfind("# ----")
    col_labels_pos = content.find("# energy i0 itrans mutrans")
    assert (
        col_labels_pos > header_end_pos
    ), "Column labels should appear after the header-end separator"


def test_xdi_output_data_rows_present(client):
    """Data rows must be present in the output."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    data_lines = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    assert len(data_lines) == len(
        XDI_DF
    ), f"Expected {len(XDI_DF)} data rows, got {len(data_lines)}"


def test_xdi_output_space_delimited_data(client):
    """Data rows must be whitespace-delimited (not comma-separated), per the XDI spec."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    data_lines = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    for line in data_lines:
        assert "," not in line, f"Data line should not contain commas: {line!r}"
        assert len(line.split()) == 4


def test_xdi_output_numeric_data_parseable(client):
    """Data values must be parseable as floats."""
    content = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    data_lines = [ln for ln in content.splitlines() if ln and not ln.startswith("#")]
    for line in data_lines:
        for v in line.split():
            float(v)  # should not raise


def test_xdi_output_returns_bytes(client):
    """The serializer must return bytes (not str)."""
    buf = io.BytesIO()
    client["xdi_scan"].export(buf, format="application/x-xdi")
    buf.seek(0)
    raw = buf.read()
    assert isinstance(raw, bytes)


# ---------------------------------------------------------------------------
# Tests: roundtrip
# ---------------------------------------------------------------------------


def test_xdi_roundtrip(client, tmp_path):
    """Output must be re-parseable by XDIAdapter (roundtrip integrity)."""
    import collections

    out = tmp_path / "roundtrip.xdi"
    client["xdi_scan"].export(out)

    content = out.read_text()
    lines = iter(content.splitlines(keepends=True))

    version_line = next(lines)
    m = re.match(r"#\s*XDI/(\S+)(?:\s+(\S+))?", version_line)
    assert m is not None, f"Version line not parseable: {version_line!r}"
    assert m.group(1) == "1.0"

    field_end_re = re.compile(r"#\s*/{3,}")
    header_end_re = re.compile(r"#\s*-{3,}")
    fields = collections.defaultdict(dict)

    for line in lines:
        if line[0] != "#":
            pytest.fail(f"Non-comment line in header: {line!r}")
        if re.match(field_end_re, line):
            break
        elif re.match(header_end_re, line):
            break
        stripped = line[1:].strip()
        if not stripped or ":" not in stripped:
            continue
        key, val = stripped.split(":", 1)
        val = val.strip()
        if "." in key:
            namespace, tag = key.split(".", 1)
            fields[namespace.strip()][tag.strip()] = val

    assert "Element" in fields
    assert "symbol" in fields["Element"]
    assert "edge" in fields["Element"]
    assert "Mono" in fields
    assert "d_spacing" in fields["Mono"]


# ---------------------------------------------------------------------------
# Tests: format alias
# ---------------------------------------------------------------------------


def test_xdi_format_alias(client):
    """Requesting format='xdi' (file extension alias) should work the same as 'application/x-xdi'."""
    content_alias = _export_to_string(client["xdi_scan"], format="xdi")
    content_full = _export_to_string(client["xdi_scan"], format="application/x-xdi")
    assert content_alias == content_full


# ---------------------------------------------------------------------------
# Tests: non-XDI containers cannot be exported as XDI
# ---------------------------------------------------------------------------


def test_plain_container_cannot_export_as_xdi(client):
    """A container without Spec('xdi') must not be exportable as application/x-xdi."""
    with pytest.raises(ClientError):
        _export_to_string(client["plain_container"], format="application/x-xdi")
