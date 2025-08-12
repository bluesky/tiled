import re
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

from ..client import Context, from_context
from ..client.register import register
from ..examples.xdi import data
from ..server.app import build_app_from_config


def load_data(text: str) -> tuple[list[str], pd.DataFrame]:
    # This regular expression matches and removes a block of comment lines
    #
    # Pattern breakdown:
    #   - #\s*\/+\r?\n
    #       Matches a line starting with '#', optional whitespace, one or more '/',
    #       then a newline(unix or windows).
    #       Example #  //////// newline
    #   - (?:#\s+.*\r?\n)*
    #       Matches zero or more lines starting with '#', at least one space, then any text,
    #       then a newline(unix or windows).
    #       Example #    Comment here newline
    #   - #\s*-+\r?\n
    #       Matches a line starting with '#', optional whitespace, one or more '-',
    #       then a newline(unix or windows).
    #       Example #      -----------newline
    # Example matched block:
    #   # ////////
    #   # Comment Here
    #   # More Comments
    #   # -------
    text = re.sub(r"#\s*\/+\r?\n(?:#\s+.*\r?\n)*#\s*-+\r?\n", "", text)
    lines = text.strip().splitlines()
    metadata_lines = [line.replace(" ", "") for line in lines if line.startswith("#")]
    data_lines = [line.strip() for line in lines if not line.startswith("#")]
    return metadata_lines, pd.read_csv(
        StringIO("\n".join(data_lines)), sep=r"\s+", header=None
    )


@pytest.mark.asyncio
async def test_xdi_round_trip(tmp_path: Path):
    """
    Steps:

    1. Parse XDI string into DataFrame + dict.
    2. Export as XDI to client.
    3. Read exported XDI into DataFrame + dict.

    Compare result of (3) to result of (1).
    """
    # Write example data file.
    Path(tmp_path / "files").mkdir()
    with open(tmp_path / "files" / "example.xdi", "w") as file:
        file.write(data)
    config = {
        "trees": [
            {
                "tree": "catalog",
                "path": "/",
                "args": {
                    "uri": tmp_path / "catalog.db",
                    "readable_storage": [tmp_path / "files"],
                    "init_if_not_exists": True,
                    "adapters_by_mimetype": {
                        "application/x-xdi": "tiled.examples.xdi:XDIAdapter"
                    },
                },
            }
        ],
        "media_types": {"xdi": {"application/x-xdi": "tiled.examples.xdi:write_xdi"}},
        "file_extensions": {"xdi": "application/x-xdi"},
    }
    with Context.from_app(build_app_from_config(config)) as context:
        client = from_context(context)
        await register(
            client,
            tmp_path / "files",
            adapters_by_mimetype={"application/x-xdi": "tiled.examples.xdi:XDIAdapter"},
            mimetypes_by_file_ext={".xdi": "application/x-xdi"},
        )

        client["example"].export(str(tmp_path / "exported.xdi"))
        actual = Path(tmp_path / "exported.xdi").read_text()
        metadata_actual, df_actual = load_data(actual)
        metadata_expected, df_expected = load_data(data)
        assert df_actual.equals(df_expected)
        assert len(metadata_actual) == len(metadata_expected) and set(
            metadata_actual
        ) == set(metadata_expected)
