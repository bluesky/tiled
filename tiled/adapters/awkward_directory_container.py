from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterator


class DirectoryContainer(Mapping[str, bytes]):
    """A storage container for byte-arrays representing Awkward Array buffers

    Each buffer is stored as a separate file in a given directory, with the
    filename corresponding to the form key.
    """

    def __init__(self, directory: Path):
        self.directory = directory

    def __getitem__(self, key: str) -> bytes:
        with open(self.directory / key, "rb") as file:
            return file.read()

    def __setitem__(self, key: str, value: bytes) -> None:
        with open(self.directory / key, "wb") as file:
            file.write(value)

    def __delitem__(self, key: str) -> None:
        (self.directory / key).unlink(missing_ok=True)

    def __iter__(self) -> Iterator[str]:
        yield from (p.name for p in self.directory.iterdir())

    def __len__(self) -> int:
        return sum(1 for _ in self.__iter__())
