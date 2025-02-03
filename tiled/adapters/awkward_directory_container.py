from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterator


class DirectoryContainer(Mapping[str, bytes]):
    """ """

    def __init__(self, directory: Path, form: Any):
        """

        Parameters
        ----------
        directory :
        form :
        """
        self.directory = directory
        self.form = form

    def __getitem__(self, form_key: str) -> bytes:
        """

        Parameters
        ----------
        form_key :

        Returns
        -------

        """
        with open(self.directory / form_key, "rb") as file:
            return file.read()

    def __setitem__(self, form_key: str, value: bytes) -> None:
        """

        Parameters
        ----------
        form_key :
        value :

        Returns
        -------

        """
        with open(self.directory / form_key, "wb") as file:
            file.write(value)

    def __delitem__(self, form_key: str) -> None:
        """

        Parameters
        ----------
        form_key :

        Returns
        -------

        """
        (self.directory / form_key).unlink(missing_ok=True)

    def __iter__(self) -> Iterator[str]:
        """

        Returns
        -------

        """
        yield from self.form.expected_from_buffers()

    def __len__(self) -> int:
        """

        Returns
        -------

        """
        return len(self.form.expected_from_buffers())
