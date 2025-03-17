from typing import Annotated, Optional

from pydantic import AfterValidator, BaseModel, Field
from pydantic_settings import CliApp, CliSubCommand

from tiled.commandline._utils import ContextCommand


def to_seconds(expires_in: Optional[str]) -> Optional[str]:
    if expires_in is None:
        return expires_in
    return expires_in + "s" if expires_in.isdigit() else expires_in


class Create(ContextCommand):
    expires_in: Annotated[
        Optional[str],
        "Number of seconds until API key expires, given as integer seconds "
        "or a string like: '3y' (years), '3d' (days), '5m' (minutes), '1h' "
        "(hours), '30s' (seconds). If None, it will never expire or it will "
        "have the maximum lifetime allowed by the server. ",
        AfterValidator(to_seconds),
    ] = None
    scopes: Annotated[
        Optional[set[str]],
        "Restrict the access available to this API key by listing scopes. "
        "By default, it will inherit the scopes of its owner.",
    ] = None
    note: Annotated[Optional[str], "Add a note to label this API key."] = None
    no_verify: Annotated[bool, "Skip SSL verification."] = False

    def cli_cmd(self) -> None:
        context = self.context()
        info = context.create_api_key(
            scopes=self.scopes, expires_in=self.expires_in, note=self.note
        )
        # TODO Print other info to the stderr?
        print(info["secret"])


class Revoke(ContextCommand):
    first_eight: Annotated[
        str,
        "First eight characters of API key (or the whole key)",
        AfterValidator(lambda string: string[:8]),
    ]

    async def cli_cmd(self) -> None:
        context = self.context()
        context.revoke_api_key(self.first_eight)


class ListKeys(ContextCommand):
    def cli_cmd(self) -> None:
        context = self.context()
        info = context.whoami()
        if not info["api_keys"]:
            print("No API keys found")
            return
        max_note_len = max(len(api_key.get("note", "") for api_key in info["api_keys"]))
        print(
            f"First 8   Expires at (UTC)     Latest activity      Note{' ' * (max_note_len - 4)}  Scopes"
        )
        for api_key in info["api_keys"]:
            note_padding = 2 + max_note_len - len(api_key["note"] or "")
            if api_key["expiration_time"] is None:
                expiration_time = "-"
            else:
                expiration_time = (
                    api_key["expiration_time"]
                    .replace(microsecond=0, tzinfo=None)
                    .isoformat()
                )
            if api_key["latest_activity"] is None:
                latest_activity = "-"
            else:
                latest_activity = (
                    api_key["latest_activity"]
                    .replace(microsecond=0, tzinfo=None)
                    .isoformat()
                )
            print(
                (
                    f"{api_key['first_eight']:10}"
                    f"{expiration_time:21}"
                    f"{latest_activity:21}"
                    f"{(api_key['note'] or '')}{' ' * note_padding}"
                    f"{' '.join(api_key['scopes']) or '-'}"
                )
            )


class APIKeys(BaseModel):
    list_keys: CliSubCommand[ListKeys] = Field(alias="list")
    create: CliSubCommand[Create]
    revoke: CliSubCommand[Revoke]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)
