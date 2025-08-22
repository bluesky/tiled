from pathlib import Path

from tiled.access_control.access_tags import AccessTagsCompiler
from tiled.access_control.scopes import ALL_SCOPES


def group_parser(groupname):
    return {
        "group_A": ["alice", "bob"],
        "admins": ["cara"],
    }[groupname]


def main():
    file_directory = Path(__file__).resolve().parent

    access_tags_compiler = AccessTagsCompiler(
        ALL_SCOPES,
        Path(file_directory, "tag_definitions.yml"),
        {"uri": f"file:{file_directory}/compiled_tags.sqlite"},
        group_parser,
    )

    access_tags_compiler.load_tag_config()
    access_tags_compiler.compile()
    access_tags_compiler.connection.close()


if __name__ == "__main__":
    main()
