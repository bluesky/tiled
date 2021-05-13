"""
This module handles client configuration.

See config.py for server configuration.

It contains several functions that are factored to facilitate testing,
but the user-facing functionality is striaghtforward.
"""
import collections
import collections.abc
import os
import sys
import warnings

import appdirs

from .utils import parse


__all__ = ["list_profiles", "load_profiles", "paths"]


# Paths later in the list ("closer" to the user) have higher precedence.
paths = [
    os.getenv(
        "TILED_SITE_PROFILES",
        os.path.join(appdirs.site_config_dir("tiled"), "profiles"),
    ),  # system
    os.path.join(sys.prefix, "etc", "tiled", "profiles"),  # environment
    os.getenv(
        "TILED_PROFILES", os.path.join(appdirs.user_config_dir("tiled"), "profiles")
    ),  # user
]


def gather_profiles(paths, strict=True):
    """
    For each path in paths, return a dict mapping filepath to content.
    """
    levels = []
    for path in paths:
        filepath_to_content = {}
        if os.path.isdir(path):
            for filename in os.listdir(path):
                filepath = os.path.join(path, filename)
                # Ignore hidden files and .py files.
                if (
                    filename.startswith(".")
                    or filename.endswith(".py")
                    or filename == "__pycache__"
                ):
                    continue
                try:
                    with open(filepath) as file:
                        content = parse(file)
                except Exception as err:
                    if strict:
                        raise
                    else:
                        warnings.warn(
                            f"Skipping {filepath!r}. Failed to parse with error: {err!r}."
                        )
                        continue
                if not isinstance(content, collections.abc.Mapping):
                    if strict:
                        raise
                    else:
                        warnings.warn(
                            f"Skipping {filepath!r}. Content has invalid structure (not a mapping)."
                        )
                filepath_to_content[filepath] = content
        levels.append(filepath_to_content)
    return levels


def resolve_precedence(levels):
    """
    Given a list of mappings (filename-to-content), resolve precedence.

    In the event of irresolvable collisions, drop the offenders and warn.

    The result is a mapping from profile name to (filepath, content).
    """
    # Map each profile_name to the file(s) that define a profile with that name.
    # This is used to track collisions.
    profile_name_to_filepaths_per_level = [
        collections.defaultdict(list) for _ in range(len(paths))
    ]
    for profile_name_to_filepaths, filepath_to_content in zip(
        profile_name_to_filepaths_per_level, levels
    ):
        for filepath, content in filepath_to_content.items():
            for profile_name in content:
                profile_name_to_filepaths[profile_name].append(filepath)
    combined = {}
    collisions = {}
    for profile_name_to_filepaths, filepath_to_content in zip(
        profile_name_to_filepaths_per_level, levels
    ):
        for profile_name, filepaths in profile_name_to_filepaths.items():
            # A profile name in this level resolves any collisions in the previous level.
            collisions.pop(profile_name, None)
            # Does more than one file *in this same level (directory) in the search path*
            # define a profile with the same name? If so, there is no sure way to decide
            # precedence so we will omit it and issue a warning, unless something later
            # in the search path overrides it.
            if len(filepaths) > 1:
                # Stash collision for possible warning below.
                collisions[profile_name] = filepaths
                # If a previous level defined this, remove it.
                combined.pop(profile_name, None)
            else:
                (filepath,) = filepaths
                combined[profile_name] = (
                    filepath,
                    filepath_to_content[filepath][profile_name],
                )
    MSG = """More than file in the same directory:

{filepaths}

defines a profile with the name {profile_name!r}.

The profile will be ommitted. Fix this by removing one of the duplicates"""
    for profile_name, filepaths in collisions.items():
        if filepaths[0].startswith(paths[-1]):
            msg = (MSG + ".").format(
                filepaths="\n".join(filepaths), profile_name=profile_name
            )
        else:
            msg = (
                MSG
                + (
                    "or by defining a profile with that name in a "
                    f"file in the user config directory {paths[-1]} "
                    "to override them."
                )
            ).format(filepaths="\n".join(filepaths), profile_name=profile_name)
        warnings.warn(msg)
    return combined


def load_profiles():
    """
    Return a mapping of profile_name to (source_path, content).

    The search path for the source files is available from Python as:

    >>> tiled.client.profiles.paths

    or from a CLI as:

    $ tiled profile paths

    See also

    $ tiled profile list
    $ tiled profile show PROFILE_NAME
    """
    levels = gather_profiles(paths, strict=False)
    profiles = resolve_precedence(levels)
    return profiles


def list_profiles():
    """
    Return a mapping of profile names to source filepath.

    The search path for the source files is available from Python as:

    >>> tiled.client.profiles.paths

    or from a CLI as:

    $ tiled profile paths

    See also

    $ tiled profile list
    $ tiled profile show PROFILE_NAME
    """
    levels = gather_profiles(paths, strict=False)
    profiles = resolve_precedence(levels)
    return {name: source_filepath for name, (source_filepath, _) in profiles.items()}


class ProfileNotFound(KeyError):
    pass
