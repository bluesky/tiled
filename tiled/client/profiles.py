"""
This module handles client-side configuration.

It contains several functions that are factored to facilitate testing,
but the user-facing functionality is striaghtforward.
"""
import collections
import collections.abc
import os
import sys
import warnings

import appdirs

from .catalog import from_uri


__all__ = ["discover_profiles", "from_profile", "paths"]


# Paths later in the list ("closer" to the user) have higher precedence.
paths = [
    os.getenv("TILED_SYSTEM_PROFILES", appdirs.site_config_dir("tiled")),  # system
    os.path.join(sys.prefix, "etc", "tiled"),  # environment
    os.getenv("TILED_PROFILES", appdirs.user_config_dir("tiled")),  # user
]


def parse(filepath):
    """
    Given a config filepath, detect the format from its name, and parse it.
    """
    _, ext = os.path.splitext(filepath)
    if ext in (".yml", ".yaml"):
        import yaml

        with open(filepath) as file:
            return yaml.safe_load(file.read())

    # TODO Support TOML and maybe others.
    else:
        raise UnrecognizedExtension("Must be .yaml or .yml")


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
                try:
                    content = parse(filepath)
                except Exception:
                    if strict:
                        raise
                    else:
                        warnings.warn(f"Failed to parse {filepath}. Skipping.")
                if not isinstance(content, collections.abc.Mapping):
                    if strict:
                        raise
                    else:
                        warnings.warn(
                            f"Content of {filepath} is not a mapping. Skipping."
                        )
                filepath_to_content[filepath] = content
        levels.append(filepath_to_content)
    return levels


def resolve_precedence(levels):
    """
    Given a list of mappings (filename-to-content), resolve precedence.

    In the event of irresolvable collisions, drop the offenders and warn.
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
            # Does not than one file *in this same level (directory) in the search path*
            # define a profile with the same name. If so, there is no sure way to decide
            # precedence so we will omit it and issue a warning, unless something later
            # in the search path overrides it.
            if len(filepaths) > 1:
                # Stash collision for possible warning below.
                collisions[profile_name] = filepaths
                # If a previous level defined this, remove it.
                combined.pop(profile_name, None)
            else:
                (filepath,) = filepaths
                combined.update(filepath_to_content[filepath])
    NEWLINE = "\n"  # because '\n' cannot be used inside f-string below
    for profile_name, filepaths in collisions.items():
        warnings.warn(
            f"""More than file in the same directory

{NEWLINE.join(filepaths)}

defines a profile with the name {profile_name}.

The profile will be ommiited. Fix this by removing one of the duplicates
or by overriding them with a user-level profile defined under {paths[-1]}."""
        )
    return combined


def discover_profiles():
    """
    Return a mapping of profile names to profiles.

    Search path is available from Python as:

    >>> tiled.client.profiles.paths

    or from a CLI as:

    $ tiled profiles paths
    """
    levels = gather_profiles(paths, strict=False)
    profiles = resolve_precedence(levels)
    return profiles


def from_profile(name):
    """
    Build a Catalog based a 'profile' (a named configuration).

    List available profiles from Python like:

    >>> from tiled.client.profiles import discover_profiles
    >>> list(discover_profiles())

    or from a CLI like:

    $ tiled profiles list
    """
    profiles = discover_profiles()
    try:
        profile = profiles[name]
    except KeyError as err:
        NEWLINE = "\n"  # because '\n' cannot be used inside f-string below
        raise ProfileNotFound(
            f"""Profile {name} not found. Found profiles:

{NEWLINE.join(profiles)}

from configuration in directories:

{NEWLINE.join(paths)}"""
        ) from err
    return from_uri(**profile)
    # TODO Recognize 'direct' profile.


class UnrecognizedExtension(ValueError):
    pass


class ProfileNotFound(KeyError):
    pass
