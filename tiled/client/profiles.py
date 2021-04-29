import collections
import collections.abc
import os
import sys
import warnings

import appdirs

from .catalog import from_uri


# Paths later in the list ("closer" to the user) have higher precedence.
paths = [
    os.getenv("TILED_SYSTEM_PROFILES", appdirs.site_config_dir("tiled")),  # system
    os.path.join(sys.prefix, "etc", "tiled"),  # environment
    os.getenv("TILED_PROFILES", appdirs.user_config_dir("tiled")),  # user
]


def parse(filename):
    _, ext = os.path.splitext(filename)
    if ext in (".yml", ".yaml"):
        import yaml

        with open(filename) as file:
            return yaml.safe_load(file.read())

    # TODO Support TOML and maybe others.
    else:
        raise UnrecognizedExtension("Must be .yaml or .yml")


def merge_profiles():
    # Map filepath to parsed content.
    parsed = {}
    # Map each profile_name to the file(s) that define a profile with that name.
    # This is used to track collisions.
    profile_name_to_filepaths = [
        collections.defaultdict(list) for _ in range(len(paths))
    ]
    for i, path in enumerate(paths):
        if os.path.isdir(path):
            for filename in os.listdir(path):
                filepath = os.path.join(path, filename)
                try:
                    content = parse(filepath)
                except Exception:
                    warnings.warn(f"Failed to parse {filepath}. Skipping.")
                if not isinstance(content, collections.abc.Mapping):
                    warnings.warn(f"Content of {filepath} is not a mapping. Skipping.")
                parsed[filepath] = content
                for profile_name in content:
                    profile_name_to_filepaths[i][profile_name].append(filepath)
    combined = {}
    collisions = {}
    for level in profile_name_to_filepaths:
        for profile_name, filepaths in level.items():
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
                combined.update(parsed[filepath])
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


class UnrecognizedExtension(ValueError):
    pass


def from_profile(name):
    profile = merge_profiles()[name]
    return from_uri(**profile)
    # TODO Recognize 'direct' profile.
