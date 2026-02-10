docs:
  rm -rf docs/build && rm -rf docs/source/reference/generated && uv run --all-extras --group docs sphinx-autobuild -W --ignore docs/source/reference/client-profiles.md --ignore docs/source/reference/api.yml --ignore docs/source/reference/service-configuration.md docs/source docs/build/html
