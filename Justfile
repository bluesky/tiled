docs:
  rm -rf docs/build && rm -rf docs/source/reference/generated && uv run --all-extras --group docs sphinx-autobuild -W docs/source docs/build/html
