#!/bin/bash
set -vxeuo pipefail

# Start LDAP (in docker)
source start_LDAP.sh

# These packages are installed in the base environment but may be older
# versions. Explicitly upgrade them because they often create
# installation problems if out of date.
python -m pip install --upgrade pip setuptools wheel numpy
# Versioneer uses the most recent git tag to generate __version__, which appears
# in the published documentation and is declared by the client when it
# connects to a server.
git fetch --tags --unshallow
python -m pip install '.[all]'
python -m pip list
