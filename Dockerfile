FROM node:16-alpine AS web_frontend_builder
WORKDIR /code
COPY web-frontend .
RUN npm install && npm run build

# We cannot upgrade to Python 3.11 until numba supports it.
# The `sparse` library relies on numba.
FROM python:3.12-slim as builder

# We need git at build time in order for versioneer to work, which in turn is
# needed for the server to correctly report the library_version in the /api/v1/
# route.
# We need gcc to compile thriftpy2, a secondary dependency.
RUN apt-get -y update && apt-get install -y git gcc

WORKDIR /code

# Ensure logs and error messages do not get stuck in a buffer.
ENV PYTHONUNBUFFERED=1

# Use a venv to avoid interfering with system Python.
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
# This is equivalent to `source $VIRTUAL_ENV/bin/activate` but it
# persists into the runtime so we avoid the need to account for it
# in ENTRYPOINT or CMD.
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install build dependencies.
RUN pip install --no-cache-dir cython

COPY --from=web_frontend_builder /code/dist /code/share/tiled/ui
COPY . .

# Skip building the UI here because we already did it in the stage
# above using a node container.
RUN TILED_BUILD_SKIP_UI=1 pip install '.[server]'

# FROM base as test
#
# RUN pip install '.[client,dev]'
# RUN pytest -v

FROM python:3.12-slim as runner

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

WORKDIR /deploy
RUN mkdir /deploy/config
RUN mkdir -p /storage
COPY ./example_configs/single_catalog_single_user.yml /deploy/config
ENV TILED_CONFIG=/deploy/config

EXPOSE 8000

CMD ["tiled", "serve", "config", "--host", "0.0.0.0", "--port", "8000", "--scalable"]
