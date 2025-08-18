# syntax=docker/dockerfile:1.9
ARG PYTHON_VERSION=3.12
FROM docker.io/node:22-alpine AS web_frontend_build
WORKDIR /src
COPY web-frontend .
RUN set -ex && npm install && npm run build

##########################################################################

# This stage doubles as setting up for the build and as the devcontainer
FROM docker.io/python:${PYTHON_VERSION} AS developer
ARG PYTHON_VERSION=3.12

# Ensure apt-get doesn't open a menu on you.
ENV DEBIAN_FRONTEND=noninteractive

# Ensure logs and error messages do not get stuck in a buffer.
ENV PYTHONUNBUFFERED=1

RUN set -ex && \
apt-get update -qy && \
apt-get install -qyy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    build-essential \
    ca-certificates \
    gcc

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# - Silence uv complaining about not being able to use hard links,
# - tell uv to byte-compile packages for faster application startups,
# - prevent uv from accidentally downloading isolated Python builds,
# - pick a Python (use `/usr/bin/python3.12` on uv 0.5.0 and later),
# - and finally declare `/app` as the target for `uv sync`.
# - Skip building the UI here because we already did it in the stage
#   above using a node container.
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python${PYTHON_VERSION} \
    UV_PROJECT_ENVIRONMENT=/app \
    TILED_BUILD_SKIP_UI=1

# Using a subdirectory as the workdir allows mounting source of
# dependencies into the devcontainer when they are sibling
# directories to the local checkout
WORKDIR /workspaces/tiled

# Synchronize DEPENDENCIES without the application itself.
# This layer is cached until the build changes: changes to the
# application will run require rerunning this step.
COPY pyproject.toml hatch_build.py README.md .
RUN set -ex && \
    uv sync \
        --extra server \
        --no-dev \
        --no-install-project

# Now install the rest from `./src`: The APPLICATION w/o dependencies.
# `./src` will NOT be copied into the runtime container.
COPY . src

##########################################################################

FROM developer AS app_build
RUN set -ex && \
    uv sync \
        --project src \
        --extra server \
        # Add httpie as a developer convenience.
        # --with httpie \
        --no-dev \
        --no-editable

##########################################################################

FROM docker.io/python:${PYTHON_VERSION}-slim AS app_runtime
ARG PYTHON_VERSION=3.12

# Add the application virtualenv to search path.
ENV PATH=/app/bin:$PATH

# Don't run your app as root.
RUN set -ex && \
groupadd -r app && \
useradd -r -d /app -g app -N app

# See <https://hynek.me/articles/docker-signals/>.
STOPSIGNAL SIGINT

# Note how the runtime dependencies differ from build-time ones.
# Notably, there is no uv either!
RUN set -ex && \
apt-get update -qy && \
apt-get install -qyy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    curl && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /deploy/config
RUN mkdir -p /storage && chown -R app:app /storage
COPY ./example_configs/single_catalog_single_user.yml /deploy/config
ENV TILED_CONFIG=/deploy/config

# Copy the pre-built `/app` directory to the runtime container
# and change the ownership to user app and group app in one step.
COPY --from=app_build --chown=app:app /app /app
COPY --from=web_frontend_build --chown=app:app /src/dist /src/share/tiled/ui

USER app
WORKDIR /app

# Smoke test that the application can, in fact, be imported.
RUN set -ex && \
python -V && \
python -Im site && \
python -Ic 'import tiled'

EXPOSE 8000

CMD ["tiled", "serve", "config", "--host", "0.0.0.0", "--port", "8000", "--scalable"]
