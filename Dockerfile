# syntax=docker/dockerfile:1.9
ARG PYTHON_VERSION=3.12
FROM docker.io/node:22-alpine AS web_frontend_build
WORKDIR /src
COPY web-frontend .
RUN set -ex && npm install && npm run build

##########################################################################

FROM docker.io/ubuntu:noble AS build

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

# Synchronize DEPENDENCIES without the application itself.
# This layer is cached until uv.lock or pyproject.toml change, which are
# only temporarily mounted into the build container since we don't need
# them in the production one.
# You can create `/app` using `uv venv` in a separate `RUN`
# step to have it cached, but with uv it's so fast, it's not worth
# it, so we let `uv sync` create it for us automagically.
RUN --mount=type=cache,target=/root/.cache \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    set -ex && \
    uv sync \
	--extra server \
        --locked \
        --no-dev \
        --no-install-project

# Now install the rest from `/src`: The APPLICATION w/o dependencies.
# `/src` will NOT be copied into the runtime container.
# LEAVE THIS OUT if your application is NOT a proper Python package.
COPY . /src
WORKDIR /src
RUN --mount=type=cache,target=/root/.cache \
    set -ex && \
    uv sync \
	--extra server \
        # We want as httpie as a developer convenience.
	--with httpie \
        --locked \
        --no-dev \
        --no-editable


##########################################################################

##########################################################################

FROM docker.io/ubuntu:noble
SHELL ["sh", "-exc"]

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
    python${PYTHON_VERSION} && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy the pre-built `/app` directory to the runtime container
# and change the ownership to user app and group app in one step.
COPY --from=build --chown=app:app /app /app
COPY --from=web_frontend_build --chown=app:app /src/dist /src/share/tiled/ui

USER app
WORKDIR /app

# Smoke test that the application can, in fact, be imported.
RUN set -ex && \
python -V && \
python -Im site && \
python -Ic 'import tiled'

RUN mkdir /deploy/config
RUN mkdir -p /storage
COPY ./example_configs/single_catalog_single_user.yml /deploy/config
ENV TILED_CONFIG=/deploy/config

EXPOSE 8000

CMD ["tiled", "serve", "config", "--host", "0.0.0.0", "--port", "8000", "--scalable"]
