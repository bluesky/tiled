FROM node:16-alpine AS web_builder
WORKDIR /app
COPY web-frontend .
RUN npm install && npm run build

FROM python:3.9-slim as base

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONUNBUFFERED=1

FROM base as builder
WORKDIR /app

# for versioneer
RUN apt-get update && apt-get install -y git

# copy requirements over first so this layer is cached and we don't have to reinstall deps when editing src
COPY requirements-server.txt requirements-formats.txt requirements-dataframe.txt requirements-array.txt requirements-xarray.txt requirements-sparse.txt requirements-compression.txt docker/requirements-docker.txt /tmp/
RUN pip install --upgrade pip
RUN pip install \
  -r /tmp/requirements-server.txt \
  -r /tmp/requirements-formats.txt \
  -r /tmp/requirements-dataframe.txt \
  -r /tmp/requirements-array.txt \
  -r /tmp/requirements-xarray.txt \
  -r /tmp/requirements-sparse.txt \
  -r /tmp/requirements-compression.txt \
  -r /tmp/requirements-docker.txt

COPY --from=web_builder /app/build /app/share/tiled/ui
COPY . .

# note requirements listed here but all deps should be already satisfied
RUN pip install '.[server, formats, dataframe, array, xarray, compression, sparse]'

FROM builder as test

RUN pip install '.[client]'
RUN pip install -r requirements-dev.txt
RUN pytest -v

FROM base as app

WORKDIR /deploy

COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY --from=builder /app/docker/gunicorn_config.py /app/docker/entrypoint.sh /app/docker/check_config.py /app/

EXPOSE 8000

ENTRYPOINT ["/app/entrypoint.sh"]
