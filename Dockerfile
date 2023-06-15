FROM node:16-alpine AS web_frontend_builder
WORKDIR /code
COPY web-frontend .
RUN npm install && npm run build

# We cannot upgrade to Python 3.11 until numba supports it.
# The `sparse` library relies on numba.
FROM python:3.10-slim as builder

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

# Copy requirements over first so this layer is cached and we don't have to
# reinstall dependencies when only the tiled source has changed.
COPY requirements-server.txt requirements-formats.txt requirements-dataframe.txt requirements-array.txt requirements-xarray.txt requirements-sparse.txt requirements-compression.txt /code/
RUN pip install --upgrade --no-cache-dir cython pip wheel
RUN pip install --upgrade --no-cache-dir \
  -r /code/requirements-array.txt \
  -r /code/requirements-compression.txt \
  -r /code/requirements-dataframe.txt \
  -r /code/requirements-formats.txt \
  -r /code/requirements-server.txt \
  -r /code/requirements-sparse.txt \
  -r /code/requirements-xarray.txt

COPY --from=web_frontend_builder /code/build /code/share/tiled/ui
COPY . .

# note requirements listed here but all deps should be already satisfied
RUN pip install '.[array, compression, dataframe, formats, server, sparse, xarray]'

# FROM base as test
#
# RUN pip install '.[client]'
# RUN pip install -r requirements-dev.txt
# RUN pytest -v

FROM python:3.10-slim as runner

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

WORKDIR /deploy

EXPOSE 8000

CMD ["tiled", "serve", "config", "--host", "0.0.0.0", "--port", "8000", "--scalable", "/deploy"]
