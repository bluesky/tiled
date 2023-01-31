# to build locally, use:
#  docker build -t tiled . -f ./docker/Dockerfile

FROM node:16-alpine AS web_frontend_builder
WORKDIR /code
COPY web-frontend .
RUN npm install && npm run build

# We cannot upgrade to Python 3.11 until numba supports it.
# The `sparse` library relies on numba.
FROM python:3.10 as base
ENV PYTHONUNBUFFERED=1
WORKDIR /code

# Copy requirements over first so this layer is cached
# and we don't have to reinstall deps when only the tiled
# source has changed.
COPY requirements-server.txt requirements-formats.txt requirements-dataframe.txt requirements-array.txt requirements-xarray.txt requirements-sparse.txt requirements-compression.txt /code/
RUN pip install --upgrade --no-cache-dir pip wheel
RUN pip install --upgrade --no-cache-dir \
  -r /code/requirements-server.txt \
  -r /code/requirements-formats.txt \
  -r /code/requirements-dataframe.txt \
  -r /code/requirements-array.txt \
  -r /code/requirements-xarray.txt \
  -r /code/requirements-sparse.txt \
  -r /code/requirements-compression.txt

COPY --from=web_frontend_builder /code/build /code/share/tiled/ui
COPY . .

# note requirements listed here but all deps should be already satisfied
RUN pip install '.[server, formats, dataframe, array, xarray, compression, sparse]'

# FROM base as test
#
# RUN pip install '.[client]'
# RUN pip install -r requirements-dev.txt
# RUN pytest -v

FROM base as app

WORKDIR /deploy

COPY ./docker/entrypoint /usr/local/bin/entrypoint
COPY ./docker/check_config.py /usr/local/bin/check_config.py
RUN chmod +x /usr/local/bin/entrypoint
RUN chmod +x /usr/local/bin/check_config.py
ENTRYPOINT ["/usr/local/bin/entrypoint"]

EXPOSE 8000

CMD ["tiled", "serve", "config", "--host", "0.0.0.0", "--port", "8000"]
