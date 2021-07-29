FROM python:3.8 as base

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

FROM base as builder

WORKDIR /install

COPY . .

RUN pip install --upgrade pip
RUN pip install -r requirements-server.txt
RUN pip install -r requirements-array.txt
RUN pip install -r requirements-dataframe.txt
RUN pip install -r requirements-xarray.txt
RUN pip install --use-feature=in-tree-build '.[server, array, dataframe, xarray]'

FROM base as app

COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

WORKDIR /app

ENTRYPOINT ["tiled"]
