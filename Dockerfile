FROM python:3.8 as base

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

FROM base as builder

WORKDIR /build

COPY . .

RUN pip install --upgrade pip
RUN pip install gunicorn
RUN pip install --use-feature=in-tree-build '.[server, array, dataframe, xarray]'

FROM base as app

COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

WORKDIR /deploy

COPY gunicorn_config.py .
EXPOSE 8000

ENTRYPOINT ["gunicorn", "--config", "gunicorn_config.py"]
