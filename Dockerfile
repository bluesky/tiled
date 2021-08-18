FROM python:3.8-slim as base

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONUNBUFFERED=1

FROM base as builder

WORKDIR /build

COPY ./docker/requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

RUN pip install --use-feature=in-tree-build './[server,formats]'

FROM base as app

COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

COPY ./docker/gunicorn_config.py /config/gunicorn_config.py

WORKDIR /deploy

COPY ./docker/check_config.py ./docker/entrypoint.sh ./

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
