FROM python:3.8-slim as base

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONUNBUFFERED=1

FROM base as builder

WORKDIR /app

COPY docker/requirements.txt /tmp/
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt

COPY . .

RUN pip install -e '.[server, formats]'

FROM builder as test
RUN pip install -r docker/requirements-test.txt
RUN pytest -v

FROM base as app

COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY --from=builder /app /app

WORKDIR /deploy

RUN mkdir /deploy/prometheus
ENV PROMETHEUS_MULTIPROC_DIR=/deploy/prometheus

EXPOSE 8000

ENTRYPOINT ["/app/docker/entrypoint.sh"]
