FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY ./ /app
WORKDIR /app
RUN pip install -U pip
RUN pip install -e .[server,xarray,dataframe,xarray]

# CMD  tiled serve directory /data