bind = "0.0.0.0:8000"
workers = 4
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
timeout = 60
keepalive = 2
wsgi_app = "tiled.server.app:app_factory()"
worker_tmp_dir = "/dev/shm"
errorlog = "-"
accesslog = "-"
loglevel = "debug"
capture_output = True


# https://github.com/prometheus/client_python/#multiprocess-mode-eg-gunicorn
import prometheus_client.multiprocess


def child_exit(server, worker):
    prometheus_client.multiprocess.mark_process_dead(worker.pid)
