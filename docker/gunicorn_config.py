import sys
import prometheus_client.multiprocess


def child_exit(server, worker):
    # https://github.com/prometheus/client_python/#multiprocess-mode-eg-gunicorn
    try:
        prometheus_client.multiprocess.mark_process_dead(worker.pid)
    except TypeError:
        """Prints a nice error in a race condition where gunicorn tries to
        shut down a worker's child prometheus client but it was already shut down."""
        print(
            "Gunicorn worker process failed to stop prometheus client."
            "This is likely because of a previous error in starting up the "
            "tiled worker.",
            file=sys.stderr,
        )


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
