"""
Reproduce: Tiled serves a stale (truncated) view of an HDF5 dataset that a
SWMR writer has grown and flushed -- e.g. the file has 100 rows, Tiled returns 70.

Each test file is registered in Tiled TWO ways:

  <name>_file  -> the whole file, as a *container* node (what `tiled serve directory`
                  does). Children are discovered from the file on every request.
  <name>_ds    -> the dataset itself, as an *array* node (what `.register(...,
                  parameters={"dataset": ...})`, ingestors, etc. produce). Its shape
                  is stored in the catalog at registration time.

A writer then appends rows (flushing after every append, as a correct SWMR writer
should) while reader threads poll Tiled. At the end, every view is compared with
the final, flushed length on disk.

Usage:
    python repro_hdf5_stale.py [--files 6] [--initial 70] [--final 100] [--readers 4]
"""

import argparse
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from pathlib import Path

import h5py
import httpx
import numpy

from tiled.client import from_uri

API_KEY = "secret"
NCOLS = 3


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(directory, port, log_path):
    tiled = Path(sys.executable).with_name("tiled")
    env = dict(os.environ, TILED_HDF5_SWMR_DEFAULT="1")
    proc = subprocess.Popen(
        [
            str(tiled),
            "serve",
            "catalog",
            "--temp",
            "-r",
            str(directory),
            f"--api-key={API_KEY}",
            f"--port={port}",
        ],
        env=env,
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
    )
    url = f"http://127.0.0.1:{port}"
    for _ in range(120):
        if proc.poll() is not None:
            sys.exit(f"Server exited early; see {log_path}")
        try:
            if httpx.get(f"{url}/healthz").status_code == 200:
                return proc, url
        except httpx.TransportError:
            pass
        time.sleep(0.5)
    proc.terminate()
    sys.exit(f"Server did not start; see {log_path}")


def tiled_len(client, key, view):
    """Length of the dataset as Tiled reports it, via a FRESH lookup (no stale client node)."""
    node = client[key]["data"] if view == "file" else client[key]
    advertised = node.shape[0]
    actual = node.read().shape[0]
    return advertised, actual


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--files", type=int, default=6)
    p.add_argument("--initial", type=int, default=70, help="rows when registered")
    p.add_argument("--final", type=int, default=100, help="rows after writing")
    p.add_argument("--readers", type=int, default=4, help="concurrent polling threads")
    p.add_argument(
        "--settle", type=float, default=2.0, help="seconds to wait after last flush"
    )
    p.add_argument("--keep", action="store_true", help="keep temp dir and server log")
    args = p.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="tiled-swmr-"))
    log_path = workdir / "server.log"
    print(f"workdir: {workdir}")

    # ---- writer: create files and switch to SWMR ------------------------------------
    names = [f"f{i}" for i in range(args.files)]
    files, dsets = {}, {}
    flushed = {}  # name -> rows durably visible to SWMR readers
    for name in names:
        f = h5py.File(workdir / f"{name}.h5", "w", libver="latest")
        ds = f.create_dataset(
            "data",
            data=numpy.arange(args.initial * NCOLS, dtype="f8").reshape(-1, NCOLS),
            maxshape=(None, NCOLS),
            chunks=(10, NCOLS),
        )
        f.swmr_mode = True
        f.flush()
        files[name], dsets[name], flushed[name] = f, ds, args.initial

    proc, url = start_server(workdir, free_port(), log_path)
    try:
        client = from_uri(url, api_key=API_KEY)
        for name in names:
            path = workdir / f"{name}.h5"
            client.register(path, key=f"{name}_file")  # container
            client.register(
                path, key=f"{name}_ds", parameters={"dataset": "data"}
            )  # array
        print(f"registered {len(names)} files x 2 views at {args.initial} rows\n")

        # ---- concurrent readers while writing --------------------------------------
        stop = threading.Event()
        errors = defaultdict(list)
        polls = defaultdict(int)

        def reader():
            c = from_uri(url, api_key=API_KEY)
            while not stop.is_set():
                name, view = random.choice(names), random.choice(["file", "ds"])
                try:
                    tiled_len(c, f"{name}_{view}", view)
                    polls[view] += 1
                except Exception as e:  # noqa: BLE001
                    errors[view].append(f"{type(e).__name__}: {str(e)[:120]}")

        threads = [
            threading.Thread(target=reader, daemon=True) for _ in range(args.readers)
        ]
        for t in threads:
            t.start()

        # ---- writer: grow every file in small batches, flushing each time ------------
        while any(flushed[n] < args.final for n in names):
            name = random.choice([n for n in names if flushed[n] < args.final])
            ds = dsets[name]
            old = flushed[name]
            new = min(old + random.randint(1, 7), args.final)
            ds.resize((new, NCOLS))
            rows = numpy.arange(old * NCOLS, new * NCOLS).reshape(-1, NCOLS)
            ds[old:new] = rows
            ds.flush()
            flushed[name] = new
            time.sleep(random.uniform(0, 0.05))

        time.sleep(args.settle)
        stop.set()
        for t in threads:
            t.join()

        # ---- final verdict ----------------------------------------------------------
        print(
            f"{'node':<10} {'view':<10} {'on disk':>8} {'advertised':>11} {'served':>7}  result"
        )
        stale = 0
        fresh = from_uri(url, api_key=API_KEY)
        for name in names:
            for view, label in (("file", "container"), ("ds", "array")):
                try:
                    adv, got = tiled_len(fresh, f"{name}_{view}", view)
                    ok = got == flushed[name]
                    res = "ok" if ok else f"STALE ({flushed[name] - got} rows missing)"
                except Exception as e:  # noqa: BLE001
                    adv = got = "-"
                    ok, res = False, f"ERROR {type(e).__name__}"
                stale += not ok
                print(
                    f"{name:<10} {label:<10} {flushed[name]:>8} {adv!s:>11} {got!s:>7}  {res}"
                )

        print(f"\npolls during writing: {dict(polls)}")
        for view, errs in errors.items():
            print(f"errors during writing ({view}): {len(errs)}, e.g. {errs[0]}")
        print(
            f"\n{stale} of {2 * len(names)} views are stale/erroring after all data was flushed."
        )
        return 1 if stale else 0
    finally:
        proc.terminate()
        proc.wait()
        for f in files.values():
            f.close()
        if args.keep:
            print(f"kept {workdir}")
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
