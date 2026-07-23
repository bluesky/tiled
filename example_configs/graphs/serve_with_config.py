from __future__ import annotations

import argparse

import uvicorn
from sqlalchemy import Column, Integer, Table

from tiled.config import parse_configs
from tiled.graph.store import _metadata
from tiled.server.app import build_app_from_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--api-key", default="secret")
    parser.add_argument("catalog", nargs="?", default=None)
    args = parser.parse_args()

    # Ensure graph metadata can resolve entities.node_id foreign key.
    Table(
        "nodes",
        _metadata,
        Column("id", Integer, primary_key=True),
        extend_existing=True,
    )

    config = parse_configs(args.config)
    app = build_app_from_config(config)

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
