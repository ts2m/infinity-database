"""Command-line interface for running dataflow pipelines."""

from __future__ import annotations
import argparse
from .core.pipeline import run_from_config

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("-c","--config", required=True)
    args = ap.parse_args()
    if args.cmd == "run":
        run_from_config(args.config)

if __name__ == "__main__":
    main()
