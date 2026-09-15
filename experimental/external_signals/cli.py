from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import ExternalSignalsPipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Attach historical price / BSR signals to cases")
    p.add_argument("--cases", type=Path, required=True)
    p.add_argument("--case-shelf", type=Path, required=True)
    p.add_argument("--external-history", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    return p


def main() -> None:
    a = build_parser().parse_args()
    worker = ExternalSignalsPipeline(
        a.cases, a.case_shelf, a.external_history, a.output_dir
    )
    try:
        result = worker.run()
    finally:
        worker.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
