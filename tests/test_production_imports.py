from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DIRS = (
    "data_prep",
    "population_scan",
    "market_discovery",
    "market_build",
    "case_build",
    "scripts",
    "configs",
)


def test_core_production_imports() -> None:
    from market_build.pipeline import MarketBuildPipeline
    from case_build.focal_selection import FocalSelectionPipeline
    from case_build.pipeline import CaseShelfBuilder
    from case_build.ground_truth.pipeline import GroundTruthPipeline
    from case_build.quality.pipeline import CaseQualityPipeline

    assert MarketBuildPipeline is not None
    assert FocalSelectionPipeline is not None
    assert CaseShelfBuilder is not None
    assert GroundTruthPipeline is not None
    assert CaseQualityPipeline is not None

    packager = ROOT / "scripts" / "package_market_cases.py"
    source = packager.read_text(encoding="utf-8")
    assert "class MarketCasePackager" in source


def test_production_python_does_not_import_legacy_or_experimental() -> None:
    banned = ("legacy", "experimental")
    offenders: list[str] = []
    for folder in PRODUCTION_DIRS:
        root = ROOT / folder
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                names: list[str] = []
                if isinstance(node, ast.Import):
                    names = [alias.name.split(".")[0] for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module.split(".")[0]]
                if any(name in banned for name in names):
                    offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []
