"""Repository smoke tests: the monorepo package layout resolves as AGENTS.md documents."""

from __future__ import annotations

import importlib

import pytest

PACKAGES: tuple[str, ...] = (
    "apps",
    "apps.api",
    "apps.api.app",
    "apps.worker",
    "apps.worker.worker",
    "pipelines",
    "pipelines.ingestion",
    "pipelines.ml",
    "pipelines.replay",
)


@pytest.mark.parametrize("module_name", PACKAGES)
def test_package_is_importable(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert module.__doc__, f"{module_name} must carry a descriptive module docstring"
