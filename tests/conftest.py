"""Shared pytest fixtures: repo paths, loaded rules, sample models."""

from __future__ import annotations

from pathlib import Path

import pytest

import dpif.config as config_mod
from dpif.models import Source, Target
from dpif.rules.engine import load_rules


@pytest.fixture(autouse=True)
def isolate_local_dotenv(monkeypatch: pytest.MonkeyPatch):
    """Ensure automated tests are hermetic and isolated from local developer .env files."""
    orig_load_env_file = config_mod.load_env_file

    def safe_load_env_file(dotenv_path=None, override=False):
        if dotenv_path is None:
            return None
        return orig_load_env_file(dotenv_path=dotenv_path, override=override)

    monkeypatch.setattr(config_mod, "load_env_file", safe_load_env_file)
    for var in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_JOB_ID",
        "DATABRICKS_CLUSTER_ID",
        "DATABRICKS_RUN_ID",
        "DATABRICKS_PIPELINE_ID",
        "DPIF_DATABRICKS_HOST",
        "DPIF_DATABRICKS_TOKEN",
        "DPIF_DATABRICKS_JOB_ID",
        "DPIF_DATABRICKS_CLUSTER_ID",
        "DPIF_DATABRICKS_RUN_ID",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return here.parent


@pytest.fixture(scope="session")
def rules_dir(repo_root: Path) -> Path:
    return repo_root / "rules"


@pytest.fixture(scope="session")
def all_rules():
    rules = load_rules()
    assert len(rules) >= 5, "expected at least 5 loaded rules"
    return rules


@pytest.fixture(scope="session")
def rules_by_id(all_rules):
    return {r.rule_id: r for r in all_rules}


@pytest.fixture()
def sample_source() -> Source:
    return Source(source_id="source-001", name="customer-source", type="adls", format="parquet")


@pytest.fixture()
def sample_target() -> Target:
    return Target(target_id="target-001", type="delta", catalog="prod", schema="marts")


@pytest.fixture()
def fixtures_dir(repo_root: Path) -> Path:
    return repo_root / "tests" / "fixtures"


@pytest.fixture()
def contracts_dir(fixtures_dir: Path) -> Path:
    return fixtures_dir / "contracts"


@pytest.fixture()
def metadata_dir(fixtures_dir: Path) -> Path:
    return fixtures_dir / "metadata"


@pytest.fixture()
def code_dir(fixtures_dir: Path) -> Path:
    return fixtures_dir / "code"
