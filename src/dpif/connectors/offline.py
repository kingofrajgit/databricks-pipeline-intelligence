"""Offline fixture-backed connector. Explicitly NOT live data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpif.connectors.base import DatabricksConnector


class OfflineDatabricksConnector(DatabricksConnector):
    """Serve fixture metadata from ``tests/fixtures/databricks`` and ``metadata``."""

    def __init__(self, fixtures_dir: str | Path | None = None) -> None:
        self.fixtures_dirs: list[Path] = []
        if fixtures_dir:
            p = Path(fixtures_dir)
            self.fixtures_dirs.append(p)
            for cand in (
                p.parent / "databricks",
                p.parent / "metadata",
                p / "databricks",
                p / "metadata",
            ):
                if cand.is_dir() and cand not in self.fixtures_dirs:
                    self.fixtures_dirs.append(cand)
        else:
            here = Path(__file__).resolve()
            for parent in [here.parent, *here.parents]:
                c_db = parent / "tests" / "fixtures" / "databricks"
                c_md = parent / "tests" / "fixtures" / "metadata"
                c_rt = parent / "tests" / "fixtures" / "runtime"
                if c_db.is_dir():
                    self.fixtures_dirs.append(c_db)
                if c_md.is_dir():
                    self.fixtures_dirs.append(c_md)
                if c_rt.is_dir():
                    self.fixtures_dirs.append(c_rt)
                if (parent / "pyproject.toml").exists():
                    break
        if not self.fixtures_dirs:
            self.fixtures_dirs = [
                Path("tests/fixtures/databricks"),
                Path("tests/fixtures/metadata"),
                Path("tests/fixtures/runtime"),
            ]
        self.source = "fixture (not a live Databricks API response)"

    def _load(self, name: str) -> dict[str, Any]:
        for base in self.fixtures_dirs:
            for suffix in (".json", ".yaml", ".yml"):
                p = base / f"{name}{suffix}"
                if p.exists():
                    if suffix == ".json":
                        return json.loads(p.read_text(encoding="utf-8"))
                    import yaml

                    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        return {}

    def _find_fixture(self, prefix: str, identifier: str) -> dict[str, Any] | None:
        raw = str(identifier).strip()
        stem = raw.removesuffix(".json").removesuffix(".yaml").removesuffix(".yml")
        candidates = [
            raw,
            stem,
            f"{prefix}_{stem}",
        ]
        if stem.startswith(f"{prefix}_"):
            candidates.append(stem[len(prefix) + 1 :])

        for cand in candidates:
            res = self._load(cand)
            if res:
                return res
        return None

    def get_job(self, job_id: int | str) -> dict[str, Any]:
        data = self._find_fixture("job", str(job_id))
        if not data:
            data = self._load("job_default")
        return {**data, "_connector": "offline-fixture", "evidence_source": "FIXTURE"}

    def get_cluster(self, cluster_id: str) -> dict[str, Any]:
        data = self._find_fixture("cluster", str(cluster_id))
        if not data:
            data = self._load("cluster_default")
        res = {**data, "_connector": "offline-fixture", "evidence_source": "FIXTURE"}
        if "cluster_id" not in res:
            res["cluster_id"] = str(cluster_id)
        return res

    def get_cluster_policy(self, policy_id: str) -> dict[str, Any] | None:
        data = self._find_fixture("policy", str(policy_id))
        if not data and policy_id in ("standard", "default", ""):
            data = self._load("policy_standard")
        if not data:
            return None
        return {**data, "_connector": "offline-fixture", "evidence_source": "FIXTURE"}

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        data = self._find_fixture("pipeline", str(pipeline_id))
        if not data and pipeline_id in ("good", "default", ""):
            data = self._load("pipeline_good")
        if not data:
            return None
        return {**data, "_connector": "offline-fixture", "evidence_source": "FIXTURE"}

    def get_permissions(self, object_type: str, object_id: str) -> dict[str, Any] | None:
        data = self._find_fixture("permissions", str(object_id)) or self._find_fixture(
            "permissions", str(object_type)
        )
        if not data and (
            object_id in ("prod", "default", "") or object_type in ("prod", "default")
        ):
            data = self._load("permissions_prod")
        if not data:
            return None
        return {**data, "_connector": "offline-fixture", "evidence_source": "FIXTURE"}

    def get_table_profile(self, table: str) -> dict[str, Any]:
        safe = table.replace(".", "_").replace("/", "_")
        data = self._load(f"table_{safe}") or self._load("table_default")
        return {**data, "_connector": "offline-fixture", "evidence_source": "FIXTURE"}

    def get_recent_runs(self, job_id: int | str, limit: int = 10) -> list[dict[str, Any]]:
        data = self._load(f"runs_{job_id}")
        runs = data.get("runs", []) if isinstance(data, dict) else []
        return runs[:limit]

    def get_runtime_run(self, run_id: int | str) -> dict[str, Any] | None:
        data = (
            self._find_fixture("runtime_run", str(run_id))
            or self._find_fixture("run", str(run_id))
            or self._load(f"runtime/{run_id}")
            or self._load(f"runtime_run_{run_id}")
            or self._load(f"run_{run_id}")
        )
        if not data and str(run_id) in ("healthy", "default", ""):
            data = self._load("runtime/healthy_run") or self._load("runtime_run_default")
        if not data:
            return None
        return {**data, "_connector": "offline-fixture", "evidence_source": "FIXTURE"}

    def get_run(self, run_id: int | str) -> dict[str, Any] | None:
        return self.get_runtime_run(run_id)

    def get_workspace_status(self) -> dict[str, Any] | None:
        return {
            "host": "https://offline-fixture.cloud.databricks.com",
            "status": "offline-fixture",
            "_connector": "offline-fixture",
            "evidence_source": "FIXTURE",
        }

    def mode(self) -> str:
        return "offline-fixture"
