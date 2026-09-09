"""PySpark structural detectors over CodeAnalysis (Phase 4).

Every detector returns plain result dicts (``triggered`` + observed/expected/
recommendation/confidence/assumptions); evaluators turn them into Findings.
Static analysis never claims runtime quantities — wording stays potential.
"""

from __future__ import annotations

from typing import Any

from dpif.code import flow as flow_mod
from dpif.code.models import CodeAnalysis, Operation, OperationType

SHUFFLE_OPS = frozenset(
    {
        OperationType.GROUP_BY,
        OperationType.DISTINCT,
        OperationType.DROP_DUPLICATES,
        OperationType.ORDER_BY,
        OperationType.REPARTITION,
        OperationType.WINDOW,
        OperationType.JOIN,
        OperationType.UNION,
    }
)


def _volume(context: dict[str, Any]) -> float | None:
    for key in ("data_size_gb", "table_size_gb", "expected_volume_gb"):
        value = context.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _base(observed: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    method = str(context.get("collection_method") or "unknown")
    source = str(context.get("evidence_source") or "code + profile metadata")
    return {"method": method, "evidence_source": source, **observed}


def analyze_driver_collect(
    analysis: CodeAnalysis,
    context: dict[str, Any],
    small_gb: float = 1.0,
    critical_gb: float = 500.0,
) -> list[dict[str, Any]]:
    """Per-COLLECT-op verdicts: unrestricted vs limit/filtered vs small data."""
    volume = _volume(context)
    loop_vars = {loop.get("variable") for loop in analysis.driver_loops}
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.COLLECT):
        limited = bool(op.context.get("limited"))
        filtered = bool(op.context.get("filtered"))
        limit_n = op.context.get("limit_n")
        observed = _base(
            {
                "operation": "collect()",
                "line": op.line,
                "dataframe": op.dataframe,
                "code": op.code,
                "limited": limited,
                "limit_n": limit_n,
                "filtered": filtered,
                "input_gb": volume,
            },
            context,
        )
        assumptions: dict[str, Any] = {}
        iterated = (
            op.dataframe in loop_vars
            if op.dataframe
            else any(loop.get("collected_via") == "collect" for loop in analysis.driver_loops)
        )
        if iterated:
            assumptions["driver-loop"] = "collected rows are iterated in Python"
        if volume is None:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.55,
                    "observed": observed,
                    "expected": {"bounded_collect_or_known_small_input": True},
                    "recommendation": (
                        "Driver-side collect() with UNKNOWN input size: bound it "
                        "(limit/filter) or establish the input is small."
                    ),
                    "assumptions": {**assumptions, "context-unavailable": "input size unknown"},
                    "op": op,
                }
            )
        elif limited or (volume < small_gb):
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.6,
                    "observed": observed,
                    "expected": {"input_gb": f"< {small_gb} or bounded"},
                    "recommendation": (
                        "Small/bounded collect(); keep the bound and avoid growing "
                        "the collected frame."
                    ),
                    "assumptions": assumptions,
                    "op": op,
                }
            )
        else:
            critical = volume >= critical_gb
            results.append(
                {
                    "triggered": True,
                    "level": "CRITICAL" if critical else "HIGH",
                    "confidence": 0.95 if iterated else 0.9,
                    "observed": observed,
                    "expected": {"driver_collect_input_gb": f"< {small_gb}"},
                    "recommendation": (
                        "Potential large driver-side collection: materializing "
                        f"{volume:.0f} GB on the driver risks memory pressure/OOM; "
                        "keep data distributed (write/aggregate) or bound the result."
                    ),
                    "assumptions": assumptions,
                    "op": op,
                }
            )
    return results


def analyze_topandas(
    analysis: CodeAnalysis,
    context: dict[str, Any],
    small_gb: float = 1.0,
    critical_gb: float = 500.0,
) -> list[dict[str, Any]]:
    """toPandas() evaluated against data evidence; bounded use is calmer."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.TO_PANDAS):
        limited = bool(op.context.get("limited"))
        observed = _base(
            {
                "operation": "toPandas()",
                "line": op.line,
                "dataframe": op.dataframe,
                "code": op.code,
                "limited": limited,
                "limit_n": op.context.get("limit_n"),
                "input_gb": volume,
            },
            context,
        )
        if limited:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.6,
                    "observed": observed,
                    "expected": {"bounded_frame": True},
                    "recommendation": (
                        "Bounded toPandas(); confirm the bound holds as data grows."
                    ),
                    "assumptions": {},
                    "op": op,
                }
            )
        elif volume is None:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.55,
                    "observed": observed,
                    "expected": {"known_small_input": True},
                    "recommendation": (
                        "toPandas() with UNKNOWN input size: establish the frame "
                        "is small or bound it first."
                    ),
                    "assumptions": {"context-unavailable": "input size unknown"},
                    "op": op,
                }
            )
        elif volume < small_gb:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.6,
                    "observed": observed,
                    "expected": {"input_gb": f"< {small_gb}"},
                    "recommendation": "Small-frame toPandas(); revisit if the input grows.",
                    "assumptions": {},
                    "op": op,
                }
            )
        else:
            critical = volume >= critical_gb
            results.append(
                {
                    "triggered": True,
                    "level": "CRITICAL" if critical else "HIGH",
                    "confidence": 0.9,
                    "observed": observed,
                    "expected": {"driver_frame_gb": f"< {small_gb} or bounded"},
                    "recommendation": (
                        f"Potential large toPandas() over ~{volume:.0f} GB: prefer "
                        "distributed writes/aggregates or Arrow-based bounded transfer."
                    ),
                    "assumptions": {},
                    "op": op,
                }
            )
    return results


def analyze_shuffle_risk(
    analysis: CodeAnalysis,
    context: dict[str, Any],
    shuffle_gb: float = 100.0,
    high_gb: float = 1000.0,
) -> list[dict[str, Any]]:
    """Potential (never proven) shuffle risk per shuffle-class operation."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.operations:
        if op.operation_type not in SHUFFLE_OPS:
            continue
        if op.operation_type == OperationType.JOIN:
            how = str(op.arguments.get("how", "inner")).lower()
            if how in ("broadcast",):
                continue
        observed = _base(
            {
                "operation": op.operation_type.value,
                "line": op.line,
                "dataframe": op.dataframe,
                "code": op.code,
                "input_gb": volume,
            },
            context,
        )
        if volume is None:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.5,
                    "observed": observed,
                    "expected": {"known_volume": True},
                    "recommendation": (
                        f"Potential shuffle risk from {op.operation_type.value.lower()}(); "
                        "size the operation once input volume is known."
                    ),
                    "assumptions": {"context-unavailable": "input size unknown"},
                    "op": op,
                }
            )
        elif volume >= shuffle_gb:
            results.append(
                {
                    "triggered": True,
                    "level": "HIGH" if volume >= high_gb else "MEDIUM",
                    "confidence": 0.7,
                    "observed": observed,
                    "expected": {"shuffle_sized": True},
                    "recommendation": (
                        f"Potential shuffle risk: {op.operation_type.value.lower()}() over "
                        f"~{volume:.0f} GB — confirm partitioning and skew handling; "
                        "static analysis cannot prove shuffle volume."
                    ),
                    "assumptions": {},
                    "op": op,
                }
            )
    return results


def analyze_repartition(
    analysis: CodeAnalysis, context: dict[str, Any], max_partitions: int = 2000
) -> list[dict[str, Any]]:
    """Repeated or oversized repartitioning (never 'repartition is bad')."""
    results: list[dict[str, Any]] = []
    by_flow: dict[str, list[Operation]] = {}
    for op in analysis.of_type(OperationType.REPARTITION):
        root = flow_mod.flow_root(analysis, op.dataframe or "<unknown>", op.line)
        by_flow.setdefault(root, []).append(op)
    for root, ops in by_flow.items():
        observed = _base(
            {
                "dataframe": ops[-1].dataframe or root,
                "flow_root": root,
                "repartition_count": len(ops),
                "lines": [o.line for o in ops],
            },
            context,
        )
        if len(ops) >= 2:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.7,
                    "observed": observed,
                    "expected": {"repartitions_per_frame": "<= 1"},
                    "recommendation": (
                        f"Potential excessive repartition: {len(ops)} repartition() calls on "
                        f"'{ops[-1].dataframe or root}'; each is a full shuffle — consolidate."
                    ),
                    "assumptions": {},
                    "op": ops[-1],
                }
            )
        for op in ops:
            raw_literals = op.arguments.get("literals", [])
            literals = raw_literals if isinstance(raw_literals, list) else []
            big = [n for n in literals if isinstance(n, (int, float)) and n > max_partitions]
            if big:
                results.append(
                    {
                        "triggered": True,
                        "level": "WARN",
                        "confidence": 0.7,
                        "observed": _base(
                            {
                                "dataframe": op.dataframe or root,
                                "line": op.line,
                                "partition_count": big[0],
                                "code": op.code,
                            },
                            context,
                        ),
                        "expected": {"partition_count": f"<= {max_partitions}"},
                        "recommendation": (
                            f"Unnecessarily large partition count ({big[0]}); "
                            "oversized parallelism adds task overhead."
                        ),
                        "assumptions": {},
                        "op": op,
                    }
                )
    return results


def analyze_coalesce(
    analysis: CodeAnalysis, context: dict[str, Any], large_gb: float = 100.0
) -> list[dict[str, Any]]:
    """coalesce(1)-style parallelism reduction on large inputs."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.COALESCE):
        raw_literals = op.arguments.get("literals", [])
        literals = raw_literals if isinstance(raw_literals, list) else []
        narrow = any(n == 1 for n in literals if isinstance(n, (int, float)))
        observed = _base(
            {
                "line": op.line,
                "dataframe": op.dataframe,
                "code": op.code,
                "literals": literals,
                "input_gb": volume,
            },
            context,
        )
        if narrow and (volume is None or volume >= large_gb):
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.7,
                    "observed": observed,
                    "expected": {"single_partition_write": "justified for small outputs"},
                    "recommendation": (
                        "Potential parallelism reduction: coalesce(1) forces a single "
                        "writer; justify for large outputs or keep parallelism."
                    ),
                    "assumptions": {}
                    if volume is not None
                    else {"context-unavailable": "input size unknown"},
                    "op": op,
                }
            )
    return results


def analyze_cache(analysis: CodeAnalysis, context: dict[str, Any]) -> list[dict[str, Any]]:
    """Unjustified cache()/persist() without demonstrated reuse."""
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.CACHE, OperationType.PERSIST):
        df = op.dataframe or "<unknown>"
        uses = flow_mod.downstream_uses(analysis, df, op.line) if op.dataframe else 0
        observed = _base(
            {
                "operation": op.operation_type.value,
                "line": op.line,
                "dataframe": df,
                "code": op.code,
                "downstream_uses": uses,
            },
            context,
        )
        if uses < 2:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.6,
                    "observed": observed,
                    "expected": {"reuses_after_cache": ">= 2"},
                    "recommendation": (
                        "Caching benefit is not demonstrated by static analysis: "
                        f"'{df}' is used ~{uses}x after {op.operation_type.value.lower()}(); "
                        "remove or prove reuse with runtime evidence."
                    ),
                    "assumptions": {"static-only": "reuse counted syntactically"},
                    "op": op,
                }
            )
    return results


def analyze_repeated_actions(
    analysis: CodeAnalysis, context: dict[str, Any]
) -> list[dict[str, Any]]:
    """Repeated actions on one frame that may recompute (no caching claim)."""
    results: list[dict[str, Any]] = []
    for df in analysis.dataframe_variables:
        actions = [
            op for op in analysis.operations_on(df) if op.operation_type in flow_mod.ACTION_TYPES
        ]
        if len(actions) >= 2:
            observed = _base(
                {
                    "dataframe": df,
                    "actions": [a.operation_type.value for a in actions],
                    "lines": [a.line for a in actions],
                },
                context,
            )
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.65,
                    "observed": observed,
                    "expected": {"actions_per_frame": 1},
                    "recommendation": (
                        f"Repeated Spark actions on '{df}' "
                        f"({', '.join(a.operation_type.value.lower() for a in actions)}) "
                        "may require additional computation; cache/persist only with "
                        "runtime evidence."
                    ),
                    "assumptions": {},
                    "op": actions[1],
                }
            )
    return results


def analyze_global_sort(
    analysis: CodeAnalysis, context: dict[str, Any], sort_gb: float = 100.0
) -> list[dict[str, Any]]:
    """Global orderBy/sort on large inputs: warning, never definite failure."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.ORDER_BY):
        observed = _base(
            {"line": op.line, "dataframe": op.dataframe, "code": op.code, "input_gb": volume},
            context,
        )
        if volume is None:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.5,
                    "observed": observed,
                    "expected": {"known_volume": True},
                    "recommendation": (
                        "Global ordering with UNKNOWN input size: confirm the sort "
                        "is necessary and bounded."
                    ),
                    "assumptions": {"context-unavailable": "input size unknown"},
                    "op": op,
                }
            )
        elif volume >= sort_gb:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.7,
                    "observed": observed,
                    "expected": {"global_sort_input_gb": f"< {sort_gb}"},
                    "recommendation": (
                        f"Potential performance warning: global sort over ~{volume:.0f} GB "
                        "forces a wide shuffle; prefer partitioned ordering where possible."
                    ),
                    "assumptions": {},
                    "op": op,
                }
            )
    return results


def analyze_windows(analysis: CodeAnalysis, context: dict[str, Any]) -> list[dict[str, Any]]:
    """Unbounded window specs are potentially expensive."""
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.WINDOW):
        bounded = op.arguments.get("bounded")
        observed = _base(
            {"line": op.line, "dataframe": op.dataframe, "code": op.code, "bounded": bounded},
            context,
        )
        if bounded is not True:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.65,
                    "observed": observed,
                    "expected": {"window_frame": "bounded (rowsBetween/rangeBetween)"},
                    "recommendation": (
                        "Potentially expensive window operation: add an explicit frame "
                        "bound (rowsBetween/rangeBetween) where semantics allow."
                    ),
                    "assumptions": {},
                    "op": op,
                }
            )
    return results


def analyze_dedup(
    analysis: CodeAnalysis, context: dict[str, Any], dedup_gb: float = 100.0
) -> list[dict[str, Any]]:
    """distinct()/dropDuplicates() cost risk on large inputs."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.DISTINCT, OperationType.DROP_DUPLICATES):
        observed = _base(
            {
                "operation": op.operation_type.value,
                "line": op.line,
                "dataframe": op.dataframe,
                "code": op.code,
                "input_gb": volume,
            },
            context,
        )
        if volume is None or volume >= dedup_gb:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.6,
                    "observed": observed,
                    "expected": {"dedup_input_gb": f"< {dedup_gb} or keyed"},
                    "recommendation": (
                        f"Potential cost/performance risk: {op.operation_type.value.lower()}() "
                        "shuffles on the full key set; confirm key cardinality."
                    ),
                    "assumptions": {}
                    if volume is not None
                    else {"context-unavailable": "input size unknown"},
                    "op": op,
                }
            )
    return results


def analyze_broadcast(
    analysis: CodeAnalysis, context: dict[str, Any], broadcast_max_gb: float = 10.0
) -> list[dict[str, Any]]:
    """Broadcast suitability from evidence; unknown size stays qualified."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.BROADCAST):
        observed = _base(
            {"line": op.line, "dataframe": op.dataframe, "code": op.code, "input_gb": volume},
            context,
        )
        if volume is None:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.45,
                    "observed": observed,
                    "expected": {"broadcast_input_gb": f"known < {broadcast_max_gb}"},
                    "recommendation": (
                        "Broadcast suitability cannot be established from available "
                        "evidence: confirm the broadcast side is small."
                    ),
                    "assumptions": {"context-unavailable": "broadcast input size unknown"},
                    "op": op,
                }
            )
        elif volume >= broadcast_max_gb * 10:
            results.append(
                {
                    "triggered": True,
                    "level": "HIGH",
                    "confidence": 0.75,
                    "observed": observed,
                    "expected": {"broadcast_input_gb": f"< {broadcast_max_gb}"},
                    "recommendation": (
                        f"Potentially dangerous broadcast over ~{volume:.0f} GB: "
                        "broadcast joins replicate the frame to every executor."
                    ),
                    "assumptions": {},
                    "op": op,
                }
            )
        elif volume >= broadcast_max_gb:
            results.append(
                {
                    "triggered": True,
                    "level": "WARN",
                    "confidence": 0.65,
                    "observed": observed,
                    "expected": {"broadcast_input_gb": f"< {broadcast_max_gb}"},
                    "recommendation": (
                        "Broadcast above the comfortable threshold: verify the "
                        "broadcast side, not just total input."
                    ),
                    "assumptions": {},
                    "op": op,
                }
            )
    return results


def analyze_udfs(analysis: CodeAnalysis, context: dict[str, Any]) -> list[dict[str, Any]]:
    """Python vs Pandas UDF classification with contextual severity."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.UDF, OperationType.PANDAS_UDF):
        kind = "pandas-udf" if op.operation_type == OperationType.PANDAS_UDF else "python-udf"
        observed = _base(
            {"kind": kind, "line": op.line, "code": op.code, "input_gb": volume}, context
        )
        if op.operation_type == OperationType.PANDAS_UDF:
            level, conf, rec = (
                "WARN",
                0.6,
                (
                    "Pandas UDF detected: acceptable for vectorized logic; prefer "
                    "native expressions where they suffice."
                ),
            )
        elif volume is not None and volume >= 100.0:
            level, conf, rec = (
                "HIGH",
                0.75,
                (
                    "Python UDF over a large input forces row-wise serialization; "
                    "prefer native Spark SQL/DataFrame expressions where appropriate."
                ),
            )
        else:
            level, conf, rec = (
                "WARN",
                0.55,
                (
                    "Python UDF detected: prefer native expressions where appropriate; "
                    "impact depends on input size."
                ),
            )
        results.append(
            {
                "triggered": True,
                "level": level,
                "confidence": conf,
                "observed": observed,
                "expected": {"native_expression_possible": "evaluate"},
                "recommendation": rec,
                "assumptions": {},
                "op": op,
            }
        )
    return results


def analyze_count_for_existence(
    analysis: CodeAnalysis,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Detect df.count() called solely to test emptiness/existence."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.COUNT):
        is_existence = bool(
            op.arguments.get("existence_check") or op.context.get("existence_check")
        )
        if not is_existence:
            continue
        observed = _base(
            {
                "operation": "count_for_existence",
                "line": op.line,
                "dataframe": op.dataframe,
                "code": op.code,
                "input_gb": volume,
            },
            context,
        )
        if volume is not None and volume >= 100.0:
            level, conf = "HIGH", 0.85
            rec = (
                f"df.count() used for existence check over large input (~{volume:.0f} GB): "
                "scans the entire dataset. Prefer df.isEmpty() or df.limit(1).count() > 0."
            )
        else:
            level, conf = "WARN", 0.8
            rec = (
                "df.count() used solely to test existence/emptiness forces a full "
                "distributed scan. Prefer df.isEmpty() or df.limit(1).count() > 0."
            )
        results.append(
            {
                "triggered": True,
                "level": level,
                "confidence": conf,
                "observed": observed,
                "expected": {"existence_check": "df.isEmpty() or df.limit(1).count() > 0"},
                "recommendation": rec,
                "assumptions": {
                    "static-analysis": "count() evaluated in boolean or comparison condition"
                },
                "op": op,
            }
        )
    return results


def analyze_cross_join(
    analysis: CodeAnalysis,
    context: dict[str, Any],
    cross_max_gb: float = 10.0,
) -> list[dict[str, Any]]:
    """Cartesian/cross join risk: unqualified or large volumes flagged."""
    volume = _volume(context)
    results: list[dict[str, Any]] = []
    for op in analysis.of_type(OperationType.JOIN):
        how = str(op.arguments.get("how", "")).lower().strip("'\"")
        if how != "cross":
            continue
        observed = _base(
            {
                "operation": "cross_join",
                "line": op.line,
                "dataframe": op.dataframe,
                "code": op.code,
                "how": how,
                "input_gb": volume,
            },
            context,
        )
        if volume is None:
            level, conf = "WARN", 0.6
            rec = (
                "Cartesian / cross join detected with unknown input volume: "
                "verify explicit intent and ensure inputs are small to avoid executor OOM."
            )
            assumptions = {"context-unavailable": "input size unknown"}
        elif volume >= cross_max_gb:
            level, conf = "HIGH", 0.8
            rec = (
                f"Dangerous cross join over ~{volume:.0f} GB: Cartesian product "
                "causes multiplicative row growth (O(N*M)) and catastrophic shuffle/OOM."
            )
            assumptions = {}
        else:
            level, conf = "WARN", 0.65
            rec = (
                f"Cross join on small/bounded input (~{volume:.1f} GB): verify explicit intent "
                "and ensure downstream operations filter results promptly."
            )
            assumptions = {}
        results.append(
            {
                "triggered": True,
                "level": level,
                "confidence": conf,
                "observed": observed,
                "expected": {"cross_join": "avoided or strictly bounded"},
                "recommendation": rec,
                "assumptions": assumptions,
                "op": op,
            }
        )
    return results
