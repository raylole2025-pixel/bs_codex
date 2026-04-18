from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _read_csv_rows(path: str | Path) -> list[dict[str, str]]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _float(row: dict[str, str], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    if value in {None, ""}:
        return default
    return float(value)


def _load_tasks(tasks_path: str | Path | None) -> list[dict[str, Any]]:
    if not tasks_path:
        return []
    payload = json.loads(Path(tasks_path).read_text(encoding="utf-8-sig"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("tasks"), list):
        return payload["tasks"]
    raise ValueError("tasks json must be a list or an object containing a 'tasks' list")


def build_stage1_scenario_template(
    preprocess_dir: str | Path,
    output_path: str | Path,
    *,
    tasks_path: str | Path | None = None,
    capacities: dict[str, float] | None = None,
    stage1_config: dict[str, Any] | None = None,
    stage2_config: dict[str, Any] | None = None,
    hotspots_a: list[dict[str, Any]] | None = None,
    metadata_updates: dict[str, Any] | None = None,
    scenario_name: str = "stage1-from-stk-preprocess",
) -> dict[str, Any]:
    root = Path(preprocess_dir)
    summary_path = root / "stage1_preprocess_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing preprocess summary: {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8-sig"))

    a_rows = _read_csv_rows(root / "A_clean_contacts.csv")
    b_rows = _read_csv_rows(root / "B_clean_contacts.csv")
    x_rows = _read_csv_rows(root / "X_clean_contacts.csv")
    tasks = _load_tasks(tasks_path)

    nodes_a = sorted({row["src"] for row in a_rows} | {row["dst"] for row in a_rows} | {row["gw_A"] for row in x_rows})
    nodes_b = sorted({row["src"] for row in b_rows} | {row["dst"] for row in b_rows} | {row["gw_B"] for row in x_rows})

    intra_links: list[dict[str, Any]] = []
    for idx, row in enumerate(a_rows, start=1):
        intra_links.append(
            {
                "id": f"A_ISL_{idx:06d}",
                "u": row["src"],
                "v": row["dst"],
                "domain": "A",
                "start": _float(row, "start_sec"),
                "end": _float(row, "stop_sec"),
                "delay": 0.0,
                "weight": 1.0,
            }
        )
    for idx, row in enumerate(b_rows, start=1):
        intra_links.append(
            {
                "id": f"B_ISL_{idx:06d}",
                "u": row["src"],
                "v": row["dst"],
                "domain": "B",
                "start": _float(row, "start_sec"),
                "end": _float(row, "stop_sec"),
                "delay": 0.0,
                "weight": 1.0,
            }
        )

    candidate_windows = [
        {
            "id": row.get("window_id") or f"X{idx:06d}",
            "a": row["gw_A"],
            "b": row["gw_B"],
            "start": _float(row, "start_sec"),
            "end": _float(row, "stop_sec"),
            "value": None,
        }
        for idx, row in enumerate(x_rows, start=1)
    ]

    planning_end = max(
        [0.0]
        + [_float(row, "stop_sec") for row in a_rows]
        + [_float(row, "stop_sec") for row in b_rows]
        + [_float(row, "stop_sec") for row in x_rows]
    )

    capacities_payload = {
        "A": float((capacities or {}).get("A", 600.0)),
        "B": float((capacities or {}).get("B", 2000.0)),
        "X": float((capacities or {}).get("X", 1000.0)),
    }
    stage1_payload = {
        "theta_cap": float((stage1_config or {}).get("theta_cap", 0.08)),
        "theta_hot": float((stage1_config or {}).get("theta_hot", 0.80)),
        "rho": float((stage1_config or {}).get("rho", 0.20)),
        "t_pre": float((stage1_config or {}).get("t_pre", 1800.0)),
        "d_min": float((stage1_config or {}).get("d_min", 600.0)),
        "hot_hop_limit": int((stage1_config or {}).get("hot_hop_limit", 4)),
        "bottleneck_factor_alpha": float((stage1_config or {}).get("bottleneck_factor_alpha", 0.85)),
        "eta_x": float((stage1_config or {}).get("eta_x", 0.90)),
        "static_value_snapshot_seconds": int((stage1_config or {}).get("static_value_snapshot_seconds", 600)),
        "candidate_pool_base_size": int((stage1_config or {}).get("candidate_pool_base_size", 400)),
        "candidate_pool_hot_fraction": float((stage1_config or {}).get("candidate_pool_hot_fraction", 0.30)),
        "candidate_pool_min_per_coarse_segment": int((stage1_config or {}).get("candidate_pool_min_per_coarse_segment", 3)),
        "candidate_pool_max_additional": int((stage1_config or {}).get("candidate_pool_max_additional", 150)),
        "q_eval": int((stage1_config or {}).get("q_eval", 4)),
        "omega_fr": float((stage1_config or {}).get("omega_fr", 4.0 / 9.0)),
        "omega_cap": float((stage1_config or {}).get("omega_cap", 3.0 / 9.0)),
        "omega_hot": float((stage1_config or {}).get("omega_hot", 2.0 / 9.0)),
        "elite_prune_count": int((stage1_config or {}).get("elite_prune_count", 6)),
        "ga": {
            "population_size": int((stage1_config or {}).get("population_size", 60)),
            "crossover_probability": float((stage1_config or {}).get("crossover_probability", 0.90)),
            "mutation_probability": float((stage1_config or {}).get("mutation_probability", 0.20)),
            "max_generations": int((stage1_config or {}).get("max_generations", 100)),
            "stall_generations": int((stage1_config or {}).get("stall_generations", 20)),
            "top_m": int((stage1_config or {}).get("top_m", 5)),
            "max_runtime_seconds": (stage1_config or {}).get("max_runtime_seconds"),
        },
    }
    stage2_payload = {
        "k_paths": int((stage2_config or {}).get("k_paths", 2)),
        "completion_tolerance": float((stage2_config or {}).get("completion_tolerance", 1e-6)),
        "label_keep_limit": int((stage2_config or {}).get("label_keep_limit", 16)),
    }

    payload = {
        "metadata": {
            "name": scenario_name,
            "source": "stk_access_preprocess",
            "preprocess_dir": str(root.resolve()),
            "precomputed_hops": {
                "A": str((root / "A_hop_matrix.csv").resolve()),
                "B": str((root / "B_hop_matrix.csv").resolve()),
            },
            "stage1_preprocess": {
                "summary_file": str(summary_path.resolve()),
                "analysis_start_utc": summary["analysis_start_utc"],
                "analysis_stop_utc": summary["analysis_stop_utc"],
                "snapshot_seconds": summary["snapshot_seconds"],
                "snapshot_count": summary["snapshot_count"],
            },
        },
        "planning_end": planning_end,
        "nodes": {
            "A": nodes_a,
            "B": nodes_b,
        },
        "capacities": capacities_payload,
        "stage1": stage1_payload,
        "stage2": stage2_payload,
        "intra_domain_links": intra_links,
        "candidate_windows": candidate_windows,
        "tasks": tasks,
    }
    if metadata_updates:
        payload["metadata"].update(metadata_updates)
    if hotspots_a:
        payload["hotspots"] = {"A": hotspots_a}

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload
