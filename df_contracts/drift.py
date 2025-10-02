from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

import pandas as pd

from .utils import ensure_pandas


@dataclass(slots=True)
class NumericSnapshot:
    count: int
    mean: float
    std: float
    minimum: float
    maximum: float
    quantiles: dict[str, float]

    def as_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "mean": self.mean,
            "std": self.std,
            "min": self.minimum,
            "max": self.maximum,
            "quantiles": self.quantiles,
        }


@dataclass(slots=True)
class CategoricalSnapshot:
    top_values: list[tuple[str, float]]

    def as_dict(self) -> dict[str, Any]:
        return {"top_values": self.top_values}


@dataclass(slots=True)
class ColumnSnapshot:
    column: str
    kind: str
    numeric: NumericSnapshot | None = None
    categorical: CategoricalSnapshot | None = None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"column": self.column, "kind": self.kind}
        if self.numeric:
            payload["numeric"] = self.numeric.as_dict()
        if self.categorical:
            payload["categorical"] = self.categorical.as_dict()
        return payload

    @classmethod
    def from_dict(cls, column: str, data: dict[str, Any]) -> "ColumnSnapshot":
        kind = data.get("kind", "categorical")
        numeric = None
        categorical = None
        if "numeric" in data:
            num = data["numeric"]
            numeric = NumericSnapshot(
                count=int(num.get("count", 0)),
                mean=float(num.get("mean", 0.0)),
                std=float(num.get("std", 0.0)),
                minimum=float(num.get("min", 0.0)),
                maximum=float(num.get("max", 0.0)),
                quantiles={k: float(v) for k, v in num.get("quantiles", {}).items()},
            )
        if "categorical" in data:
            cat = data["categorical"]
            categorical = CategoricalSnapshot(top_values=[(k, float(v)) for k, v in cat.get("top_values", [])])
        return cls(column=column, kind=kind, numeric=numeric, categorical=categorical)


@dataclass(slots=True)
class DriftSnapshot:
    created_at: str
    columns: dict[str, ColumnSnapshot]
    null_ratios: dict[str, float]

    def as_dict(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at,
            "columns": {name: snap.as_dict() for name, snap in self.columns.items()},
            "null_ratios": self.null_ratios,
        }

    def to_json(self) -> str:
        import orjson

        return orjson.dumps(self.as_dict(), option=orjson.OPT_INDENT_2).decode()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DriftSnapshot":
        columns = {
            name: ColumnSnapshot.from_dict(name, payload)
            for name, payload in data.get("columns", {}).items()
        }
        null_ratios = {name: float(value) for name, value in data.get("null_ratios", {}).items()}
        return cls(created_at=data.get("created_at", ""), columns=columns, null_ratios=null_ratios)


@dataclass(slots=True)
class DriftFinding:
    column: str
    kind: str
    details: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"column": self.column, "kind": self.kind, "details": self.details}


@dataclass(slots=True)
class DriftReport:
    ok: bool
    findings: list[DriftFinding] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "findings": [finding.as_dict() for finding in self.findings]}

    def to_json(self) -> str:
        import orjson

        return orjson.dumps(self.as_dict(), option=orjson.OPT_INDENT_2).decode()

    def to_html(self) -> str:
        rows = []
        for finding in self.findings:
            rows.append(
                f"<tr><td>{finding.column}</td><td>{finding.kind}</td><td><code>{finding.details}</code></td></tr>"
            )
        body = "".join(rows) or "<tr><td colspan='3'>No drift detected.</td></tr>"
        return (
            "<!DOCTYPE html><html><head><meta charset='utf-8'><title>df-contracts drift report" "</title>"
            "<style>table{border-collapse:collapse;width:100%;}th,td{border:1px solid #ddd;padding:8px;}</style>"
            "</head><body>"
            f"<h1>df-contracts drift report</h1><p>Status: {'OK' if self.ok else 'Drift detected'}</p>"
            "<table><thead><tr><th>Column</th><th>Kind</th><th>Details</th></tr></thead>"
            f"<tbody>{body}</tbody></table></body></html>"
        )


def snapshot(
    df: pd.DataFrame,
    cols: Iterable[str] | None = None,
    *,
    quantiles: tuple[float, ...] = (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99),
    topk: int = 20,
) -> DriftSnapshot:
    frame = ensure_pandas(df)
    target_columns = list(cols) if cols else list(frame.columns)
    snapshots: dict[str, ColumnSnapshot] = {}
    null_ratios: dict[str, float] = {}
    for column in target_columns:
        series = frame[column]
        null_ratios[column] = float(series.isna().mean())
        if pd.api.types.is_numeric_dtype(series):
            clean = pd.to_numeric(series, errors="coerce")
            clean = clean.dropna()
            quantile_values = {str(q): float(clean.quantile(q)) for q in quantiles if not clean.empty}
            snapshots[column] = ColumnSnapshot(
                column=column,
                kind="numeric",
                numeric=NumericSnapshot(
                    count=int(clean.count()),
                    mean=float(clean.mean()) if not clean.empty else 0.0,
                    std=float(clean.std(ddof=0)) if not clean.empty else 0.0,
                    minimum=float(clean.min()) if not clean.empty else 0.0,
                    maximum=float(clean.max()) if not clean.empty else 0.0,
                    quantiles=quantile_values,
                ),
            )
        else:
            clean = series.dropna().astype(str)
            counts = clean.value_counts(normalize=True).head(topk)
            snapshots[column] = ColumnSnapshot(
                column=column,
                kind="categorical",
                categorical=CategoricalSnapshot(top_values=[(idx, float(val)) for idx, val in counts.items()]),
            )
    return DriftSnapshot(
        created_at=datetime.utcnow().isoformat() + "Z",
        columns=snapshots,
        null_ratios=null_ratios,
    )


def compare_snapshots(
    ref: DriftSnapshot,
    cur: DriftSnapshot,
    *,
    thresholds: dict[str, float] | None = None,
) -> DriftReport:
    thresholds = thresholds or {"quantile": 0.1, "null_ratio": 0.05, "category": 0.2}
    findings: list[DriftFinding] = []
    shared = set(ref.columns).intersection(cur.columns)
    for column in sorted(shared):
        ref_snap = ref.columns[column]
        cur_snap = cur.columns[column]
        if ref_snap.kind == "numeric" and cur_snap.kind == "numeric" and ref_snap.numeric and cur_snap.numeric:
            for quantile, ref_value in ref_snap.numeric.quantiles.items():
                cur_value = cur_snap.numeric.quantiles.get(quantile)
                if cur_value is None:
                    continue
                diff = abs(cur_value - ref_value)
                allowed = thresholds.get("quantile", 0.1)
                if diff > allowed:
                    findings.append(
                        DriftFinding(
                            column=column,
                            kind="quantile",
                            details={"quantile": quantile, "ref": ref_value, "cur": cur_value, "diff": diff},
                        )
                    )
        if column in ref.null_ratios and column in cur.null_ratios:
            diff = abs(cur.null_ratios[column] - ref.null_ratios[column])
            if diff > thresholds.get("null_ratio", 0.05):
                findings.append(
                    DriftFinding(
                        column=column,
                        kind="null_ratio",
                        details={"ref": ref.null_ratios[column], "cur": cur.null_ratios[column], "diff": diff},
                    )
                )
        if (
            ref_snap.kind == "categorical"
            and cur_snap.kind == "categorical"
            and ref_snap.categorical
            and cur_snap.categorical
        ):
            ref_values = {key: val for key, val in ref_snap.categorical.top_values}
            cur_values = {key: val for key, val in cur_snap.categorical.top_values}
            missing = set(ref_values) - set(cur_values)
            churn = sum(ref_values.get(key, 0.0) for key in missing)
            if churn > thresholds.get("category", 0.2):
                findings.append(
                    DriftFinding(
                        column=column,
                        kind="category",
                        details={"missing": sorted(missing), "churn": churn},
                    )
                )
    return DriftReport(ok=not findings, findings=findings)
