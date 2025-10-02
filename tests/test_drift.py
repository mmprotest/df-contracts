from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from df_contracts.drift import DriftSnapshot, compare_snapshots, snapshot


def test_snapshot_and_compare(tmp_path: Path, sample_path: Path) -> None:
    df = pd.read_csv(sample_path)
    current = snapshot(df)
    ref_path = Path("tests/data/snapshot_ref.json")
    ref = DriftSnapshot.from_dict(json.loads(ref_path.read_text()))
    report = compare_snapshots(ref, current)
    assert report.ok
    tweaked = df.copy()
    tweaked["amount"] = tweaked["amount"] * 2
    tweaked.loc[:, "category"] = "Z"
    drifted = snapshot(tweaked)
    drift_report = compare_snapshots(ref, drifted, thresholds={"quantile": 0.5, "null_ratio": 0.05, "category": 0.3})
    assert not drift_report.ok
    html = drift_report.to_html()
    assert "df-contracts drift report" in html
