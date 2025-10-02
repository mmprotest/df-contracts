from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "df_contracts.cli", *args],
        check=False,
        capture_output=True,
        text=True,
    )


def test_cli_init_and_check(tmp_path: Path, sample_path: Path, bad_sample_path: Path) -> None:
    result = run_cli("init", str(sample_path))
    assert result.returncode == 0, result.stderr
    contract_file = tmp_path / "contract.json"
    contract_file.write_text(result.stdout)

    check_ok = run_cli(
        "check",
        str(sample_path),
        "--contract",
        str(contract_file),
    )
    assert check_ok.returncode == 0, check_ok.stderr

    report_file = tmp_path / "report.json"
    check_bad = run_cli(
        "check",
        str(bad_sample_path),
        "--contract",
        str(contract_file),
        "--report",
        str(report_file),
    )
    assert check_bad.returncode != 0
    payload = json.loads(report_file.read_text())
    assert payload["violations"]
