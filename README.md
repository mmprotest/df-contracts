# df-contracts

`df-contracts` is a lightweight yet batteries-included toolkit for defining, evolving, and enforcing DataFrame quality contracts. It is designed for Python users who want strong guarantees without heavyweight infrastructure.

## Why df-contracts?

* **Developer productivity** – infer contracts from real data, lint for best practices, and view rich HTML reports directly in notebooks or CI logs.
* **Safe evolution** – diff contracts, detect breaking changes, and gate deployments when schemas drift.
* **Observability** – capture drift snapshots and compare them over time to stay ahead of data quality surprises.
* **Integrations** – generate TypedDicts, Pydantic models, dbt generic tests, Great Expectations suites, and SQL DDL from a single source of truth.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

Optional extras:

```bash
pip install -e .[polars]   # add polars support
```

## Fast three-minute tour

1. Infer a contract:
   ```python
   import pandas as pd
   from df_contracts import infer_contract

   df = pd.read_csv("data/orders.csv")
   result = infer_contract(df, name="orders")
   contract = result.contract
   print(result.suggestions)
   ```
2. Lint and apply improvements:
   ```python
   from df_contracts.linting import suggest_improvements

   lint = suggest_improvements(contract, df)
   updated = lint.apply(contract)
   ```
3. Validate with sampling, profiles, and HTML reports:
   ```python
   from df_contracts import validate

   report = validate(df, updated, profile="dev", sample=0.2, by=["country"], with_snapshot=True)
   report  # renders as rich HTML in notebooks
   ```
4. Snapshot drift, detect changes, and export artefacts:
   ```python
   from df_contracts.drift import snapshot, compare_snapshots
   from df_contracts.exporters import from_contract_to_sql

   baseline = snapshot(df)
   # ...later...
   new_report = compare_snapshots(baseline, snapshot(df))
   sql = from_contract_to_sql(updated, dialect="postgres")
   ```

The repository ships with a runnable `notebooks/killer_demo.ipynb` notebook demonstrating the full workflow.

## HTML & CI-friendly reports

`ValidationReport.to_html()` creates sortable, colour-coded summaries ideal for Jupyter or dashboards. The CLI exposes `--html`, `--junit`, and `--pr-md` flags so that CI pipelines can upload HTML, JUnit XML, and Markdown artefacts with zero extra tooling.

## Sampling & profiles

Validation supports probabilistic sampling and stratification via `sample=` and `by=` arguments. Contracts may define `profiles` with per-environment overrides (e.g. relaxed null ratios for development). The CLI mirrors this with `--sample`, `--by`, and `--profile` flags.

## Drift snapshots

Capture compact statistics with `dfc snapshot data.csv --out baseline.json`. Later, run `dfc drift data.csv --ref baseline.json --report drift.json --html drift.html` to highlight quantile shifts, category churn, and null-rate changes.

## Contract diffs & breaking change detection

Use `dfc diff-contracts old.json new.json --fail-on-breaking` to surface column additions/removals, dtype and nullability changes, rule updates, and enum churn. Breaking diffs exit non-zero, perfect for gating pull requests.

## Exporters

* `dfc export-types contract.json --kind typedict --out models.py`
* `dfc export-dbt contract.json --table orders --out tests.yml`
* `dfc export-gx contract.json --out suite.json`
* `dfc sql contract.json --dialect postgres --out schema.sql`

These helpers make it trivial to keep downstream systems aligned with the source contract.

## Pre-commit integration

Add the following to `.pre-commit-config.yaml` to validate committed datasets:

```yaml
- repo: local
  hooks:
    - id: dfc-check
      name: df-contracts check
      entry: dfc check --contract contracts/orders.json --profile prod --html reports/orders.html
      language: system
      files: "data/.*\\.(csv|parquet)$"
```

## Pytest plugin & CI

Enable the plugin via `pytest_plugins = ["df_contracts.plugin"]`. Tests may call `df_contracts.must_match(contract, df, profile="dev")`, and pytest options `--dfc-report`, `--dfc-junit`, and `--dfc-html` aggregate run-level artefacts automatically.

Example GitHub Actions step:

```yaml
- name: Validate data contract
  run: dfc check tests/data/sample.csv --contract contracts/orders.json --html build/report.html --junit build/report.xml
- uses: actions/upload-artifact@v3
  with:
    name: df-contracts-reports
    path: build/
```

## Development

```bash
pip install -e .
pip install -r requirements-dev.txt  # optional helper if you create one
pytest
```

Run `dfc --help` to explore every command.
