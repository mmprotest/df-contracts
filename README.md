# df-contracts
Lightweight, Python-first DataFrame contracts (schema + rules) for pandas.

## Install
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

## Overview
`df-contracts` helps teams encode schema and data quality expectations for pandas DataFrames.
Define a contract, validate datasets, and surface actionable reports from Python, the CLI, or pytest.

## Features
- Declarative column specifications including dtype, nullability, bounds, enums, regex, and more.
- Row and table level rules with rich reporting and JSON export.
- Contract inference from real datasets to bootstrap adoption quickly.
- Typer-powered CLI and pytest plugin for streamlined automation.

## Development
Install development dependencies and run the test suite:

```bash
pip install -e .[polars]
pip install -r requirements-dev.txt  # optional helper if you create one
pytest
```
