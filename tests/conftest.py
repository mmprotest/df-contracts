from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

pytest_plugins = ["df_contracts.plugin"]


@pytest.fixture(scope="session")
def sample_path() -> Path:
    return Path("tests/data/sample.csv")


@pytest.fixture(scope="session")
def bad_sample_path() -> Path:
    return Path("tests/data/bad_sample.csv")


@pytest.fixture
def sample_df(sample_path: Path) -> pd.DataFrame:
    return pd.read_csv(sample_path)


@pytest.fixture
def bad_sample_df(bad_sample_path: Path) -> pd.DataFrame:
    return pd.read_csv(bad_sample_path)
