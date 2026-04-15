from __future__ import annotations

from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def make_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf)
    return buf.getvalue()


class FakeUploadedFile:
    def __init__(self, df: pd.DataFrame, name: str = "arquivo.parquet") -> None:
        self._data = make_parquet_bytes(df)
        self.name = name

    def getvalue(self) -> bytes:
        return self._data


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [1.0, 2.0, 3.0]})


@pytest.fixture
def sample_df2() -> pd.DataFrame:
    return pd.DataFrame({"id": [4, 5], "name": ["d", "e"], "value": [4.0, 5.0]})


@pytest.fixture
def different_schema_df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1], "name": ["x"], "extra": ["y"]})


@pytest.fixture
def type_mismatch_df() -> pd.DataFrame:
    return pd.DataFrame({"id": ["1", "2"], "name": ["a", "b"], "value": [1.0, 2.0]})
