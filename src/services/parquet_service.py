from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


@dataclass
class SchemaMismatch:
    missing_in_new: list[str]
    missing_in_existing: list[str]
    type_mismatches: list[dict[str, str]]

    @property
    def has_errors(self) -> bool:
        return bool(
            self.missing_in_new or self.missing_in_existing or self.type_mismatches
        )


class ParquetService:
    def load_uploaded_parquet(self, uploaded_file) -> pd.DataFrame:
        return pd.read_parquet(BytesIO(uploaded_file.getvalue()))

    def load_parquet_file(self, file_path: Path) -> pd.DataFrame:
        return pd.read_parquet(file_path)

    def save_parquet_file(self, dataframe: pd.DataFrame, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(file_path, index=False)

    def merge_dataframes(self, dataframes: list[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(dataframes, ignore_index=True)

    def read_schema(self, file_obj) -> list[dict[str, str]]:
        schema = pq.read_schema(BytesIO(file_obj.getvalue()))
        return [
            {"column": field.name, "dtype": str(field.type)}
            for field in schema
        ]

    def compare_schemas(
        self, existing_df: pd.DataFrame, new_df: pd.DataFrame
    ) -> SchemaMismatch:
        existing_types = {
            column: str(dtype) for column, dtype in existing_df.dtypes.items()
        }
        new_types = {column: str(dtype) for column, dtype in new_df.dtypes.items()}

        existing_columns = set(existing_types)
        new_columns = set(new_types)

        type_mismatches = []
        for column in sorted(existing_columns & new_columns):
            if existing_types[column] != new_types[column]:
                type_mismatches.append(
                    {
                        "column": column,
                        "existing_type": existing_types[column],
                        "new_type": new_types[column],
                    }
                )

        return SchemaMismatch(
            missing_in_new=sorted(existing_columns - new_columns),
            missing_in_existing=sorted(new_columns - existing_columns),
            type_mismatches=type_mismatches,
        )

    def build_schema_table(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        total_rows = max(len(dataframe), 1)
        rows = []
        for column in dataframe.columns:
            null_count = int(dataframe[column].isna().sum())
            rows.append(
                {
                    "coluna": column,
                    "tipo": str(dataframe[column].dtype),
                    "nulos": null_count,
                    "percentual_nulos": round((null_count / total_rows) * 100, 2),
                }
            )
        return pd.DataFrame(rows)

