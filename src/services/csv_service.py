from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd

from src.services.schema_service import SchemaService


class CSVService:
    """Leitura de CSV com tipagem híbrida: schema (para boolean/number/date-time)
    + inferência do pandas (para string genérico).
    """

    def __init__(self, schema_service: SchemaService | None = None) -> None:
        self.schema_service = schema_service or SchemaService()

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #
    def load_uploaded_csv(
        self, uploaded_file, entity_name: str | None = None
    ) -> pd.DataFrame:
        raw: bytes = uploaded_file.getvalue()
        return self._read_csv_bytes(raw, entity_name)

    def load_csv_file(self, file_path: Path, entity_name: str | None = None) -> pd.DataFrame:
        return self._read_csv_path(file_path, entity_name)

    def read_schema(self, file_obj) -> list[dict[str, str]]:
        """Infere schema a partir do header do CSV (sem usar curve-schema)."""
        df = pd.read_csv(BytesIO(file_obj.getvalue()), nrows=0)
        return [
            {"column": col, "dtype": str(dtype)} for col, dtype in df.dtypes.items()
        ]

    # ------------------------------------------------------------------ #
    #  Internals
    # ------------------------------------------------------------------ #
    def _read_csv_bytes(self, data: bytes, entity_name: str | None = None) -> pd.DataFrame:
        separator = self._detect_separator_from_bytes(data)
        dtype, parse_dates = self._schema_hints(entity_name)

        # Primeiro lê o header para saber quais colunas existem
        header_df = pd.read_csv(BytesIO(data), sep=separator, nrows=0, encoding="utf-8")
        available_cols = set(header_df.columns)
        dtype = self._filter_cols(dtype, available_cols)
        parse_dates = self._filter_list(parse_dates, available_cols)

        return pd.read_csv(
            BytesIO(data),
            sep=separator,
            dtype=dtype,
            parse_dates=parse_dates,
            encoding="utf-8",
        )

    def _read_csv_path(self, path: Path, entity_name: str | None = None) -> pd.DataFrame:
        separator = self._detect_separator_from_path(path)
        dtype, parse_dates = self._schema_hints(entity_name)

        header_df = pd.read_csv(path, sep=separator, nrows=0, encoding="utf-8")
        available_cols = set(header_df.columns)
        dtype = self._filter_cols(dtype, available_cols)
        parse_dates = self._filter_list(parse_dates, available_cols)

        return pd.read_csv(
            path,
            sep=separator,
            dtype=dtype,
            parse_dates=parse_dates,
            encoding="utf-8",
        )

    @staticmethod
    def _filter_cols(
        dtype: dict[str, str] | None, available_cols: set[str]
    ) -> dict[str, str] | None:
        if dtype is None:
            return None
        filtered = {k: v for k, v in dtype.items() if k in available_cols}
        return filtered if filtered else None

    @staticmethod
    def _filter_list(
        cols: list[str] | None, available_cols: set[str]
    ) -> list[str] | None:
        if cols is None:
            return None
        filtered = [c for c in cols if c in available_cols]
        return filtered if filtered else None

    def _schema_hints(
        self, entity_name: str | None = None
    ) -> tuple[dict[str, str] | None, list[str] | None]:
        if not entity_name:
            return None, None
        dtype = self.schema_service.get_pandas_dtypes(entity_name)
        parse_dates = self.schema_service.get_date_columns(entity_name)
        return dtype, parse_dates

    def _detect_separator_from_bytes(self, data: bytes) -> str:
        sample = data[:4096]
        try:
            text = sample.decode("utf-8")
        except UnicodeDecodeError:
            text = sample.decode("latin1")
        return self._detect_separator(text)

    def _detect_separator_from_path(self, path: Path) -> str:
        with open(path, "rb") as fh:
            sample = fh.read(4096)
        return self._detect_separator_from_bytes(sample)

    @staticmethod
    def _detect_separator(sample_text: str) -> str:
        first_line = sample_text.split("\n")[0] if "\n" in sample_text else sample_text
        candidates = {
            ",": first_line.count(","),
            ";": first_line.count(";"),
            "\t": first_line.count("\t"),
        }
        best = max(candidates, key=candidates.get)  # type: ignore[arg-type]
        return best if candidates[best] > 0 else ","
