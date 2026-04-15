from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.services.parquet_service import ParquetService, SchemaMismatch
from src.services.storage_service import StorageService


@dataclass
class UploadResult:
    entity_name: str
    uploaded_files: int
    batch_rows: int
    total_rows: int
    saved_path: Path
    schema_table: pd.DataFrame
    preview_df: pd.DataFrame


class UploadService:
    def __init__(
        self, storage_service: StorageService, parquet_service: ParquetService
    ) -> None:
        self.storage_service = storage_service
        self.parquet_service = parquet_service

    def sanitize_entity_name(self, entity_name: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9_-]+", "_", entity_name.strip().lower())
        return sanitized.strip("_")

    def process_upload(self, entity_name: str, uploaded_files: list) -> UploadResult:
        normalized_name = self._resolve_entity_name(entity_name)
        if not uploaded_files:
            raise ValueError("Selecione ao menos um arquivo parquet.")

        batch_frames = [
            self.parquet_service.load_uploaded_parquet(uploaded_file)
            for uploaded_file in uploaded_files
        ]
        return self._finalize_upload(normalized_name, batch_frames, len(uploaded_files))

    def process_folder_upload(
        self, uploaded_files: list, entity_name: str = ""
    ) -> UploadResult:
        if not uploaded_files:
            raise ValueError("Selecione ao menos uma pasta com arquivos parquet.")

        normalized_name = self._resolve_entity_name(
            entity_name, fallback_name=self._infer_folder_name(uploaded_files)
        )
        batch_frames = [
            self.parquet_service.load_uploaded_parquet(uploaded_file)
            for uploaded_file in uploaded_files
        ]
        return self._finalize_upload(normalized_name, batch_frames, len(uploaded_files))

    def _finalize_upload(
        self,
        entity_name: str,
        batch_frames: list[pd.DataFrame],
        uploaded_files_count: int,
    ) -> UploadResult:
        batch_df = self.parquet_service.merge_dataframes(batch_frames)
        entity_dataset_path = self.storage_service.get_entity_dataset_path(entity_name)

        if entity_dataset_path.exists():
            existing_df = self.parquet_service.load_parquet_file(entity_dataset_path)
            mismatch = self.parquet_service.compare_schemas(existing_df, batch_df)
            if mismatch.has_errors:
                raise ValueError(self._format_schema_error(mismatch))
            final_df = self.parquet_service.merge_dataframes([existing_df, batch_df])
        else:
            final_df = batch_df

        self.parquet_service.save_parquet_file(final_df, entity_dataset_path)

        return UploadResult(
            entity_name=entity_name,
            uploaded_files=uploaded_files_count,
            batch_rows=int(len(batch_df)),
            total_rows=int(len(final_df)),
            saved_path=entity_dataset_path,
            schema_table=self.parquet_service.build_schema_table(final_df),
            preview_df=final_df.head(100),
        )

    def _resolve_entity_name(
        self, entity_name: str, fallback_name: str | None = None
    ) -> str:
        candidate = entity_name
        if not candidate and fallback_name:
            candidate = fallback_name

        normalized_name = self.sanitize_entity_name(candidate)
        if not normalized_name:
            raise ValueError("Informe um nome de entidade valido.")
        return normalized_name

    def _infer_folder_name(self, uploaded_files: list) -> str | None:
        for uploaded_file in uploaded_files:
            file_name = getattr(uploaded_file, "name", "")
            normalized_path = file_name.replace("\\", "/").strip("/")
            parts = [part for part in normalized_path.split("/") if part]
            if len(parts) > 1:
                return parts[0]
        return None

    def load_entity_view(self, entity_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        dataset_path = self.storage_service.get_entity_dataset_path(entity_name)
        if not dataset_path.exists():
            raise FileNotFoundError(f"Entidade '{entity_name}' nao encontrada.")

        dataframe = self.parquet_service.load_parquet_file(dataset_path)
        schema_table = self.parquet_service.build_schema_table(dataframe)
        return dataframe, schema_table

    def _format_schema_error(self, mismatch: SchemaMismatch) -> str:
        details = []
        if mismatch.missing_in_new:
            details.append(
                "Colunas existentes ausentes no novo lote: "
                + ", ".join(mismatch.missing_in_new)
            )
        if mismatch.missing_in_existing:
            details.append(
                "Colunas novas ausentes no consolidado atual: "
                + ", ".join(mismatch.missing_in_existing)
            )
        if mismatch.type_mismatches:
            mismatch_lines = [
                f"{item['column']} ({item['existing_type']} vs {item['new_type']})"
                for item in mismatch.type_mismatches
            ]
            details.append("Tipos divergentes: " + ", ".join(mismatch_lines))
        return "Schema incompativel para merge. " + " | ".join(details)
