from __future__ import annotations

import pandas as pd
import pytest

from src.services.parquet_service import ParquetService
from tests.conftest import FakeUploadedFile


@pytest.fixture
def service() -> ParquetService:
    return ParquetService()


class TestLoadUploadedParquet:
    def test_returns_dataframe(self, service, sample_df):
        fake_file = FakeUploadedFile(sample_df)
        result = service.load_uploaded_parquet(fake_file)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == list(sample_df.columns)
        assert len(result) == len(sample_df)

    def test_values_preserved(self, service, sample_df):
        fake_file = FakeUploadedFile(sample_df)
        result = service.load_uploaded_parquet(fake_file)
        assert result["id"].tolist() == [1, 2, 3]
        assert result["name"].tolist() == ["a", "b", "c"]


class TestLoadParquetFile:
    def test_reads_saved_file(self, service, sample_df, tmp_path):
        path = tmp_path / "test.parquet"
        service.save_parquet_file(sample_df, path)
        result = service.load_parquet_file(path)
        assert len(result) == len(sample_df)
        assert list(result.columns) == list(sample_df.columns)


class TestSaveParquetFile:
    def test_creates_file(self, service, sample_df, tmp_path):
        path = tmp_path / "sub" / "test.parquet"
        service.save_parquet_file(sample_df, path)
        assert path.exists()

    def test_creates_parent_dirs(self, service, sample_df, tmp_path):
        path = tmp_path / "a" / "b" / "c" / "test.parquet"
        service.save_parquet_file(sample_df, path)
        assert path.exists()


class TestMergeDataframes:
    def test_concat_rows(self, service, sample_df, sample_df2):
        result = service.merge_dataframes([sample_df, sample_df2])
        assert len(result) == len(sample_df) + len(sample_df2)

    def test_resets_index(self, service, sample_df, sample_df2):
        result = service.merge_dataframes([sample_df, sample_df2])
        assert list(result.index) == list(range(len(result)))

    def test_single_frame(self, service, sample_df):
        result = service.merge_dataframes([sample_df])
        assert len(result) == len(sample_df)


class TestReadSchema:
    def test_returns_column_list(self, service, sample_df):
        fake_file = FakeUploadedFile(sample_df)
        schema = service.read_schema(fake_file)
        column_names = [entry["column"] for entry in schema]
        assert set(column_names) == {"id", "name", "value"}

    def test_entries_have_dtype(self, service, sample_df):
        fake_file = FakeUploadedFile(sample_df)
        schema = service.read_schema(fake_file)
        for entry in schema:
            assert "column" in entry
            assert "dtype" in entry
            assert isinstance(entry["dtype"], str)


class TestCompareSchemas:
    def test_identical_schemas_no_errors(self, service, sample_df, sample_df2):
        mismatch = service.compare_schemas(sample_df, sample_df2)
        assert not mismatch.has_errors

    def test_detects_missing_in_new(self, service, sample_df, different_schema_df):
        mismatch = service.compare_schemas(sample_df, different_schema_df)
        assert "value" in mismatch.missing_in_new

    def test_detects_missing_in_existing(self, service, sample_df, different_schema_df):
        mismatch = service.compare_schemas(sample_df, different_schema_df)
        assert "extra" in mismatch.missing_in_existing

    def test_detects_type_mismatch(self, service, sample_df, type_mismatch_df):
        mismatch = service.compare_schemas(sample_df, type_mismatch_df)
        assert mismatch.has_errors
        mismatch_columns = [m["column"] for m in mismatch.type_mismatches]
        assert "id" in mismatch_columns

    def test_has_errors_false_when_clean(self, service, sample_df):
        mismatch = service.compare_schemas(sample_df, sample_df.copy())
        assert not mismatch.has_errors


class TestBuildSchemaTable:
    def test_returns_dataframe_with_columns(self, service, sample_df):
        result = service.build_schema_table(sample_df)
        assert set(result.columns) == {"coluna", "tipo", "nulos", "percentual_nulos"}

    def test_one_row_per_column(self, service, sample_df):
        result = service.build_schema_table(sample_df)
        assert len(result) == len(sample_df.columns)

    def test_null_count_accuracy(self, service):
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, None, None]})
        result = service.build_schema_table(df)
        row_a = result[result["coluna"] == "a"].iloc[0]
        row_b = result[result["coluna"] == "b"].iloc[0]
        assert row_a["nulos"] == 1
        assert row_b["nulos"] == 3

    def test_percentage_calculation(self, service):
        df = pd.DataFrame({"a": [1, None, None, None]})
        result = service.build_schema_table(df)
        assert result.iloc[0]["percentual_nulos"] == 75.0
