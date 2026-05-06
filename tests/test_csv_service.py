from __future__ import annotations

import pandas as pd
import pytest

from src.services.csv_service import CSVService
from tests.conftest import FakeUploadedCSVFile


@pytest.fixture
def csv_service() -> CSVService:
    return CSVService()


class TestLoadUploadedCSV:
    def test_reads_simple_csv(self, csv_service):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        fake = FakeUploadedCSVFile(df)
        result = csv_service.load_uploaded_csv(fake)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["id", "name"]
        assert len(result) == 2

    def test_reads_semicolon_separated(self, csv_service):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        fake = FakeUploadedCSVFile(df, sep=";")
        result = csv_service.load_uploaded_csv(fake)
        assert len(result) == 2
        assert list(result.columns) == ["id", "name"]

    def test_schema_typing_for_boolean(self, csv_service):
        df = pd.DataFrame({"isBookedOnline": ["True", "False", "True"]})
        fake = FakeUploadedCSVFile(df)
        result = csv_service.load_uploaded_csv(fake, entity_name="appointment")
        assert result["isBookedOnline"].dtype == bool

    def test_schema_typing_for_number(self, csv_service):
        df = pd.DataFrame({"length": ["30", "60", "90"]})
        fake = FakeUploadedCSVFile(df)
        result = csv_service.load_uploaded_csv(fake, entity_name="appointment")
        assert pd.api.types.is_float_dtype(result["length"])

    def test_monetary_override(self, csv_service):
        df = pd.DataFrame({"responsiblePartyAmount": ["1500.00", "2300.50"]})
        fake = FakeUploadedCSVFile(df)
        result = csv_service.load_uploaded_csv(fake, entity_name="accounts_receivable")
        assert pd.api.types.is_float_dtype(result["responsiblePartyAmount"])

    def test_date_parsing(self, csv_service):
        df = pd.DataFrame({"startTime": ["2024-01-15T10:00:00Z", "2024-01-16T11:00:00Z"]})
        fake = FakeUploadedCSVFile(df)
        result = csv_service.load_uploaded_csv(fake, entity_name="appointment")
        assert pd.api.types.is_datetime64_any_dtype(result["startTime"])

    def test_string_generic_inferred_by_pandas(self, csv_service):
        # Schema says "id" is string in appointment, but we let pandas infer when no strong typing
        # Actually appointment id is string in schema; since it's not bool/number/date, pandas infers
        df = pd.DataFrame({"id": ["abc", "def"]})
        fake = FakeUploadedCSVFile(df)
        result = csv_service.load_uploaded_csv(fake, entity_name="appointment")
        # Should be object/string, not forced to anything else
        assert result["id"].dtype == object

    def test_no_entity_just_infers(self, csv_service):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        fake = FakeUploadedCSVFile(df)
        result = csv_service.load_uploaded_csv(fake)
        assert result["a"].dtype in ("int64", "float64")
        assert result["b"].dtype == object


class TestReadSchema:
    def test_returns_columns(self, csv_service):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        fake = FakeUploadedCSVFile(df)
        schema = csv_service.read_schema(fake)
        columns = [s["column"] for s in schema]
        assert "id" in columns
        assert "name" in columns

    def test_entries_have_dtype(self, csv_service):
        df = pd.DataFrame({"id": [1, 2]})
        fake = FakeUploadedCSVFile(df)
        schema = csv_service.read_schema(fake)
        for entry in schema:
            assert "column" in entry
            assert "dtype" in entry
