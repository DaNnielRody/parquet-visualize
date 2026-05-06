from __future__ import annotations

import pytest

from src.services.schema_service import SchemaService


@pytest.fixture
def schema_service() -> SchemaService:
    return SchemaService()


class TestHasEntity:
    def test_known_entity(self, schema_service):
        assert schema_service.has_entity("patient")

    def test_unknown_entity(self, schema_service):
        assert not schema_service.has_entity("unknown_entity_xyz")


class TestGetPandasDtypes:
    def test_returns_dict_for_known_entity(self, schema_service):
        dtypes = schema_service.get_pandas_dtypes("appointment")
        assert isinstance(dtypes, dict)
        assert "isBookedOnline" in dtypes
        assert dtypes["isBookedOnline"] == "bool"

    def test_number_fields(self, schema_service):
        dtypes = schema_service.get_pandas_dtypes("appointment")
        assert dtypes["length"] == "float64"

    def test_monetary_override_global(self, schema_service):
        dtypes = schema_service.get_pandas_dtypes("accounts_receivable")
        assert dtypes["responsiblePartyAmount"] == "float64"
        assert dtypes["patientEstimate"] == "float64"
        assert dtypes["insuranceEstimate"] == "float64"

    def test_date_time_not_in_dtype(self, schema_service):
        dtypes = schema_service.get_pandas_dtypes("appointment")
        # date-time columns should be excluded from dtype dict (handled by parse_dates)
        assert "startTime" not in dtypes
        assert "createdAt" not in dtypes

    def test_none_for_unknown(self, schema_service):
        assert schema_service.get_pandas_dtypes("nonexistent") is None

    def test_string_generic_not_forced(self, schema_service):
        dtypes = schema_service.get_pandas_dtypes("contact")
        # generic string fields like "firstName" should NOT appear, letting pandas infer freely
        assert "firstName" not in dtypes


class TestGetDateColumns:
    def test_returns_date_columns(self, schema_service):
        cols = schema_service.get_date_columns("appointment")
        assert isinstance(cols, list)
        assert "startTime" in cols
        assert "createdAt" in cols

    def test_none_for_unknown(self, schema_service):
        assert schema_service.get_date_columns("nonexistent") is None

    def test_empty_when_no_dates(self, schema_service):
        # fee_schedule has no date-time fields
        cols = schema_service.get_date_columns("fee_schedule")
        assert cols == [] or cols is None
