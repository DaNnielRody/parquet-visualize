from __future__ import annotations

import pytest

from src.services.parquet_service import ParquetService
from src.services.storage_service import StorageService
from src.services.upload_service import UploadResult, UploadService
from tests.conftest import FakeUploadedFile, make_parquet_bytes


@pytest.fixture
def services(tmp_path):
    storage = StorageService(tmp_path)
    parquet = ParquetService()
    upload = UploadService(storage, parquet)
    return upload, storage, parquet


class TestSanitizeEntityName:
    @pytest.fixture(autouse=True)
    def setup(self, services):
        self.svc, *_ = services

    def test_lowercases(self):
        assert self.svc.sanitize_entity_name("Clientes") == "clientes"

    def test_replaces_spaces(self):
        assert self.svc.sanitize_entity_name("minha entidade") == "minha_entidade"

    def test_replaces_special_chars(self):
        assert self.svc.sanitize_entity_name("foo@bar!baz") == "foo_bar_baz"

    def test_strips_leading_trailing_underscores(self):
        assert self.svc.sanitize_entity_name("  _foo_  ") == "foo"

    def test_empty_string_returns_empty(self):
        assert self.svc.sanitize_entity_name("   ") == ""

    def test_allows_hyphens(self):
        assert self.svc.sanitize_entity_name("foo-bar") == "foo-bar"


class TestProcessUpload:
    def test_raises_on_empty_entity_name(self, services, sample_df):
        svc, *_ = services
        with pytest.raises(ValueError, match="entidade"):
            svc.process_upload("  ", [FakeUploadedFile(sample_df)])

    def test_raises_on_no_files(self, services):
        svc, *_ = services
        with pytest.raises(ValueError, match="arquivo"):
            svc.process_upload("clientes", [])

    def test_returns_upload_result(self, services, sample_df):
        svc, *_ = services
        result = svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        assert isinstance(result, UploadResult)

    def test_result_counts_correct(self, services, sample_df, sample_df2):
        svc, *_ = services
        files = [FakeUploadedFile(sample_df), FakeUploadedFile(sample_df2)]
        result = svc.process_upload("clientes", files)
        assert result.uploaded_files == 2
        assert result.batch_rows == len(sample_df) + len(sample_df2)
        assert result.total_rows == len(sample_df) + len(sample_df2)

    def test_saves_file(self, services, sample_df):
        svc, storage, *_ = services
        svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        assert storage.get_entity_dataset_path("clientes").exists()

    def test_merge_on_second_upload(self, services, sample_df, sample_df2):
        svc, *_ = services
        svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        result = svc.process_upload("clientes", [FakeUploadedFile(sample_df2)])
        assert result.total_rows == len(sample_df) + len(sample_df2)
        assert result.batch_rows == len(sample_df2)

    def test_schema_mismatch_raises(self, services, sample_df, different_schema_df):
        svc, *_ = services
        svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        with pytest.raises(ValueError, match="Schema"):
            svc.process_upload("clientes", [FakeUploadedFile(different_schema_df)])

    def test_type_mismatch_raises(self, services, sample_df, type_mismatch_df):
        svc, *_ = services
        svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        with pytest.raises(ValueError, match="Schema"):
            svc.process_upload("clientes", [FakeUploadedFile(type_mismatch_df)])

    def test_entity_name_normalized(self, services, sample_df):
        svc, *_ = services
        result = svc.process_upload("Meus Clientes!", [FakeUploadedFile(sample_df)])
        assert result.entity_name == "meus_clientes"

    def test_does_not_create_dir_on_load(self, services, tmp_path):
        svc, *_ = services
        with pytest.raises(FileNotFoundError):
            svc.load_entity_view("nonexistent")
        assert not (tmp_path / "merged" / "nonexistent").exists()


class TestLoadEntityView:
    def test_raises_if_not_found(self, services):
        svc, *_ = services
        with pytest.raises(FileNotFoundError):
            svc.load_entity_view("naoexiste")

    def test_returns_dataframe_and_schema(self, services, sample_df):
        svc, *_ = services
        svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        df, schema = svc.load_entity_view("clientes")
        assert len(df) == len(sample_df)
        assert "coluna" in schema.columns


class TestProcessFolderUpload:
    def test_raises_if_no_files(self, services):
        svc, *_ = services
        with pytest.raises(ValueError, match="Selecione ao menos uma pasta"):
            svc.process_folder_upload([])

    def test_uses_folder_name_from_uploaded_paths(
        self, services, sample_df, sample_df2
    ):
        svc, *_ = services
        uploaded_files = [
            FakeUploadedFile(sample_df, "clientes/parte1.parquet"),
            FakeUploadedFile(sample_df2, "clientes/2024/parte2.parquet"),
        ]

        result = svc.process_folder_upload(uploaded_files)

        assert result.entity_name == "clientes"
        assert result.uploaded_files == 2

    def test_requires_entity_when_folder_name_cannot_be_inferred(
        self, services, sample_df
    ):
        svc, *_ = services
        uploaded_files = [FakeUploadedFile(sample_df, "parte1.parquet")]

        with pytest.raises(ValueError, match="entidade"):
            svc.process_folder_upload(uploaded_files)

    def test_processes_all_uploaded_folder_parquets(
        self, services, sample_df, sample_df2
    ):
        svc, *_ = services
        uploaded_files = [
            FakeUploadedFile(sample_df, "clientes/parte1.parquet"),
            FakeUploadedFile(sample_df2, "clientes/2024/parte2.parquet"),
        ]

        result = svc.process_folder_upload(uploaded_files)

        assert isinstance(result, UploadResult)
        assert result.entity_name == "clientes"
        assert result.uploaded_files == 2
        assert result.batch_rows == len(sample_df) + len(sample_df2)
        assert result.total_rows == len(sample_df) + len(sample_df2)

    def test_uses_explicit_entity_name_when_provided(self, services, sample_df):
        svc, *_ = services
        uploaded_files = [FakeUploadedFile(sample_df, "qualquer_nome/parte1.parquet")]

        result = svc.process_folder_upload(uploaded_files, "Meus Clientes")

        assert result.entity_name == "meus_clientes"

    def test_merges_folder_with_existing_entity(self, services, sample_df, sample_df2):
        svc, *_ = services
        svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        uploaded_files = [FakeUploadedFile(sample_df2, "novos_clientes/parte2.parquet")]
        result = svc.process_folder_upload(uploaded_files, "clientes")

        assert result.total_rows == len(sample_df) + len(sample_df2)

    def test_raises_on_schema_mismatch_with_existing_entity(
        self, services, sample_df, different_schema_df
    ):
        svc, *_ = services
        svc.process_upload("clientes", [FakeUploadedFile(sample_df)])
        uploaded_files = [
            FakeUploadedFile(different_schema_df, "novos_clientes/parte2.parquet")
        ]

        with pytest.raises(ValueError, match="Schema"):
            svc.process_folder_upload(uploaded_files, "clientes")
