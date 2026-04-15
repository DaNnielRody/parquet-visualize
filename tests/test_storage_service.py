from __future__ import annotations

import pytest

from src.services.storage_service import StorageService


@pytest.fixture
def service(tmp_path) -> StorageService:
    return StorageService(tmp_path)


class TestGetEntityDatasetPath:
    def test_returns_correct_path(self, service, tmp_path):
        path = service.get_entity_dataset_path("clientes")
        assert path == tmp_path / "merged" / "clientes" / "dataset.parquet"

    def test_does_not_create_directory(self, service, tmp_path):
        service.get_entity_dataset_path("clientes")
        assert not (tmp_path / "merged" / "clientes").exists()


class TestListEntities:
    def test_empty_when_no_merged_dir(self, service):
        assert service.list_entities() == []

    def test_empty_when_no_datasets(self, service, tmp_path):
        (tmp_path / "merged" / "clientes").mkdir(parents=True)
        assert service.list_entities() == []

    def test_lists_entities_with_dataset(self, service, tmp_path):
        for name in ["clientes", "pedidos"]:
            path = tmp_path / "merged" / name
            path.mkdir(parents=True)
            (path / "dataset.parquet").touch()
        entities = service.list_entities()
        assert entities == ["clientes", "pedidos"]

    def test_sorted_alphabetically(self, service, tmp_path):
        for name in ["zebra", "alpha", "middle"]:
            path = tmp_path / "merged" / name
            path.mkdir(parents=True)
            (path / "dataset.parquet").touch()
        assert service.list_entities() == ["alpha", "middle", "zebra"]

    def test_ignores_dirs_without_dataset(self, service, tmp_path):
        (tmp_path / "merged" / "empty").mkdir(parents=True)
        valid = tmp_path / "merged" / "valid"
        valid.mkdir(parents=True)
        (valid / "dataset.parquet").touch()
        assert service.list_entities() == ["valid"]
