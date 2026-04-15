from __future__ import annotations

import pytest

from src.services.storage_service import StorageService


@pytest.fixture
def service(tmp_path) -> StorageService:
    return StorageService(tmp_path)


@pytest.fixture
def session_service(tmp_path) -> StorageService:
    return StorageService(tmp_path, "abc123")


class TestGetEntityDatasetPath:
    def test_returns_correct_path(self, service, tmp_path):
        path = service.get_entity_dataset_path("clientes")
        assert path == tmp_path / "merged" / "clientes" / "dataset.parquet"

    def test_does_not_create_directory(self, service, tmp_path):
        service.get_entity_dataset_path("clientes")
        assert not (tmp_path / "merged" / "clientes").exists()

    def test_returns_session_scoped_path(self, session_service, tmp_path):
        path = session_service.get_entity_dataset_path("clientes")
        assert (
            path
            == tmp_path
            / "sessions"
            / "abc123"
            / "merged"
            / "clientes"
            / "dataset.parquet"
        )


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


class TestSessionManagement:
    def test_ensure_session_root_creates_directory(self, session_service, tmp_path):
        path = session_service.ensure_session_root()
        assert path == tmp_path / "sessions" / "abc123"
        assert path.exists()

    def test_clear_session_data_removes_only_current_session(
        self, session_service, tmp_path
    ):
        current = tmp_path / "sessions" / "abc123"
        other = tmp_path / "sessions" / "other456"
        current.mkdir(parents=True)
        other.mkdir(parents=True)

        session_service.clear_session_data()

        assert not current.exists()
        assert other.exists()

    def test_cleanup_stale_sessions_ignores_current_session(
        self, session_service, tmp_path
    ):
        current = tmp_path / "sessions" / "abc123"
        stale = tmp_path / "sessions" / "old456"
        current.mkdir(parents=True)
        stale.mkdir(parents=True)

        session_service.cleanup_stale_sessions(0)

        assert current.exists()
        assert not stale.exists()
