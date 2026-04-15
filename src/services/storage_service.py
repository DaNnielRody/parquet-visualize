from __future__ import annotations

from pathlib import Path


class StorageService:
    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path
        self.merged_root = self.base_path / "merged"

    def get_entity_dataset_path(self, entity_name: str) -> Path:
        return self.merged_root / entity_name / "dataset.parquet"

    def list_entities(self) -> list[str]:
        if not self.merged_root.exists():
            return []

        entities = [
            path.name
            for path in self.merged_root.iterdir()
            if path.is_dir() and (path / "dataset.parquet").exists()
        ]
        return sorted(entities)
