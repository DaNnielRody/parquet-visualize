from __future__ import annotations

import shutil
import time
from pathlib import Path


class StorageService:
    def __init__(self, base_path: Path, session_id: str | None = None) -> None:
        self.base_path = base_path
        self.session_id = session_id
        self.session_root = (
            self.base_path / "sessions" / session_id if session_id else self.base_path
        )
        self.merged_root = self.session_root / "merged"

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

    def ensure_session_root(self) -> Path:
        self.session_root.mkdir(parents=True, exist_ok=True)
        return self.session_root

    def clear_session_data(self) -> None:
        if self.session_root.exists():
            shutil.rmtree(self.session_root)

    def cleanup_stale_sessions(self, max_age_seconds: int) -> None:
        sessions_root = self.base_path / "sessions"
        if not sessions_root.exists():
            return

        now = time.time()
        for session_path in sessions_root.iterdir():
            if not session_path.is_dir():
                continue
            if self.session_id and session_path.name == self.session_id:
                continue
            age_seconds = now - session_path.stat().st_mtime
            if age_seconds > max_age_seconds:
                shutil.rmtree(session_path, ignore_errors=True)
