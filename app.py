from __future__ import annotations

import ast
import json
import tempfile
import uuid
from pathlib import Path

import pandas as pd
import streamlit as st

from src.services.parquet_service import ParquetService
from src.services.storage_service import StorageService
from src.services.upload_service import UploadService


parquet_service = ParquetService()
DATA_ROOT = Path(tempfile.gettempdir()) / "vibe_parquet"
SESSION_TTL_SECONDS = 60 * 60


def _get_session_id() -> str:
    session_id = st.session_state.get("session_id")
    if session_id is None:
        session_id = uuid.uuid4().hex
        st.session_state["session_id"] = session_id
    return session_id


def _build_services() -> tuple[StorageService, UploadService]:
    storage_service = StorageService(DATA_ROOT, _get_session_id())
    storage_service.ensure_session_root()
    storage_service.cleanup_stale_sessions(SESSION_TTL_SECONDS)
    upload_service = UploadService(storage_service, parquet_service)
    return storage_service, upload_service


def _init_upload_state() -> None:
    st.session_state.setdefault("session_cleared", False)
    st.session_state.setdefault("upload_entity_name", "")
    st.session_state.setdefault("upload_folder_entity_name", "")
    st.session_state.setdefault("upload_files_key", 0)
    st.session_state.setdefault("latest_upload_result", None)
    st.session_state.setdefault("pending_upload_reset", False)

    if st.session_state["pending_upload_reset"]:
        st.session_state["upload_entity_name"] = ""
        st.session_state["upload_folder_entity_name"] = ""
        st.session_state["pending_upload_reset"] = False


def _store_upload_result(result) -> None:
    st.session_state["latest_upload_result"] = result
    st.session_state["pending_upload_reset"] = True
    st.session_state["upload_files_key"] += 1


def _render_latest_upload_result() -> None:
    result = st.session_state.get("latest_upload_result")
    if result is None:
        return

    st.success(f"Entidade '{result.entity_name}' atualizada com sucesso.")
    col1, col2, col3 = st.columns(3)
    col1.metric("Arquivos no lote", result.uploaded_files)
    col2.metric("Linhas no lote", result.batch_rows)
    col3.metric("Linhas totais", result.total_rows)
    st.caption(f"Arquivo salvo em: `{result.saved_path}`")

    st.markdown("**Colunas da entidade**")
    st.dataframe(result.schema_table, use_container_width=True, hide_index=True)

    st.markdown("**Preview dos dados consolidados**")
    _render_data_preview(result.preview_df)


def _normalize_preview_value(value):
    if pd.isna(value):
        return None

    if isinstance(value, (dict, list)):
        return _normalize_nested_value(value)

    if isinstance(value, str):
        parsed = _try_parse_structured_string(value)
        if parsed is not None:
            return _normalize_nested_value(parsed)
        return value

    return value


def _normalize_nested_value(value):
    if isinstance(value, dict):
        return {
            str(key): _normalize_nested_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_normalize_nested_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_nested_value(item) for item in value]
    if pd.isna(value):
        return None
    if isinstance(value, str):
        parsed = _try_parse_structured_string(value)
        if parsed is not None:
            return _normalize_nested_value(parsed)
    return value


def _try_parse_structured_string(value: str):
    stripped = value.strip()
    if not stripped:
        return None

    if stripped[0] not in "[{":
        return None

    try:
        return json.loads(stripped)
    except (json.JSONDecodeError, TypeError):
        pass

    try:
        return ast.literal_eval(stripped)
    except (ValueError, SyntaxError):
        return None


def _build_json_preview(dataframe: pd.DataFrame, limit: int = 25) -> list[dict]:
    preview_df = dataframe.head(limit)
    records = []
    for row in preview_df.to_dict(orient="records"):
        records.append(
            {
                str(column): _normalize_preview_value(value)
                for column, value in row.items()
            }
        )
    return records


def _render_data_preview(dataframe: pd.DataFrame) -> None:
    tab_structured, tab_table = st.tabs(["Estruturado", "Tabela bruta"])
    with tab_structured:
        st.json(_build_json_preview(dataframe), expanded=2)
    with tab_table:
        st.dataframe(dataframe.head(100), use_container_width=True, hide_index=True)


def _clear_session(storage_service: StorageService) -> None:
    storage_service.clear_session_data()
    st.session_state["latest_upload_result"] = None
    st.session_state["pending_upload_reset"] = False
    st.session_state["upload_entity_name"] = ""
    st.session_state["upload_folder_entity_name"] = ""
    st.session_state["upload_files_key"] += 1
    st.session_state["session_cleared"] = True
    st.rerun()


def render_upload_section(storage_service: StorageService, upload_service: UploadService) -> None:
    _init_upload_state()
    st.subheader("Upload por entidade")
    st.write(
        "Envie varios arquivos `.parquet` para a mesma entidade. "
        "Os dados ficam isolados nesta sessao e nao sao compartilhados com outros usuarios."
    )
    st.caption("Os arquivos desta sessao sao efemeros e sao apagados ao limpar ou expirar a sessao.")

    if st.session_state.get("session_cleared"):
        st.info("Os dados temporarios desta sessao foram removidos.")
        st.session_state["session_cleared"] = False

    if st.button("Apagar dados desta sessao", type="secondary"):
        _clear_session(storage_service)

    _render_latest_upload_result()

    form_files, form_folder = st.columns(2)

    with form_files:
        with st.form("upload-files-form"):
            entity_name = st.text_input(
                "Entidade",
                key="upload_entity_name",
                placeholder="Ex.: clientes, pedidos, faturamento",
            )
            uploaded_files = st.file_uploader(
                "Arquivos parquet",
                type=["parquet"],
                accept_multiple_files=True,
                key=f"upload_files_{st.session_state['upload_files_key']}",
            )
            submitted_files = st.form_submit_button("Processar arquivos")

        if submitted_files:
            try:
                result = upload_service.process_upload(entity_name, uploaded_files)
            except Exception as exc:
                st.error(str(exc))
            else:
                _store_upload_result(result)
                st.rerun()

    with form_folder:
        with st.form("upload-folder-form"):
            folder_entity_name = st.text_input(
                "Entidade (opcional)",
                key="upload_folder_entity_name",
                placeholder="Se vazio, usa o nome da pasta.",
            )
            uploaded_folder_files = st.file_uploader(
                "Upload de pasta",
                type=["parquet"],
                accept_multiple_files="directory",
                key=f"upload_folder_files_{st.session_state['upload_files_key']}",
            )
            submitted_folder = st.form_submit_button("Processar pasta")

        st.caption(
            "Ao processar uma pasta, o nome da entidade e opcional. "
            "Se nao for informado, o app usa o nome da pasta enviada."
        )

        if submitted_folder:
            try:
                result = upload_service.process_folder_upload(
                    uploaded_folder_files, folder_entity_name
                )
            except Exception as exc:
                st.error(str(exc))
            else:
                _store_upload_result(result)
                st.rerun()


def render_entities_section(storage_service: StorageService, upload_service: UploadService) -> None:
    st.subheader("Entidades consolidadas")
    entities = storage_service.list_entities()
    if not entities:
        st.info("Nenhuma entidade disponivel nesta sessao.")
        return

    selected_entity = st.selectbox("Selecione uma entidade", entities)
    dataframe, schema_table = upload_service.load_entity_view(selected_entity)

    col1, col2, col3 = st.columns(3)
    col1.metric("Linhas", int(len(dataframe)))
    col2.metric("Colunas", int(len(dataframe.columns)))
    col3.metric("Preview", min(len(dataframe), 100))

    st.markdown("**Tabela de colunas**")
    st.dataframe(schema_table, use_container_width=True, hide_index=True)

    st.markdown("**Dados da entidade**")
    _render_data_preview(dataframe)


def main() -> None:
    st.set_page_config(page_title="Parquet Entity Uploader", layout="wide")
    storage_service, upload_service = _build_services()
    st.title("Parquet Entity Uploader")
    st.caption(
        "Upload multiplo de arquivos parquet por entidade, com isolamento por sessao e visualizacao tabular."
    )

    tab_upload, tab_entities = st.tabs(["Upload", "Entidades"])
    with tab_upload:
        render_upload_section(storage_service, upload_service)
    with tab_entities:
        render_entities_section(storage_service, upload_service)


if __name__ == "__main__":
    main()
