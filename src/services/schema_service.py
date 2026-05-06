from __future__ import annotations

import json
from pathlib import Path


# Campos monetários/númericos por natureza, mesmo quando o schema diz "string".
# O nome da coluna é em lowercase para comparação case-insensitive.
_MONETARY_FIELDS = {
    "responsiblepartyamount",
    "patientestimate",
    "insuranceestimate",
    "cost",
    "amount",
}

# Overrides por entidade: collection -> {coluna: tipo_pandas}
_ENTITY_TYPE_OVERRIDES: dict[str, dict[str, str]] = {
    "accounts_receivable": {
        "responsiblePartyAmount": "float64",
        "patientEstimate": "float64",
        "insuranceEstimate": "float64",
    },
    "appointment_items": {
        "cost": "float64",
    },
    "chargeable_item": {
        "cost": "float64",
    },
    "fee_schedule_chargeable_items": {
        "cost": "float64",
    },
    "ledger_entry": {
        "amount": "float64",
    },
    "treatment_plan_item": {
        "amount": "float64",
    },
}


class SchemaService:
    """Carrega curve-schema.json e fornece tipos pandas para leitura de CSV."""

    def __init__(self, schema_path: Path | None = None) -> None:
        if schema_path is None:
            # sobe de src/services/ até a raiz do projeto
            schema_path = Path(__file__).resolve().parents[2] / "curve-schema.json"
        self._schemas = self._load(schema_path)
        self._entity_map = {
            entry["collection"]: entry["schema"] for entry in self._schemas
        }

    def _load(self, path: Path) -> list[dict]:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)

    def has_entity(self, entity_name: str) -> bool:
        return entity_name in self._entity_map

    def get_pandas_dtypes(self, entity_name: str) -> dict[str, str] | None:
        """Retorna dicionário coluna->tipo para o argumento *dtype* do pd.read_csv.

        Regras de prioridade:
        1. Override explícito por entidade.
        2. Campos monetários globais (_MONETARY_FIELDS).
        3. Tipos fortes do schema (boolean, number, integer).
        4. string + format: date-time  -> não entra no dtype (usar parse_dates).
        5. string genérico              -> não entra no dtype (deixar pandas inferir).
        """
        schema = self._entity_map.get(entity_name)
        if not schema:
            return None

        properties = schema.get("properties", {})
        dtypes: dict[str, str] = {}
        overrides = _ENTITY_TYPE_OVERRIDES.get(entity_name, {})

        for col, spec in properties.items():
            json_type = spec.get("type")
            fmt = spec.get("format")
            col_lower = col.lower()

            if col in overrides:
                dtypes[col] = overrides[col]
                continue

            if col_lower in _MONETARY_FIELDS:
                dtypes[col] = "float64"
                continue

            if json_type == "boolean":
                dtypes[col] = "bool"
            elif json_type == "number":
                dtypes[col] = "float64"
            elif json_type == "integer":
                dtypes[col] = "Int64"
            # string + date-time -> omitir (tratado por parse_dates)
            # string genérico    -> omitir (inferência livre do pandas)

        return dtypes if dtypes else None

    def get_date_columns(self, entity_name: str) -> list[str] | None:
        """Retorna lista de colunas que devem ser parseadas como datetime."""
        schema = self._entity_map.get(entity_name)
        if not schema:
            return None

        properties = schema.get("properties", {})
        date_cols = [
            col
            for col, spec in properties.items()
            if spec.get("type") == "string" and spec.get("format") == "date-time"
        ]
        return date_cols if date_cols else None
