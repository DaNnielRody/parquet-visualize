# Parquet Entity Uploader

Aplicativo em Streamlit para upload de multiplos arquivos Parquet por entidade.

## Como rodar

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Como rodar com Docker

```bash
docker compose up --build
```

O app fica disponivel em `http://localhost:8501`.

Os dados enviados ficam isolados por sessao de usuario e sao gravados em armazenamento temporario do container. Eles nao devem ser tratados como persistentes.

## Como funciona

- Cada upload pertence a uma entidade.
- Arquivos enviados para a mesma entidade sao consolidados em um unico dataset dentro da sessao atual.
- O app aceita upload de uma pasta inteira pelo navegador para ingerir todos os `.parquet`.
- O consolidado fica salvo temporariamente em um diretorio isolado da sessao, dentro de `/tmp/vibe_parquet/sessions/<session_id>/merged/<entidade>/dataset.parquet`.
- A interface mostra tabela de colunas e preview dos dados.
- Cada sessao enxerga apenas os proprios arquivos.
- A sessao pode ser limpa manualmente pela interface, e sessoes antigas sao removidas automaticamente por expiracao.
