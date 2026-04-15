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

Os dados consolidados ficam persistidos em um volume Docker nomeado, mesmo que o container seja parado ou recriado:

```bash
docker volume ls
docker volume inspect vibe_parquet_parquet_data
```

Para parar sem apagar os dados:

```bash
docker compose down
```

Para parar e apagar tambem o volume persistente:

```bash
docker compose down -v
```

## Como funciona

- Cada upload pertence a uma entidade.
- Arquivos enviados para a mesma entidade sao consolidados em um unico dataset.
- O app aceita upload de uma pasta inteira pelo navegador para ingerir todos os `.parquet`.
- O consolidado fica salvo em `data/merged/<entidade>/dataset.parquet`.
- A interface mostra tabela de colunas e preview dos dados.
