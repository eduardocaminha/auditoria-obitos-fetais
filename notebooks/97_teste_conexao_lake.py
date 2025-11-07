# Databricks notebook source
# MAGIC %md
# MAGIC # Teste de Conexão ao Lake (Óbitos Fetais)
# MAGIC 
# MAGIC Este notebook simples conecta no Lake utilizando o helper padrão (`/Workspace/Libraries/Lake`) e executa uma query SQL arbitrária.
# MAGIC 
# MAGIC 1. Ajuste a constante `QUERY_SQL` com o SELECT desejado.
# MAGIC 2. (Opcional) Ajuste `FETCH_LIMIT` para controlar o número de linhas retornadas.
# MAGIC 3. Execute as células em ordem.

# COMMAND ----------

# MAGIC %run /Workspace/Libraries/Lake

# COMMAND ----------

from pprint import pprint
from datetime import datetime

# COMMAND ----------

# Configurações do teste — ajuste conforme necessário
QUERY_SQL = """
SELECT
    PREA.CD_ATENDIMENTO,
    PREA.CD_PROCEDIMENTO,
    PREA.DT_PROCEDIMENTO_REALIZADO
FROM RAWZN.RAW_HSP_TB_PROCEDIMENTO_REALIZADO PREA
WHERE PREA.DT_PROCEDIMENTO_REALIZADO >= DATE '2024-01-01'
FETCH FIRST 10 ROWS ONLY
"""

# Limite adicional aplicado no pandas (segurança)
FETCH_LIMIT = 50

print("=" * 80)
print("Configuração atual do teste de conexão")
print("=" * 80)
print(f"Horário de execução: {datetime.now().isoformat()}")
print("Consulta definida:")
print(QUERY_SQL.strip())
print(f"Limite adicional (pandas): {FETCH_LIMIT}")
print("=" * 80)

# COMMAND ----------

# Conectar ao Lake (usa o helper padrão com secrets armazenados no Databricks)
connect_to_datalake(
    username="USR_PROD_INFORMATICA_SAUDE",
    password=dbutils.secrets.get(scope="INNOVATION_RAW", key="USR_PROD_INFORMATICA_SAUDE"),
    layer="RAWZN",
    level="LOW",
    dbx_secret_scope="INNOVATION_RAW"
)

print("✅ Conexão estabelecida com sucesso.")

# COMMAND ----------

# Executar query
print("▶️ Executando query...")
resultado_pd = run_sql(QUERY_SQL)

if resultado_pd is None or len(resultado_pd) == 0:
    print("⚠️ Nenhum registro retornado pela consulta.")
else:
    print(f"✅ Consulta executada. Registros retornados: {len(resultado_pd):,}")
    # Exibir amostra respeitando o limite local
    display(resultado_pd.head(FETCH_LIMIT))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos passos
# MAGIC - Ajuste o SQL e execute novamente para testar outros cenários.
# MAGIC - Copie trechos da consulta diretamente daqui para reaproveitar no pipeline.
# MAGIC - Use `resultado_pd` para análises rápidas (ex.: `resultado_pd.describe()`).


