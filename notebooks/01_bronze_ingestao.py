# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingestão de Laudos
# MAGIC 
# MAGIC Extrai laudos de ultrassom obstétrico (HSP + PSC) e salva em Delta Lake.
# MAGIC 
# MAGIC **Execução**: Diária (job Databricks)
# MAGIC **Tabela**: `innovation_dev.bronze.auditoria_obitos_fetais_raw`

# COMMAND ----------

# MAGIC %run /Workspace/Libraries/Lake

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import sys

# Adicionar diretório do projeto ao path
sys.path.append('/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/notebooks')

spark = SparkSession.getActiveSession()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

# Parâmetros do período (último dia por padrão)
# Para job diário, processa apenas o dia anterior
DATA_PROCESSAMENTO = datetime.now() - timedelta(days=1)
PERIODO_INICIO = DATA_PROCESSAMENTO.strftime('%Y-%m-%d')
PERIODO_FIM = datetime.now().strftime('%Y-%m-%d')

# Lista de códigos de procedimento (ultrassom obstétrico)
CD_PROCEDIMENTO_LIST = [
    33010110, 33010250, 33010269, 33010285,
    33010295, 33010293, 40901238, 40901246,
    40901505, 33010390, 33010501, 33020019,
    99030250, 99030293, 33010360, 33019061,
    33999901, 98409220, 98224063, 98409031,
    98409043, 90020251, 33010382, 40901254,
    40901289, 40901297, 40901262, 33010307,
    40902013, 40901270, 33010609, 40902021,
    99030110, 99030111, 98409145, 98409029,
    98409033, 98409239, 98409030, 33010375,
]

# Tabela Delta Bronze
BRONZE_TABLE = "innovation_dev.bronze.auditoria_obitos_fetais_raw"

print("=" * 80)
print("CONFIGURAÇÃO BRONZE")
print("=" * 80)
print(f"Período: {PERIODO_INICIO} a {PERIODO_FIM}")
print(f"Tabela Bronze: {BRONZE_TABLE}")
print(f"Procedimentos: {len(CD_PROCEDIMENTO_LIST)}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conexão com Datalake

# COMMAND ----------

connect_to_datalake(
    username="USR_PROD_INFORMATICA_SAUDE",
    password=dbutils.secrets.get(scope="INNOVATION_RAW", key="USR_PROD_INFORMATICA_SAUDE"),
    layer="RAWZN",
    level="LOW",
    dbx_secret_scope="INNOVATION_RAW" 
)

print("✅ Conexão com datalake estabelecida!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extração de Laudos (HSP + PSC)

# COMMAND ----------

procedimentos_csv = ", ".join(str(x) for x in CD_PROCEDIMENTO_LIST)

query = f"""
SELECT 
    'HSP' AS FONTE,
    PREA.CD_ATENDIMENTO,
    PREA.CD_OCORRENCIA,
    PREA.CD_ORDEM,
    PREA.CD_PROCEDIMENTO,
    P.NM_PROCEDIMENTO,
    LAUP.DS_LAUDO_MEDICO,
    PREA.DT_PROCEDIMENTO_REALIZADO,
    ATE.CD_PACIENTE,
    PAC.NM_PACIENTE,
    CURRENT_TIMESTAMP AS DT_INGESTAO
FROM RAWZN.RAW_HSP_TB_PROCEDIMENTO_REALIZADO PREA
INNER JOIN RAWZN.RAW_HSP_TB_PROCEDIMENTO P
    ON PREA.CD_PROCEDIMENTO = P.CD_PROCEDIMENTO
INNER JOIN RAWZN.RAW_HSP_TB_LAUDO_PACIENTE LAUP 
    ON PREA.CD_ATENDIMENTO = LAUP.CD_ATENDIMENTO 
    AND PREA.CD_OCORRENCIA = LAUP.CD_OCORRENCIA 
    AND PREA.CD_ORDEM = LAUP.CD_ORDEM
INNER JOIN RAWZN.RAW_HSP_TM_ATENDIMENTO ATE
    ON PREA.CD_ATENDIMENTO = ATE.CD_ATENDIMENTO
INNER JOIN RAWZN.RAW_HSP_TB_PACIENTE PAC
    ON ATE.CD_PACIENTE = PAC.CD_PACIENTE
WHERE PREA.CD_PROCEDIMENTO IN ({procedimentos_csv})
AND PREA.DT_PROCEDIMENTO_REALIZADO >= DATE '{PERIODO_INICIO}'
AND PREA.DT_PROCEDIMENTO_REALIZADO < DATE '{PERIODO_FIM}'
AND LAUP.DS_LAUDO_MEDICO IS NOT NULL

UNION ALL

SELECT 
    'PSC' AS FONTE,
    PREA.CD_ATENDIMENTO,
    PREA.CD_OCORRENCIA,
    PREA.CD_ORDEM,
    PREA.CD_PROCEDIMENTO,
    P.NM_PROCEDIMENTO,
    LAUP.DS_LAUDO_MEDICO,
    PREA.DT_PROCEDIMENTO_REALIZADO,
    ATE.CD_PACIENTE,
    PAC.NM_PACIENTE,
    CURRENT_TIMESTAMP AS DT_INGESTAO
FROM RAWZN.RAW_PSC_TB_PROCEDIMENTO_REALIZADO PREA
INNER JOIN RAWZN.RAW_PSC_TB_PROCEDIMENTO P
    ON PREA.CD_PROCEDIMENTO = P.CD_PROCEDIMENTO
INNER JOIN RAWZN.RAW_PSC_TB_LAUDO_PACIENTE LAUP 
    ON PREA.CD_ATENDIMENTO = LAUP.CD_ATENDIMENTO 
    AND PREA.CD_OCORRENCIA = LAUP.CD_OCORRENCIA 
    AND PREA.CD_ORDEM = LAUP.CD_ORDEM
INNER JOIN RAWZN.RAW_PSC_TM_ATENDIMENTO ATE
    ON PREA.CD_ATENDIMENTO = ATE.CD_ATENDIMENTO
INNER JOIN RAWZN.RAW_PSC_TB_PACIENTE PAC
    ON ATE.CD_PACIENTE = PAC.CD_PACIENTE
WHERE PREA.CD_PROCEDIMENTO IN ({procedimentos_csv})
AND PREA.DT_PROCEDIMENTO_REALIZADO >= DATE '{PERIODO_INICIO}'
AND PREA.DT_PROCEDIMENTO_REALIZADO < DATE '{PERIODO_FIM}'
AND LAUP.DS_LAUDO_MEDICO IS NOT NULL
"""

# Executar query (retorna pandas)
df_pandas = run_sql(query)

# Converter para Spark DataFrame
df_spark = spark.createDataFrame(df_pandas)

print(f"✅ Laudos extraídos: {df_spark.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Chave Única e Limpar

# COMMAND ----------

# Criar chave única para evitar duplicatas
df_spark = df_spark.withColumn(
    "LAUDO_ID",
    concat(
        col("FONTE"),
        lit("_"),
        col("CD_ATENDIMENTO"),
        lit("_"),
        col("CD_OCORRENCIA"),
        lit("_"),
        col("CD_ORDEM")
    )
)

# Filtrar laudos vazios
df_spark = df_spark.filter(
    length(trim(col("DS_LAUDO_MEDICO"))) > 0
)

print(f"✅ Laudos válidos após limpeza: {df_spark.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvar em Delta Lake (Bronze)

# Salvar em Delta Lake com append (evita duplicatas pela chave)
df_spark.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"✅ Dados salvos em {BRONZE_TABLE}")
print(f"   Total de registros: {df_spark.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Estatísticas

# COMMAND ----------

total = df_spark.count()
pacientes_unicos = df_spark.select("CD_PACIENTE").distinct().count()

print("=" * 80)
print("ESTATÍSTICAS BRONZE")
print("=" * 80)
print(f"Total de laudos: {total}")
print(f"Por fonte:")
df_spark.groupBy("FONTE").count().show()
print(f"Pacientes únicos: {pacientes_unicos}")
print("=" * 80)

