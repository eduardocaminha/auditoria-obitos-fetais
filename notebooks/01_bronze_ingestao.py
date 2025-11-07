# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingestão de Laudos
# MAGIC 
# MAGIC Extrai laudos de ultrassom obstétrico (HSP + PSC) diretamente do Lake e grava a camada Bronze em Delta.
# MAGIC Notebook pensado para execução manual/standalone.

# COMMAND ----------

# MAGIC %run /Workspace/Libraries/Lake

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import pandas as pd


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

# Parâmetros manuais (ajuste conforme necessário)
PERIODO_INICIO = '2025-10-01'
PERIODO_FIM = '2025-11-01'

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

# Tabela Delta Bronze e modo de gravação
BRONZE_TABLE = "innovation_dev.bronze.auditoria_obitos_fetais_raw"
BRONZE_WRITE_MODE = "overwrite"  # overwrite ou append

print("=" * 80)
print("CONFIGURAÇÃO BRONZE")
print("=" * 80)
print(f"Período: {PERIODO_INICIO} a {PERIODO_FIM}")
print(f"Tabela Bronze: {BRONZE_TABLE}")
print(f"Procedimentos: {len(CD_PROCEDIMENTO_LIST)}")
print(f"Modo de gravação: {BRONZE_WRITE_MODE}")
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
    PAC.NM_PACIENTE
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
    PAC.NM_PACIENTE
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

laudos_pd = run_sql(query)

if len(laudos_pd) == 0:
    print("⚠️ Nenhum laudo encontrado para o período informado.")
    df_bronze_pd = pd.DataFrame(columns=[
        'FONTE', 'CD_ATENDIMENTO', 'CD_OCORRENCIA', 'CD_ORDEM', 'CD_PROCEDIMENTO',
        'NM_PROCEDIMENTO', 'DS_LAUDO_MEDICO', 'DT_PROCEDIMENTO_REALIZADO',
        'CD_PACIENTE', 'NM_PACIENTE'
    ])
else:
    df_bronze_pd = pd.DataFrame(laudos_pd)
    df_bronze_pd = df_bronze_pd[
        df_bronze_pd['DS_LAUDO_MEDICO'].astype(str).str.strip().str.len() > 0
    ]

    total_exames = len(df_bronze_pd)
    pacientes_unicos = df_bronze_pd['CD_PACIENTE'].nunique()
    exames_por_fonte = df_bronze_pd.groupby('FONTE').size()

    print("=" * 80)
    print("ESTATÍSTICAS DA EXTRAÇÃO")
    print("=" * 80)
    print(f"Total de exames: {total_exames:,}")
    print(f"Pacientes únicos: {pacientes_unicos:,}")
    if pacientes_unicos > 0:
        print(f"Média de exames por paciente: {(total_exames / pacientes_unicos):.2f}")
    print("\nPor fonte:")
    for fonte, qtd in exames_por_fonte.items():
        print(f"  {fonte}: {qtd:,} exames")
    print("=" * 80)

if len(df_bronze_pd) > 0:
    df_bronze_pd['DT_PROCEDIMENTO_REALIZADO'] = pd.to_datetime(
        df_bronze_pd['DT_PROCEDIMENTO_REALIZADO'], errors='coerce'
    )

    df_bronze = spark.createDataFrame(df_bronze_pd)
    
    # Criar chave única para evitar duplicatas
    df_bronze = df_bronze.withColumn(
        "LAUDO_ID",
        F.concat(
            F.col("FONTE"),
            F.lit("_"),
            F.col("CD_ATENDIMENTO"),
            F.lit("_"),
            F.col("CD_OCORRENCIA"),
            F.lit("_"),
            F.col("CD_ORDEM")
        )
    )
    
    # Remover duplicatas baseado na chave única
    df_bronze = df_bronze.dropDuplicates(["LAUDO_ID"])
    
    df_bronze = df_bronze.withColumn("DT_INGESTAO", F.current_timestamp())

    print(f"💾 Gravando {df_bronze.count():,} registros na camada Bronze: {BRONZE_TABLE}")
    
    writer = df_bronze.write.format("delta").mode(BRONZE_WRITE_MODE)
    if BRONZE_WRITE_MODE == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(BRONZE_TABLE)
    
    spark.catalog.refreshTable(BRONZE_TABLE)
    
    # Estatísticas finais
    total = df_bronze.count()
    pacientes_unicos = df_bronze.select("CD_PACIENTE").distinct().count()
    
    print("=" * 80)
    print("ESTATÍSTICAS BRONZE")
    print("=" * 80)
    print(f"Total de laudos: {total:,}")
    print(f"Pacientes únicos: {pacientes_unicos:,}")
    print(f"Por fonte:")
    df_bronze.groupBy("FONTE").count().show()
    print("=" * 80)
else:
    try:
        spark.table(BRONZE_TABLE).limit(0)
        print("ℹ️ Nenhuma atualização aplicada; tabela Bronze permanece inalterada.")
    except AnalysisException:
        print("ℹ️ Tabela Bronze ainda não existe e não há dados para criá-la.")

