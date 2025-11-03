# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de Laudos (Óbitos Fetais)
# MAGIC 
# MAGIC Extrai laudos de ultrassom obstétrico com informações do paciente.

# COMMAND ----------

# MAGIC %run /Workspace/Libraries/Lake

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros
# MAGIC - Período configurável: informe datas no formato 'YYYY-MM-DD'
# MAGIC - Exemplo: início '2020-09-01' e fim '2020-10-01'

# COMMAND ----------

# Parâmetros do período (formato: 'YYYY-MM-DD')
PERIODO_INICIO = '2020-09-01'
PERIODO_FIM = '2020-10-01'

# Lista de códigos de procedimento (separados por vírgulas)
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

# COMMAND ----------

# Conectar ao datalake
connect_to_datalake(
    username="USR_PROD_INFORMATICA_SAUDE",
    password=dbutils.secrets.get(scope="INNOVATION_RAW", key="USR_PROD_INFORMATICA_SAUDE"),
    layer="RAWZN",
    level="LOW",
    dbx_secret_scope="INNOVATION_RAW" 
)

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

# COMMAND ----------

# Executar query
laudos_pd = run_sql(query)
if len(laudos_pd) == 0:
    print("Nenhum laudo encontrado para o período")
else:
    df = pd.DataFrame(laudos_pd)
    # Defensivo
    df = df[df['DS_LAUDO_MEDICO'].astype(str).str.strip().str.len() > 0]

    # COMMAND ----------

    # Exportar CSV (com compressão e particionamento se necessário)
    BASE_PATH = "/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/outputs"
    BASE_NAME = f"laudos_{PERIODO_INICIO[:7]}_{PERIODO_FIM[:7]}"

    CHUNK_SIZE = 200_000  # linhas por parte (ajuste conforme necessário)

    if len(df) <= CHUNK_SIZE:
        CSV_PATH = f"{BASE_PATH}/{BASE_NAME}.csv.gz"
        df.to_csv(
            CSV_PATH,
            index=False,
            encoding='utf-8-sig',
            sep=';',
            decimal=',',
            compression='gzip'
        )
        print(f"✅ CSV salvo: {CSV_PATH} | Registros: {len(df)} (gzip)")
    else:
        num_parts = (len(df) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for part_idx in range(num_parts):
            start = part_idx * CHUNK_SIZE
            end = __builtins__.min((part_idx + 1) * CHUNK_SIZE, len(df))
            part_df = df.iloc[start:end]
            CSV_PATH = f"{BASE_PATH}/{BASE_NAME}_part{part_idx + 1:02d}.csv.gz"
            part_df.to_csv(
                CSV_PATH,
                index=False,
                encoding='utf-8-sig',
                sep=';',
                decimal=',',
                compression='gzip'
            )
            print(f"✅ CSV salvo: {CSV_PATH} | Registros: {len(df)} (gzip)")

