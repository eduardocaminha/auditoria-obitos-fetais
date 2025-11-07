# Databricks notebook source
# MAGIC %md
# MAGIC # Protótipo: Detecção de Subnotificações de Óbito Fetal
# MAGIC 
# MAGIC Pipeline exploratório combinando laudos obstétricos positivos, diagnósticos por CID10 e auditoria oficial para investigar possíveis subnotificações.

# COMMAND ----------

# MAGIC %run /Workspace/Libraries/Lake

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurações

# COMMAND ----------

# Período analisado (ajuste conforme necessário)
PERIODO_INICIO = '2024-01-01'
PERIODO_FIM = '2024-01-31'

# Janela temporal (± em dias) para buscar outros atendimentos da mãe
JANELA_DIAS = 7

# Lista de CIDs associados a óbito fetal / eventos correlatos
CID10_LIST = [
    'P95',   # Morte fetal
    'P964',  # Outros transtornos respiratórios do recém-nascido após anestesia materna e analgésicos durante trabalho de parto e parto
    'P011',  # Feto e recém-nascido afetados por placenta prévia
    'P021',  # Feto e recém-nascido afetados por outras formas de descolamento placentário e hemorragia anteparto
    'P039',  # Feto e recém-nascido afetados por complicações de cordão umbilical, não especificadas
    'P059',  # Transtornos relacionados ao crescimento fetal não especificados
    'P969',  # Transtorno perinatal não especificado
    'O365',  # Óbito fetal intraparto
    'O368',  # Outros problemas fetais especificados
    'O3654', # Óbito fetal durante o trabalho de parto
    'Z371',  # Gestação única com óbito fetal
    'Z370'   # Gestação única com nascido vivo (usado para comparação / possíveis inconsistências)
]

# Tabela da auditoria oficial (ajuste se necessário)
AUDITORIA_TABLE = "RAWZN.TB_AUDITORIA_OBITO_ITEM"

print("Configuração carregada:")
print(f"  Período: {PERIODO_INICIO} -> {PERIODO_FIM}")
print(f"  Janela ±dias: {JANELA_DIAS}")
print(f"  Total de CIDs monitorados: {len(CID10_LIST)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Laudos obstétricos positivos (Silver)

# COMMAND ----------

cid_list_sql = ", ".join(f"'{cid}'" for cid in CID10_LIST)

df_laudos = spark.sql(f"""
SELECT
    fonte,
    cd_atendimento,
    cd_paciente,
    nm_paciente,
    dt_procedimento_realizado,
    dt_processamento,
    obito_fetal_clinico,
    termo_detectado
FROM innovation_dev.silver.auditoria_obitos_fetais_processado
WHERE obito_fetal_clinico = 1
  AND DATE(dt_procedimento_realizado) BETWEEN DATE '{PERIODO_INICIO}' AND DATE '{PERIODO_FIM}'
""")

if df_laudos.count() == 0:
    print("⚠️ Nenhum laudo positivo encontrado para o período informado.")
else:
    print(f"✅ Laudos positivos: {df_laudos.count():,}")
    display(df_laudos.select('fonte', 'cd_atendimento', 'cd_paciente', 'dt_procedimento_realizado', 'termo_detectado'))

# COMMAND ----------

# Preparar referência temporal (garantir timestamp)
df_laudos = df_laudos.withColumn(
    "dt_referencia",
    F.coalesce(
        F.to_timestamp('dt_procedimento_realizado'),
        F.to_timestamp('dt_processamento')
    )
)

df_laudos = df_laudos.filter(F.col('dt_referencia').isNotNull())

df_laudos.createOrReplaceTempView("vw_laudos_pos")

print(f"Laudos com referência temporal: {df_laudos.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Buscar atendimentos da mãe (janela ±7 dias)

# COMMAND ----------

query_atendimentos_mae = f"""
WITH laudos AS (
    SELECT DISTINCT
        cd_atendimento AS cd_atendimento_laud,
        cd_paciente AS cd_paciente_mae,
        nm_paciente,
        dt_referencia,
        termo_detectado
    FROM vw_laudos_pos
)

, atendimentos_mãe AS (
    SELECT 'HSP' AS fonte, tm.cd_atendimento, tm.cd_paciente, tm.cd_atendimento_mae, tm.dt_atendimento
    FROM RAWZN.RAW_HSP_TM_ATENDIMENTO tm
    UNION ALL
    SELECT 'PSC' AS fonte, tm.cd_atendimento, tm.cd_paciente, tm.cd_atendimento_mae, tm.dt_atendimento
    FROM RAWZN.RAW_PSC_TM_ATENDIMENTO tm
)

SELECT
    l.cd_atendimento_laud,
    l.cd_paciente_mae,
    l.nm_paciente,
    l.dt_referencia,
    l.termo_detectado,
    a.fonte,
    a.cd_atendimento AS cd_atendimento_mae,
    a.dt_atendimento AS dt_atendimento_mae
FROM laudos l
JOIN atendimentos_mãe a
  ON a.cd_paciente = l.cd_paciente_mae
 AND a.dt_atendimento BETWEEN l.dt_referencia - INTERVAL {JANELA_DIAS} DAYS
                           AND l.dt_referencia + INTERVAL {JANELA_DIAS} DAYS
"""

df_atendimentos_mae = spark.sql(query_atendimentos_mae)

print(f"Atendimentos da mãe encontrados (janela ±{JANELA_DIAS} dias): {df_atendimentos_mae.count():,}")
display(df_atendimentos_mae.limit(10))

df_atendimentos_mae.createOrReplaceTempView("vw_atendimentos_mae")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identificar registros de feto vinculados

# COMMAND ----------

query_fetos = """
WITH fetos AS (
    SELECT 'HSP' AS fonte, tm.cd_atendimento, tm.cd_paciente, tm.cd_atendimento_mae, tm.dt_atendimento
    FROM RAWZN.RAW_HSP_TM_ATENDIMENTO tm
    WHERE tm.cd_atendimento_mae IS NOT NULL
    UNION ALL
    SELECT 'PSC' AS fonte, tm.cd_atendimento, tm.cd_paciente, tm.cd_atendimento_mae, tm.dt_atendimento
    FROM RAWZN.RAW_PSC_TM_ATENDIMENTO tm
    WHERE tm.cd_atendimento_mae IS NOT NULL
)

SELECT
    mae.cd_atendimento_laud,
    mae.cd_paciente_mae,
    mae.nm_paciente,
    mae.dt_referencia,
    mae.fonte AS fonte_mae,
    mae.cd_atendimento_mae,
    mae.dt_atendimento_mae,
    f.fonte AS fonte_feto,
    f.cd_atendimento AS cd_atendimento_feto,
    f.cd_paciente AS cd_paciente_feto,
    f.dt_atendimento AS dt_atendimento_feto
FROM vw_atendimentos_mae mae
LEFT JOIN fetos f
  ON f.cd_atendimento_mae = mae.cd_atendimento_mae
"""

df_vinculos = spark.sql(query_fetos)

df_vinculos = df_vinculos.withColumn(
    "possui_registro_feto",
    F.when(F.col('cd_atendimento_feto').isNotNull(), F.lit(True)).otherwise(F.lit(False))
)

display(df_vinculos.limit(10))

print("Resumo vínculos mãe ⇄ feto:")
display(
    df_vinculos.groupBy('possui_registro_feto').count()
)

df_vinculos.createOrReplaceTempView("vw_vinculos_mae_feto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Diagnósticos por CID10

# COMMAND ----------

cid_sql_list = ", ".join(f"'{cid}'" for cid in CID10_LIST)

query_cid = f"""
SELECT
    diag.fonte,
    diag.cd_atendimento,
    diag.cd_paciente,
    diag.cd_cid10,
    diag.dt_referencia
FROM (
    SELECT
        'HSP' AS fonte,
        cd_atendimento,
        cd_paciente,
        cd_cid10,
        COALESCE(dt_diagnostico, dt_atendimento) AS dt_referencia,
        NVL(fl_validado, 'S') AS fl_validado
    FROM RAWZN.RAW_HSP_TB_DIAGNOSTICO_ATENDIMENTO
    WHERE cd_cid10 IN ({cid_sql_list})
      AND COALESCE(dt_diagnostico, dt_atendimento) BETWEEN DATE '{PERIODO_INICIO}' AND DATE '{PERIODO_FIM}' + 1 - INTERVAL '1' SECOND

    UNION ALL

    SELECT
        'PSC' AS fonte,
        cd_atendimento,
        cd_paciente,
        cd_cid10,
        COALESCE(dt_diagnostico, dt_atendimento) AS dt_referencia,
        NVL(fl_validado, 'S') AS fl_validado
    FROM RAWZN.RAW_PSC_TB_DIAGNOSTICO_ATENDIMENTO
    WHERE cd_cid10 IN ({cid_sql_list})
      AND COALESCE(dt_diagnostico, dt_atendimento) BETWEEN DATE '{PERIODO_INICIO}' AND DATE '{PERIODO_FIM}' + 1 - INTERVAL '1' SECOND
) diag
WHERE diag.fl_validado = 'S'
"""

df_cid = spark.sql(query_cid)

print(f"Diagnósticos com CIDs relevantes: {df_cid.count():,}")
display(df_cid.limit(10))

df_cid.createOrReplaceTempView("vw_cid_obito")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Consolidação (Laudo + CID) e checagem na auditoria

# COMMAND ----------

# Consolidação: full outer join por paciente/atendimento
df_laudos_chave = df_vinculos.select(
    'cd_paciente_mae',
    'cd_atendimento_mae',
    'cd_atendimento_feto',
    'dt_referencia',
    'possui_registro_feto'
).distinct()

df_cid_chave = df_cid.select(
    F.col('cd_paciente').alias('cid_cd_paciente'),
    F.col('cd_atendimento').alias('cid_cd_atendimento'),
    'cd_cid10',
    'dt_referencia'
).distinct()

df_merged = df_laudos_chave.join(
    df_cid_chave,
    (df_laudos_chave.cd_paciente_mae == df_cid_chave.cid_cd_paciente) &
    (df_laudos_chave.cd_atendimento_mae == df_cid_chave.cid_cd_atendimento),
    how='full_outer'
)

df_merged = df_merged.withColumn(
    'fonte_laudo',
    F.when(F.col('cd_atendimento_mae').isNotNull(), F.lit(True)).otherwise(F.lit(False))
).withColumn(
    'fonte_cid',
    F.when(F.col('cid_cd_atendimento').isNotNull(), F.lit(True)).otherwise(F.lit(False))
)

df_merged = df_merged.withColumn(
    'mae_sem_feto',
    F.when(
        F.col('fonte_laudo') & (F.col('cd_atendimento_feto').isNull()),
        F.lit(True)
    ).otherwise(F.lit(False))
)

display(df_merged.limit(20))

print("Resumo das combinações Laudo x CID:")
display(
    df_merged.groupBy('fonte_laudo', 'fonte_cid').count()
)

# COMMAND ----------

# Checar presença na tabela de auditoria oficial

df_atendimentos_unicos = df_merged.select(
    F.coalesce('cd_atendimento_mae', 'cid_cd_atendimento').alias('cd_atendimento')
).filter(F.col('cd_atendimento').isNotNull()).distinct()

df_atendimentos_unicos.createOrReplaceTempView("vw_atendimentos_alvo")

query_auditoria = f"""
SELECT
    alvo.cd_atendimento,
    CASE WHEN audit.cd_atendimento IS NOT NULL THEN 'SIM' ELSE 'NAO' END AS na_auditoria
FROM vw_atendimentos_alvo alvo
LEFT JOIN {AUDITORIA_TABLE} audit
  ON audit.cd_atendimento = alvo.cd_atendimento
"""

df_auditoria_flag = spark.sql(query_auditoria)

df_resultado = df_merged.join(
    df_auditoria_flag,
    df_merged.cd_atendimento_mae == df_auditoria_flag.cd_atendimento,
    how='left'
).drop(df_auditoria_flag.cd_atendimento)

df_resultado = df_resultado.withColumn(
    'na_auditoria',
    F.coalesce('na_auditoria', F.lit('NAO'))
)

display(df_resultado.limit(20))

print("Distribuição por presença na auditoria:")
display(
    df_resultado.groupBy('na_auditoria').count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Visões auxiliares

# COMMAND ----------

print("Mães sem registro de feto correspondente:")
display(
    df_resultado.filter('mae_sem_feto = true').select('cd_atendimento_mae', 'cd_paciente_mae', 'cd_atendimento_feto', 'na_auditoria')
)

print("Registros encontrando CID mas sem laudo positivo vinculado:")
display(
    df_resultado.filter('fonte_cid = true AND fonte_laudo = false')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exportar resultados (opcional)

# COMMAND ----------

from pyspark.sql import DataFrame

def exportar_para_delta(df: DataFrame, path: str, mode: str = "overwrite"):
    """Salva DataFrame no formato Delta para consultas posteriores."""
    df.write.format("delta").mode(mode).save(path)

# Exemplo (comente se não desejar exportar durante os testes)
# exportar_para_delta(df_resultado, "/mnt/datalake/dev/obitos_fetais/subnotificacao/resultado")

print("Fluxo completo executado.")


