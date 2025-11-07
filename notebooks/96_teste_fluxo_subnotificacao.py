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
import pandas as pd
import unicodedata
import re

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
    # Núcleo (altamente específicos para óbito fetal)
    'P95',   # Morte fetal de causa não especificada
    'P96.4', # Morte neonatal precoce de causa não especificada (monitorar possíveis falsos positivos)
    'O36.4', # Cuidado materno por morte intrauterina de um ou mais fetos
    'O31.1', # Morte de um feto ou mais em gestação múltipla
    'O31.2', # Feto papiráceo (indica morte de co-gêmeo)
    'Z37.1', # Nascimento de um feto morto, único
    'Z37.3', # Gêmeos – um nascido vivo e um morto
    'Z37.4', # Gêmeos – ambos mortos
    'Z37.6', # Outros múltiplos – alguns vivos e outros mortos
    'Z37.7', # Outros múltiplos – todos mortos

    # Contexto forte (placenta/cordão) – manter em produção com validação adicional
    'O43.1', # Descolamento prematuro da placenta
    'O69.1', # Compressão do cordão umbilical
    'O69.2', # Prolapso do cordão umbilical
    'O69.3', # Circular de cordão com compressão
    'O69.8', # Outras complicações do cordão umbilical
    'O69.9', # Complicação não especificada do cordão umbilical

    # 'O36.5', # (avaliar disponibilidade local) cuidado materno por morte fetal tardia
    # 'P964',  # Transtorno respiratório do RN após anestesia materna – pouco específico para óbito fetal
    # 'P011',  # Feto afetado por placenta prévia – indicativo, porém não implica óbito
    # 'P021',  # Feto afetado por descolamento placentário – manter em monitoramento
    # 'P039',  # Complicações de cordão – cobertas por O69.*
    # 'P059',  # Transtornos de crescimento fetal – pouco específico
    # 'P969',  # Transtorno perinatal não especificado – alto risco de falso positivo
    # 'O365',  # Óbito fetal intraparto – substituído por O36.4/O31.*
    # 'O368',  # Problemas fetais especificados – revisar lista local antes de ativar
    # 'O3654', # Óbito fetal durante trabalho de parto – granularidade não disponível em todos os sistemas
    # 'Z370',  # Gestação única com nascido vivo – usar apenas para análises de contraste
]

# Tabela da auditoria oficial (ajuste se necessário)
AUDITORIA_TABLE = "RAWZN.TB_AUDITORIA_OBITO_ITEM"

print("Configuração carregada:")
print(f"  Período: {PERIODO_INICIO} -> {PERIODO_FIM}")
print(f"  Janela ±dias: {JANELA_DIAS}")
print(f"  Total de CIDs monitorados: {len(CID10_LIST)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Laudos obstétricos (extração direta do Lake)

# COMMAND ----------

# Lista de procedimentos obstétricos (mesma dos notebooks de extração manual)
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

procedimentos_csv = ", ".join(str(x) for x in CD_PROCEDIMENTO_LIST)

query_laudos = f"""
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

laudos_pd = run_sql(query_laudos)

if len(laudos_pd) == 0:
    print("⚠️ Nenhum laudo obstétrico encontrado para o período informado.")
    df_laudos_pd = pd.DataFrame(columns=[
        'FONTE', 'CD_ATENDIMENTO', 'CD_OCORRENCIA', 'CD_ORDEM', 'CD_PROCEDIMENTO',
        'NM_PROCEDIMENTO', 'DS_LAUDO_MEDICO', 'DT_PROCEDIMENTO_REALIZADO',
        'CD_PACIENTE', 'NM_PACIENTE'
    ])
else:
    df_laudos_pd = pd.DataFrame(laudos_pd)
    df_laudos_pd = df_laudos_pd[
        df_laudos_pd['DS_LAUDO_MEDICO'].astype(str).str.strip().str.len() > 0
    ]

    total_exames = len(df_laudos_pd)
    pacientes_unicos = df_laudos_pd['CD_PACIENTE'].nunique()
    exames_por_fonte = df_laudos_pd.groupby('FONTE').size()

    print("=" * 80)
    print("ESTATÍSTICAS DOS LAUDOS EXTRAÍDOS")
    print("=" * 80)
    print(f"Total de exames: {total_exames:,}")
    print(f"Pacientes únicos: {pacientes_unicos:,}")
    if pacientes_unicos > 0:
        print(f"Média de exames por paciente: {(total_exames / pacientes_unicos):.2f}")
    print("\nPor fonte:")
    for fonte, qtd in exames_por_fonte.items():
        print(f"  {fonte}: {qtd:,} exames")
    print("=" * 80)

# COMMAND ----------

# Normalização e classificação rápida (mesma lógica do processamento Silver)

def normalize_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'\s+', ' ', text)
    return text

def extract_semanas(text):
    pattern = r"(\d{1,2})\s*(?:semanas?|s(?:\s*\d+\s*d)?)"
    matches = re.findall(pattern, text)
    semanas = list(set([int(m) for m in matches if m.isdigit()]))
    return semanas

def has_ig_above_22_semanas(text):
    semanas = extract_semanas(text)
    if len(semanas) == 0:
        return False
    return any(sem >= 22 for sem in semanas)

patterns_obito = [
    (r"obito fetal", "óbito fetal"),
    (r"morte fetal", "morte fetal"),
    (r"obito intra.?uterino", "óbito intrauterino"),
    (r"feto morto", "feto morto"),
    (r"sem batimentos cardiacos fetais", "sem batimentos cardíacos fetais"),
    (r"ausencia de batimentos cardiacos fetais", "ausência de batimentos cardíacos fetais"),
    (r"batimentos cardiacos fetais nao (?:caracterizados|demonstrados|identificados)", "batimentos cardíacos fetais não caracterizados"),
    (r"sem atividade cardiaca fetal", "sem atividade cardíaca fetal"),
    (r"ausencia de atividade cardiaca fetal", "ausência de atividade cardíaca fetal"),
    (r"feto sem vitalidade", "feto sem vitalidade"),
    (r"sem movimentos fetais", "sem movimentos fetais"),
    (r"ausencia de movimentos fetais", "ausência de movimentos fetais"),
    (r"movimentos (?:corporeos|fetais) (?:e|e/ou)?.*nao (?:caracterizados|demonstrados|identificados)", "movimentos corpóreos/fetais não caracterizados"),
    (r"cessacao.*atividade cardiaca", "cessação de atividade cardíaca"),
    (r"morte do feto", "morte do feto"),
]

def classificar_obito_fetal(texto_norm, texto_original):
    match_encontrado = None
    pattern_match = None

    for pattern_tuple in patterns_obito:
        pattern = pattern_tuple[0]
        match = re.search(pattern, texto_norm)
        if match:
            match_encontrado = match
            pattern_match = pattern
            break

    if match_encontrado is None:
        return (0, None)

    if not has_ig_above_22_semanas(texto_norm):
        return (0, None)

    texto_original_str = str(texto_original)
    texto_original_norm = normalize_text(texto_original_str)

    match_original_norm = re.search(pattern_match, texto_original_norm)

    if match_original_norm:
        pos_inicio_norm = match_original_norm.start()
        pos_fim_norm = match_original_norm.end()
        len_original_norm = len(texto_original_norm)
        len_original = len(texto_original_str)

        if len_original_norm > 0:
            pos_inicio = int((pos_inicio_norm / len_original_norm) * len_original)
            pos_fim = int((pos_fim_norm / len_original_norm) * len_original)
        else:
            pos_inicio = 0
            pos_fim = len_original
    else:
        pos_inicio_norm = match_encontrado.start()
        pos_fim_norm = match_encontrado.end()
        len_norm = len(texto_norm)
        len_original = len(texto_original_str)

        if len_norm > 0:
            pos_inicio = int((pos_inicio_norm / len_norm) * len_original)
            pos_fim = int((pos_fim_norm / len_norm) * len_original)
        else:
            pos_inicio = 0
            pos_fim = len_original

    pos_inicio = max(0, pos_inicio)
    pos_fim = min(len(texto_original_str), pos_fim)

    contexto = 50
    inicio_contexto = max(0, pos_inicio - contexto)
    fim_contexto = min(len(texto_original_str), pos_fim + contexto)

    trecho_capturado = texto_original_str[inicio_contexto:fim_contexto].strip()

    return (1, trecho_capturado)

if len(df_laudos_pd) > 0:
    df_laudos_pd['texto_norm'] = df_laudos_pd['DS_LAUDO_MEDICO'].apply(normalize_text)
    df_laudos_pd['classificacao'] = df_laudos_pd.apply(
        lambda row: classificar_obito_fetal(row['texto_norm'], row['DS_LAUDO_MEDICO']), axis=1
    )
    df_laudos_pd['obito_fetal_clinico'] = df_laudos_pd['classificacao'].apply(lambda x: x[0])
    df_laudos_pd['termo_detectado'] = df_laudos_pd['classificacao'].apply(lambda x: x[1])

    df_laudos_pos_pd = df_laudos_pd[df_laudos_pd['obito_fetal_clinico'] == 1].copy()

    print(f"Laudos positivos identificados pelo classificador: {len(df_laudos_pos_pd):,}")
    display(df_laudos_pos_pd[['FONTE', 'CD_ATENDIMENTO', 'CD_PACIENTE', 'DT_PROCEDIMENTO_REALIZADO', 'termo_detectado']].head(10))
else:
    df_laudos_pos_pd = pd.DataFrame(columns=df_laudos_pd.columns)

colunas_renomear = {
    'FONTE': 'fonte',
    'CD_ATENDIMENTO': 'cd_atendimento',
    'CD_OCORRENCIA': 'cd_ocorrencia',
    'CD_ORDEM': 'cd_ordem',
    'CD_PROCEDIMENTO': 'cd_procedimento',
    'NM_PROCEDIMENTO': 'nm_procedimento',
    'DS_LAUDO_MEDICO': 'texto_original',
    'DT_PROCEDIMENTO_REALIZADO': 'dt_procedimento_realizado',
    'CD_PACIENTE': 'cd_paciente',
    'NM_PACIENTE': 'nm_paciente'
}

df_laudos_pos_pd = df_laudos_pos_pd.rename(columns=colunas_renomear)
df_laudos_pos_pd = df_laudos_pos_pd.drop(columns=['classificacao'], errors='ignore')

df_laudos = spark.createDataFrame(df_laudos_pos_pd)

# Preparar referência temporal
df_laudos = df_laudos.withColumn(
    "dt_referencia",
    F.to_timestamp('dt_procedimento_realizado')
)

df_laudos = df_laudos.filter(F.col('dt_referencia').isNotNull())

df_laudos.createOrReplaceTempView("vw_laudos_pos")

print(f"Laudos positivos com referência temporal: {df_laudos.count():,}")

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

, atendimentos_mae AS (
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
JOIN atendimentos_mae a
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


