# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Processamento e Classificação
# MAGIC 
# MAGIC Lê a camada Bronze, classifica os laudos e grava a Silver.
# MAGIC Notebook preparado para execuções stand-alone.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
import pandas as pd
import unicodedata
import re

# Padrões textuais de óbito fetal (mesmos utilizados no protótipo)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

BRONZE_TABLE = "innovation_dev.bronze.auditoria_obitos_fetais_raw"
SILVER_TABLE = "innovation_dev.silver.auditoria_obitos_fetais_processado"
SILVER_WRITE_MODE = "overwrite"  # Troque para "append" se necessário

print("=" * 80)
print("CONFIGURAÇÃO SILVER")
print("=" * 80)
print(f"Tabela Bronze: {BRONZE_TABLE}")
print(f"Tabela Silver: {SILVER_TABLE}")
print(f"Modo de gravação: {SILVER_WRITE_MODE}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ler Bronze

# COMMAND ----------

try:
    bronze_df = spark.table(BRONZE_TABLE)
except AnalysisException:
    print(f"⚠️ Tabela Bronze {BRONZE_TABLE} não encontrada. Execute a ingestão antes de processar a Silver.")
    bronze_pd = pd.DataFrame()
else:
    bronze_pd = bronze_df.toPandas()

if bronze_pd.empty:
    print("⚠️ Nenhum dado disponível para processamento.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções de Classificação

# COMMAND ----------

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

print("✅ Funções de classificação definidas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aplicar Classificação

# COMMAND ----------

if not bronze_pd.empty:
    bronze_pd = bronze_pd[
        bronze_pd['DS_LAUDO_MEDICO'].astype(str).str.strip().str.len() > 0
    ]

    print(f"📋 Registros válidos carregados: {len(bronze_pd):,}")

    bronze_pd['texto_norm'] = bronze_pd['DS_LAUDO_MEDICO'].apply(normalize_text)
    bronze_pd['classificacao'] = bronze_pd.apply(
        lambda row: classificar_obito_fetal(row['texto_norm'], row['DS_LAUDO_MEDICO']), axis=1
    )
    bronze_pd['obito_fetal_clinico'] = bronze_pd['classificacao'].apply(lambda x: x[0])
    bronze_pd['termo_detectado'] = bronze_pd['classificacao'].apply(lambda x: x[1])

    total_positivos = (bronze_pd['obito_fetal_clinico'] == 1).sum()
    print(f"✅ Classificação concluída: {total_positivos:,} laudos positivos encontrados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transformação e Deduplicação

# COMMAND ----------

if not bronze_pd.empty:
    silver_pd = bronze_pd[bronze_pd['obito_fetal_clinico'] == 1].copy()
    
    # Remover duplicatas baseado em LAUDO_ID (se existir) ou criar chave única
    if 'LAUDO_ID' in silver_pd.columns:
        silver_pd = silver_pd.drop_duplicates(subset=['LAUDO_ID'], keep='first')
    else:
        silver_pd['LAUDO_ID'] = (
            silver_pd['FONTE'].astype(str) + '_' +
            silver_pd['CD_ATENDIMENTO'].astype(str) + '_' +
            silver_pd['CD_OCORRENCIA'].astype(str) + '_' +
            silver_pd['CD_ORDEM'].astype(str)
        )
        silver_pd = silver_pd.drop_duplicates(subset=['LAUDO_ID'], keep='first')

    print(f"📊 Laudos após deduplicação: {len(silver_pd):,}")

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

    silver_pd = silver_pd.rename(columns=colunas_renomear)
    silver_pd = silver_pd.drop(columns=['classificacao', 'texto_norm'], errors='ignore')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gravação Silver

# COMMAND ----------

if not bronze_pd.empty:
    if silver_pd.empty:
        print("ℹ️ Nenhum laudo positivo para gravar")
    else:
        # Converter tipos com segurança
        silver_pd['dt_procedimento_realizado'] = pd.to_datetime(
            silver_pd['dt_procedimento_realizado'], errors='coerce'
        )
        
        # Garantir tipos string nas colunas de código
        for col in ['cd_atendimento', 'cd_ocorrencia', 'cd_ordem', 'cd_procedimento', 'cd_paciente']:
            if col in silver_pd.columns:
                silver_pd[col] = silver_pd[col].astype(str)

        # Selecionar apenas as colunas necessárias e garantir ordem correta
        colunas_finais = [
            'fonte', 'cd_atendimento', 'cd_ocorrencia', 'cd_ordem', 
            'cd_procedimento', 'nm_procedimento', 'texto_original',
            'dt_procedimento_realizado', 'cd_paciente', 'nm_paciente',
            'obito_fetal_clinico', 'termo_detectado'
        ]
        silver_pd = silver_pd[colunas_finais]
        
        silver_schema = T.StructType([
            T.StructField('fonte', T.StringType(), True),
            T.StructField('cd_atendimento', T.StringType(), True),
            T.StructField('cd_ocorrencia', T.StringType(), True),
            T.StructField('cd_ordem', T.StringType(), True),
            T.StructField('cd_procedimento', T.StringType(), True),
            T.StructField('nm_procedimento', T.StringType(), True),
            T.StructField('texto_original', T.StringType(), True),
            T.StructField('dt_procedimento_realizado', T.TimestampType(), True),
            T.StructField('cd_paciente', T.StringType(), True),
            T.StructField('nm_paciente', T.StringType(), True),
            T.StructField('obito_fetal_clinico', T.IntegerType(), True),
            T.StructField('termo_detectado', T.StringType(), True)
        ])

        silver_df = spark.createDataFrame(silver_pd, schema=silver_schema)
        silver_df = silver_df.withColumn(
            'dt_referencia',
            F.col('dt_procedimento_realizado')
        )

        print(f"💾 Gravando {silver_df.count():,} registros na Silver: {SILVER_TABLE}")
        silver_writer = silver_df.write.format("delta").mode(SILVER_WRITE_MODE)
        if SILVER_WRITE_MODE == "overwrite":
            silver_writer = silver_writer.option("overwriteSchema", "true")
        silver_writer.saveAsTable(SILVER_TABLE)
        spark.catalog.refreshTable(SILVER_TABLE)
        
        print("✅ Silver gravada com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Estatísticas Finais

# COMMAND ----------

try:
    silver_df = spark.table(SILVER_TABLE)
    total = silver_df.count()
    pacientes_unicos = silver_df.select("cd_paciente").distinct().count()
    
    print("=" * 80)
    print("ESTATÍSTICAS SILVER")
    print("=" * 80)
    print(f"Total de laudos positivos: {total:,}")
    print(f"Pacientes únicos: {pacientes_unicos:,}")
    print(f"Por fonte:")
    silver_df.groupBy("fonte").count().show()
    print("=" * 80)
except AnalysisException:
    print("ℹ️ Tabela Silver ainda não existe")


