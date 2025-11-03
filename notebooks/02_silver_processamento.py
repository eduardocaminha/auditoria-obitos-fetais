# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Processamento e Classificação
# MAGIC 
# MAGIC Lê dados do Bronze, classifica óbitos fetais e salva em Silver.
# MAGIC 
# MAGIC **Execução**: Diária (job Databricks)
# MAGIC **Tabela**: `innovation_dev.silver.auditoria_obitos_fetais_processado`

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import re
import unicodedata
from datetime import datetime

# Adicionar diretório do projeto ao path
sys.path.append('/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/notebooks')

spark = SparkSession.getActiveSession()

# Padrões de óbito fetal (copiados de processar_obitos_fetais.py)
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

print("=" * 80)
print("CONFIGURAÇÃO SILVER")
print("=" * 80)
print(f"Tabela Bronze: {BRONZE_TABLE}")
print(f"Tabela Silver: {SILVER_TABLE}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ler Dados do Bronze

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)

# Filtrar apenas novos registros (últimos 7 dias para processar tudo)
df_bronze = df_bronze.filter(
    col("DT_INGESTAO") >= current_date() - expr("INTERVAL 7 DAYS")
)

print(f"✅ Registros do Bronze: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções de Classificação (UDF)

# COMMAND ----------

def normalize_text_udf(text):
    """Normaliza texto para comparação"""
    if not text:
        return ""
    text = str(text).lower()
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'\s+', ' ', text)
    return text

def extract_semanas_udf(text):
    """Extrai semanas gestacionais"""
    if not text:
        return []
    pattern = r"(\d{1,2})\s*(?:semanas?|s(?:\s*\d+\s*d)?)"
    matches = re.findall(pattern, text)
    semanas = list(set([int(m) for m in matches if m.isdigit()]))
    return semanas

def has_ig_above_22_udf(text):
    """Verifica se IG >= 22 semanas"""
    semanas = extract_semanas_udf(text)
    if len(semanas) == 0:
        return False
    return any(semana >= 22 for semana in semanas)

def detectar_padrao_obito_udf(texto_norm):
    """Detecta qual padrão de óbito foi encontrado"""
    if not texto_norm:
        return None
    
    for pattern_tuple in patterns_obito:
        pattern = pattern_tuple[0] if isinstance(pattern_tuple, tuple) else pattern_tuple
        if re.search(pattern, texto_norm):
            descricao = pattern_tuple[1] if isinstance(pattern_tuple, tuple) else pattern
            return descricao
    return None

def extrair_trecho_capturado_udf(texto_norm, texto_original, padrao_detectado):
    """Extrai trecho do texto original com contexto"""
    if not texto_original or not padrao_detectado:
        return None
    
    texto_original_str = str(texto_original)
    texto_original_norm = normalize_text_udf(texto_original_str)
    
    # Buscar padrão no texto original normalizado
    for pattern_tuple in patterns_obito:
        pattern = pattern_tuple[0] if isinstance(pattern_tuple, tuple) else pattern_tuple
        match = re.search(pattern, texto_original_norm)
        if match:
            pos_inicio = match.start()
            pos_fim = match.end()
            
            # Contexto de 50 caracteres
            contexto = 50
            inicio_contexto = max(0, pos_inicio - contexto)
            fim_contexto = min(len(texto_original_str), pos_fim + contexto)
            
            return texto_original_str[inicio_contexto:fim_contexto].strip()
    
    return None

def classificar_obito_fetal_udf(texto_norm, texto_original):
    """Classifica óbito fetal e retorna (classificacao, termo_detectado)"""
    if not texto_norm:
        return (0, None)
    
    # Passo 1: Detectar padrão
    padrao_detectado = detectar_padrao_obito_udf(texto_norm)
    if padrao_detectado is None:
        return (0, None)
    
    # Passo 2: Verificar IG >= 22 semanas
    if not has_ig_above_22_udf(texto_norm):
        return (0, None)
    
    # Passo 3: Extrair trecho
    trecho = extrair_trecho_capturado_udf(texto_norm, texto_original, padrao_detectado)
    
    return (1, trecho)

# Registrar UDFs
normalize_text_udf_spark = udf(normalize_text_udf, StringType())
extract_semanas_udf_spark = udf(extract_semanas_udf, ArrayType(IntegerType()))
has_ig_above_22_udf_spark = udf(has_ig_above_22_udf, BooleanType())
classificar_obito_fetal_udf_spark = udf(
    lambda texto_original: classificar_obito_fetal_udf(
        normalize_text_udf(texto_original), 
        texto_original
    ),
    StructType([
        StructField("obito_fetal_clinico", IntegerType()),
        StructField("termo_detectado", StringType())
    ])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Processar e Classificar

# COMMAND ----------

# Normalizar texto
df_silver = df_bronze.withColumn(
    "texto_normalizado",
    normalize_text_udf_spark(col("DS_LAUDO_MEDICO"))
)

# Classificar
df_silver = df_silver.withColumn(
    "classificacao",
    classificar_obito_fetal_udf_spark(col("DS_LAUDO_MEDICO"))
)

# Extrair campos da classificação
df_silver = df_silver.withColumn(
    "obito_fetal_clinico",
    col("classificacao.obito_fetal_clinico")
).withColumn(
    "termo_detectado",
    col("classificacao.termo_detectado")
)

# Remover coluna temporária
df_silver = df_silver.drop("classificacao")

# Adicionar timestamp de processamento
df_silver = df_silver.withColumn(
    "DT_PROCESSAMENTO",
    current_timestamp()
)

print(f"✅ Processamento concluído: {df_silver.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvar em Delta Lake (Silver)

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"✅ Dados salvos em {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Estatísticas

# COMMAND ----------

total = df_silver.count()
positivos = df_silver.filter(col("obito_fetal_clinico") == 1).count()
percentual = (positivos / total * 100) if total > 0 else 0

print("=" * 80)
print("ESTATÍSTICAS SILVER")
print("=" * 80)
print(f"Total processado: {total}")
print(f"Óbitos fetais detectados: {positivos} ({percentual:.2f}%)")
print(f"Por fonte:")
df_silver.groupBy("FONTE", "obito_fetal_clinico").count().show()
print("=" * 80)

