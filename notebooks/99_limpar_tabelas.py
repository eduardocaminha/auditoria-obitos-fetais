# Databricks notebook source
# MAGIC %md
# MAGIC # Limpeza de Tabelas Delta Lake
# MAGIC 
# MAGIC **⚠️ ATENÇÃO: Este notebook deleta todas as tabelas do pipeline!**
# MAGIC 
# MAGIC Use este notebook para:
# MAGIC - Limpar dados e começar do zero
# MAGIC - Resetar o pipeline completo
# MAGIC - Manutenção e testes
# MAGIC 
# MAGIC **Tabelas que serão deletadas:**
# MAGIC - `innovation_dev.bronze.auditoria_obitos_fetais_raw` (Bronze)
# MAGIC - `innovation_dev.silver.auditoria_obitos_fetais_processado` (Silver)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

BRONZE_TABLE = "innovation_dev.bronze.auditoria_obitos_fetais_raw"
SILVER_TABLE = "innovation_dev.silver.auditoria_obitos_fetais_processado"

print("=" * 80)
print("LIMPEZA DE TABELAS DELTA LAKE")
print("=" * 80)
print(f"Tabela Bronze: {BRONZE_TABLE}")
print(f"Tabela Silver: {SILVER_TABLE}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar Tabelas Existentes

# COMMAND ----------

def tabela_existe(nome_tabela):
    """Verifica se uma tabela existe"""
    try:
        spark.table(nome_tabela).limit(1).collect()
        return True
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            return False
        raise

print("🔍 Verificando tabelas existentes...")
print()

bronze_existe = tabela_existe(BRONZE_TABLE)
silver_existe = tabela_existe(SILVER_TABLE)

print(f"Bronze ({BRONZE_TABLE}): {'✅ Existe' if bronze_existe else '❌ Não existe'}")
if bronze_existe:
    count_bronze = spark.table(BRONZE_TABLE).count()
    print(f"   Registros: {count_bronze:,}")
    
print(f"Silver ({SILVER_TABLE}): {'✅ Existe' if silver_existe else '❌ Não existe'}")
if silver_existe:
    count_silver = spark.table(SILVER_TABLE).count()
    print(f"   Registros: {count_silver:,}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deletar Tabelas (IF EXISTS)

# COMMAND ----------

# Deletar tabela Silver primeiro (depende de Bronze)
if silver_existe:
    print(f"🗑️  Deletando tabela Silver: {SILVER_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
    print(f"✅ Tabela Silver deletada com sucesso!")
else:
    print(f"⏭️  Tabela Silver não existe, pulando...")

print()

# Deletar tabela Bronze
if bronze_existe:
    print(f"🗑️  Deletando tabela Bronze: {BRONZE_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
    print(f"✅ Tabela Bronze deletada com sucesso!")
else:
    print(f"⏭️  Tabela Bronze não existe, pulando...")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar Limpeza

# COMMAND ----------

print("🔍 Verificando limpeza...")
print()

bronze_existe_depois = tabela_existe(BRONZE_TABLE)
silver_existe_depois = tabela_existe(SILVER_TABLE)

if not bronze_existe_depois and not silver_existe_depois:
    print("✅ Todas as tabelas foram deletadas com sucesso!")
    print()
    print("📋 PRÓXIMOS PASSOS:")
    print("   1. Execute o notebook 01_bronze_ingestao.py para recriar a tabela Bronze")
    print("   2. Execute o notebook 02_silver_processamento.py para recriar a tabela Silver")
    print("   3. Execute o notebook 03_gold_exportacao.py para gerar o Excel")
else:
    print("⚠️  Algumas tabelas ainda existem:")
    if bronze_existe_depois:
        print(f"   - Bronze: {BRONZE_TABLE}")
    if silver_existe_depois:
        print(f"   - Silver: {SILVER_TABLE}")

print("=" * 80)

