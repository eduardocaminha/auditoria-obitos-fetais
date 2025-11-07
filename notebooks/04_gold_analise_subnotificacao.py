# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Análise de Subnotificações
# MAGIC 
# MAGIC Detecta possíveis subnotificações de óbito fetal cruzando laudos positivos e CIDs diagnósticos.
# MAGIC Identifica vínculos mãe-feto e valida contra auditoria oficial.

# COMMAND ----------

# MAGIC %run /Workspace/Libraries/Lake

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import pandas as pd

try:
    import openpyxl
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "openpyxl"])
    import openpyxl

from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

# Período analisado
PERIODO_INICIO = '2025-10-01'
PERIODO_FIM = '2025-11-01'

# Janela temporal (±dias) para buscar atendimentos relacionados
JANELA_DIAS = 7

# Tabelas
SILVER_TABLE = "innovation_dev.silver.auditoria_obitos_fetais_processado"
BRONZE_CID_TABLE = "innovation_dev.bronze.auditoria_obitos_cids"
AUDITORIA_TABLE = "RAWZN.TB_AUDITORIA_OBITO_ITEM"

# Controle de camadas
FORCAR_REPROCESSAMENTO_CID = False  # True para forçar nova extração dos CIDs

# CIDs associados a óbito fetal
CID10_LIST = [
    # Núcleo (altamente específicos)
    'P95',      # Morte fetal de causa não especificada
    'P96.4',    # Morte neonatal precoce de causa não especificada
    'O36.4',    # Cuidado materno por morte intrauterina
    'O31.1',    # Morte de um feto ou mais em gestação múltipla
    'O31.2',    # Feto papiráceo
    'Z37.1',    # Nascimento de feto morto único
    'Z37.3',    # Gêmeos – um vivo e um morto
    'Z37.4',    # Gêmeos – ambos mortos
    'Z37.6',    # Múltiplos – alguns vivos e outros mortos
    'Z37.7',    # Múltiplos – todos mortos
    # Contexto forte (placenta/cordão)
    'O43.1',    # Descolamento prematuro da placenta
    'O69.1',    # Compressão do cordão umbilical
    'O69.2',    # Prolapso do cordão umbilical
    'O69.3',    # Circular de cordão com compressão
    'O69.8',    # Outras complicações do cordão
    'O69.9',    # Complicação não especificada do cordão
]

# Output
OUTPUT_PATH = "/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/outputs"
DATA_PROCESSAMENTO = datetime.now().strftime('%Y%m%d_%H%M%S')
EXCEL_FILENAME = f"subnotificacoes_{DATA_PROCESSAMENTO}.xlsx"
EXCEL_PATH = f"{OUTPUT_PATH}/{EXCEL_FILENAME}"

print("=" * 80)
print("CONFIGURAÇÃO - ANÁLISE DE SUBNOTIFICAÇÕES")
print("=" * 80)
print(f"Período: {PERIODO_INICIO} a {PERIODO_FIM}")
print(f"Janela temporal: ±{JANELA_DIAS} dias")
print(f"Total de CIDs monitorados: {len(CID10_LIST)}")
print(f"Tabela Silver: {SILVER_TABLE}")
print(f"Tabela Auditoria: {AUDITORIA_TABLE}")
print(f"Arquivo de saída: {EXCEL_FILENAME}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conectar ao Datalake

# COMMAND ----------

connect_to_datalake(
    username="USR_PROD_INFORMATICA_SAUDE",
    password=dbutils.secrets.get(scope="INNOVATION_RAW", key="USR_PROD_INFORMATICA_SAUDE"),
    layer="RAWZN",
    level="LOW",
    dbx_secret_scope="INNOVATION_RAW"
)

print("✅ Conexão com datalake estabelecida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ler Laudos Positivos (Silver)

# COMMAND ----------

try:
    df_laudos_silver = spark.table(SILVER_TABLE)
    
    # Aplicar filtro de período
    df_laudos_silver = df_laudos_silver.filter(
        (F.col('dt_procedimento_realizado') >= F.lit(PERIODO_INICIO)) &
        (F.col('dt_procedimento_realizado') < F.lit(PERIODO_FIM))
    )
    
    total_laudos = df_laudos_silver.count()
    print(f"✅ Laudos positivos carregados: {total_laudos:,}")
    
    if total_laudos == 0:
        raise RuntimeError("⚠️ Nenhum laudo positivo encontrado para o período. Execute o processamento Silver primeiro.")
    
except AnalysisException:
    raise RuntimeError(f"⚠️ Tabela Silver {SILVER_TABLE} não encontrada. Execute o processamento Silver primeiro.")

df_laudos_silver.createOrReplaceTempView("vw_laudos_positivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. FONTE 1: Vincular Mães e Fetos dos Laudos

# COMMAND ----------

# Buscar atendimentos e fetos usando pandas + run_sql para evitar problemas de catalog
df_laudos_pd = df_laudos_silver.select('cd_paciente', 'nm_paciente', 'dt_procedimento_realizado', 'fonte', 'cd_atendimento').distinct().toPandas()

vinculos_laudos_list = []

for _, laudo in df_laudos_pd.iterrows():
    cd_paciente_mae = laudo['cd_paciente']
    nm_mae = laudo['nm_paciente']
    dt_ref = laudo['dt_procedimento_realizado']
    fonte_laudo = laudo['fonte']
    cd_atend_laudo = laudo['cd_atendimento']
    
    # Calcular janela
    dt_inicio = (pd.to_datetime(dt_ref) - pd.Timedelta(days=JANELA_DIAS)).strftime('%Y-%m-%d')
    dt_fim = (pd.to_datetime(dt_ref) + pd.Timedelta(days=JANELA_DIAS)).strftime('%Y-%m-%d')
    
    # Buscar atendimentos da mãe na janela
    query_atend_mae = f"""
    SELECT 'HSP' AS FONTE_ATEND, CD_ATENDIMENTO, CD_PACIENTE, DT_ATENDIMENTO
    FROM RAWZN.RAW_HSP_TM_ATENDIMENTO
    WHERE CD_PACIENTE = {cd_paciente_mae}
      AND DT_ATENDIMENTO >= DATE '{dt_inicio}'
      AND DT_ATENDIMENTO <= DATE '{dt_fim}'
    UNION ALL
    SELECT 'PSC' AS FONTE_ATEND, CD_ATENDIMENTO, CD_PACIENTE, DT_ATENDIMENTO
    FROM RAWZN.RAW_PSC_TM_ATENDIMENTO
    WHERE CD_PACIENTE = {cd_paciente_mae}
      AND DT_ATENDIMENTO >= DATE '{dt_inicio}'
      AND DT_ATENDIMENTO <= DATE '{dt_fim}'
    """
    
    atend_mae_pd = run_sql(query_atend_mae)
    
    if len(atend_mae_pd) > 0:
        cd_atend_maes = ", ".join(str(x) for x in atend_mae_pd['CD_ATENDIMENTO'].unique())
        
        # Buscar fetos vinculados
        query_fetos = f"""
        SELECT 'HSP' AS FONTE_FETO, CD_ATENDIMENTO AS CD_ATENDIMENTO_FETO,
               CD_PACIENTE AS CD_PACIENTE_FETO, CD_ATENDIMENTO_MAE, DT_ATENDIMENTO AS DT_ATENDIMENTO_FETO
        FROM RAWZN.RAW_HSP_TM_ATENDIMENTO
        WHERE CD_ATENDIMENTO_MAE IN ({cd_atend_maes})
        UNION ALL
        SELECT 'PSC' AS FONTE_FETO, CD_ATENDIMENTO AS CD_ATENDIMENTO_FETO,
               CD_PACIENTE AS CD_PACIENTE_FETO, CD_ATENDIMENTO_MAE, DT_ATENDIMENTO AS DT_ATENDIMENTO_FETO
        FROM RAWZN.RAW_PSC_TM_ATENDIMENTO
        WHERE CD_ATENDIMENTO_MAE IN ({cd_atend_maes})
        """
        
        fetos_pd = run_sql(query_fetos)
        
        # Criar vínculos
        for _, atend in atend_mae_pd.iterrows():
            fetos_atend = fetos_pd[fetos_pd['CD_ATENDIMENTO_MAE'] == atend['CD_ATENDIMENTO']]
            
            if len(fetos_atend) > 0:
                for _, feto in fetos_atend.iterrows():
                    vinculos_laudos_list.append({
                        'cd_paciente_mae': cd_paciente_mae,
                        'nm_mae': nm_mae,
                        'dt_referencia': dt_ref,
                        'fonte_laudo': fonte_laudo,
                        'cd_atendimento_laudo': cd_atend_laudo,
                        'cd_atendimento_mae': atend['CD_ATENDIMENTO'],
                        'dt_atendimento_mae': atend['DT_ATENDIMENTO'],
                        'fonte_feto': feto['FONTE_FETO'],
                        'cd_atendimento_feto': feto['CD_ATENDIMENTO_FETO'],
                        'cd_paciente_feto': feto['CD_PACIENTE_FETO'],
                        'dt_atendimento_feto': feto['DT_ATENDIMENTO_FETO'],
                        'origem': 'LAUDO'
                    })
            else:
                # Sem feto vinculado
                vinculos_laudos_list.append({
                    'cd_paciente_mae': cd_paciente_mae,
                    'nm_mae': nm_mae,
                    'dt_referencia': dt_ref,
                    'fonte_laudo': fonte_laudo,
                    'cd_atendimento_laudo': cd_atend_laudo,
                    'cd_atendimento_mae': atend['CD_ATENDIMENTO'],
                    'dt_atendimento_mae': atend['DT_ATENDIMENTO'],
                    'fonte_feto': None,
                    'cd_atendimento_feto': None,
                    'cd_paciente_feto': None,
                    'dt_atendimento_feto': None,
                    'origem': 'LAUDO'
                })

if len(vinculos_laudos_list) > 0:
    df_laudos_vinculos = spark.createDataFrame(pd.DataFrame(vinculos_laudos_list))
else:
    # Schema vazio
    schema = T.StructType([
        T.StructField('cd_paciente_mae', T.LongType(), True),
        T.StructField('nm_mae', T.StringType(), True),
        T.StructField('dt_referencia', T.TimestampType(), True),
        T.StructField('fonte_laudo', T.StringType(), True),
        T.StructField('cd_atendimento_laudo', T.StringType(), True),
        T.StructField('cd_atendimento_mae', T.LongType(), True),
        T.StructField('dt_atendimento_mae', T.TimestampType(), True),
        T.StructField('fonte_feto', T.StringType(), True),
        T.StructField('cd_atendimento_feto', T.LongType(), True),
        T.StructField('cd_paciente_feto', T.LongType(), True),
        T.StructField('dt_atendimento_feto', T.TimestampType(), True),
        T.StructField('origem', T.StringType(), True)
    ])
    df_laudos_vinculos = spark.createDataFrame([], schema)

total_vinculos_laudos = df_laudos_vinculos.count()
com_feto = df_laudos_vinculos.filter(F.col('cd_paciente_feto').isNotNull()).count()
sem_feto = total_vinculos_laudos - com_feto

print(f"📊 Vínculos identificados nos LAUDOS:")
print(f"   Total de registros: {total_vinculos_laudos:,}")
print(f"   Com feto vinculado: {com_feto:,}")
print(f"   Sem feto vinculado: {sem_feto:,}")

df_laudos_vinculos.createOrReplaceTempView("vw_laudos_vinculos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. FONTE 2: Buscar CIDs de Óbito Fetal (com Bronze)

# COMMAND ----------

def carregar_cids_bronze():
    """Tenta carregar CIDs da Bronze. Se não existir ou forçar, extrai do Lake."""
    if FORCAR_REPROCESSAMENTO_CID:
        print("⚠️ Forçando reprocessamento dos CIDs")
        return None
    
    try:
        cids_bronze = spark.table(BRONZE_CID_TABLE)
        total_bronze = cids_bronze.count()
        print(f"✅ CIDs carregados da Bronze: {total_bronze:,}")
        return cids_bronze
    except AnalysisException:
        print(f"ℹ️ Tabela Bronze CID {BRONZE_CID_TABLE} não encontrada. Extraindo do Lake...")
        return None

df_cids = carregar_cids_bronze()

if df_cids is None:
    # Extrair do Lake usando run_sql
    cid_sql_list = ", ".join(f"'{cid}'" for cid in CID10_LIST)
    
    query_cids = f"""
    SELECT
        'HSP' AS FONTE,
        CD_ATENDIMENTO,
        CD_PACIENTE,
        CD_CID10,
        DT_DIAGNOSTICO AS DT_REFERENCIA,
        NVL(FL_VALIDADO, 'S') AS FL_VALIDADO
    FROM RAWZN.RAW_HSP_TB_DIAGNOSTICO_ATENDIMENTO
    WHERE CD_CID10 IN ({cid_sql_list})
      AND DT_DIAGNOSTICO >= DATE '{PERIODO_INICIO}'
      AND DT_DIAGNOSTICO < DATE '{PERIODO_FIM}'
    
    UNION ALL
    
    SELECT
        'PSC' AS FONTE,
        CD_ATENDIMENTO,
        CD_PACIENTE,
        CD_CID10,
        DT_DIAGNOSTICO AS DT_REFERENCIA,
        NVL(FL_VALIDADO, 'S') AS FL_VALIDADO
    FROM RAWZN.RAW_PSC_TB_DIAGNOSTICO_ATENDIMENTO
    WHERE CD_CID10 IN ({cid_sql_list})
      AND DT_DIAGNOSTICO >= DATE '{PERIODO_INICIO}'
      AND DT_DIAGNOSTICO < DATE '{PERIODO_FIM}'
    """
    
    cids_pd = run_sql(query_cids)
    
    # Filtrar validados
    cids_pd = cids_pd[cids_pd['FL_VALIDADO'] == 'S'].copy()
    cids_pd = cids_pd.drop(columns=['FL_VALIDADO'])
    
    if len(cids_pd) > 0:
        # Converter tipos
        cids_pd['DT_REFERENCIA'] = pd.to_datetime(cids_pd['DT_REFERENCIA'])
        
        df_cids = spark.createDataFrame(cids_pd)
        df_cids = df_cids.withColumn("DT_INGESTAO", F.current_timestamp())
        
        # Salvar em Bronze
        print(f"💾 Gravando {len(cids_pd):,} CIDs na Bronze: {BRONZE_CID_TABLE}")
        df_cids.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_CID_TABLE)
        spark.catalog.refreshTable(BRONZE_CID_TABLE)
    else:
        print("⚠️ Nenhum CID encontrado para o período")
        df_cids = spark.createDataFrame([], T.StructType([
            T.StructField('FONTE', T.StringType(), True),
            T.StructField('CD_ATENDIMENTO', T.LongType(), True),
            T.StructField('CD_PACIENTE', T.LongType(), True),
            T.StructField('CD_CID10', T.StringType(), True),
            T.StructField('DT_REFERENCIA', T.TimestampType(), True)
        ]))

# Aplicar filtro de período (caso tenha carregado Bronze com período maior)
df_cids = df_cids.filter(
    (F.col('DT_REFERENCIA') >= F.lit(PERIODO_INICIO)) &
    (F.col('DT_REFERENCIA') < F.lit(PERIODO_FIM))
)

total_cids = df_cids.count()
print(f"✅ Diagnósticos com CIDs de óbito fetal: {total_cids:,}")

if total_cids > 0:
    print(f"\n📋 CIDs mais frequentes:")
    df_cids.groupBy('CD_CID10').count().orderBy(F.desc('count')).show(10, truncate=False)

df_cids.createOrReplaceTempView("vw_cids_obito")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. FONTE 2: Vincular Mães e Fetos dos CIDs

# COMMAND ----------

# Buscar atendimentos e vínculos usando run_sql
df_cids_pd = df_cids.select('CD_PACIENTE', 'CD_ATENDIMENTO', 'FONTE', 'DT_REFERENCIA', 'CD_CID10').distinct().toPandas()

vinculos_cids_list = []

for _, cid_row in df_cids_pd.iterrows():
    cd_paciente = cid_row['CD_PACIENTE']
    cd_atendimento_cid = cid_row['CD_ATENDIMENTO']
    fonte_cid = cid_row['FONTE']
    dt_ref = cid_row['DT_REFERENCIA']
    cd_cid10 = cid_row['CD_CID10']
    
    # Calcular janela
    dt_inicio = (pd.to_datetime(dt_ref) - pd.Timedelta(days=JANELA_DIAS)).strftime('%Y-%m-%d')
    dt_fim = (pd.to_datetime(dt_ref) + pd.Timedelta(days=JANELA_DIAS)).strftime('%Y-%m-%d')
    
    # Buscar atendimentos relacionados na janela
    query_atend = f"""
    SELECT 'HSP' AS FONTE_ATEND, CD_ATENDIMENTO, CD_PACIENTE, CD_ATENDIMENTO_MAE, DT_ATENDIMENTO
    FROM RAWZN.RAW_HSP_TM_ATENDIMENTO
    WHERE CD_PACIENTE = {cd_paciente}
      AND DT_ATENDIMENTO >= DATE '{dt_inicio}'
      AND DT_ATENDIMENTO <= DATE '{dt_fim}'
    UNION ALL
    SELECT 'PSC' AS FONTE_ATEND, CD_ATENDIMENTO, CD_PACIENTE, CD_ATENDIMENTO_MAE, DT_ATENDIMENTO
    FROM RAWZN.RAW_PSC_TM_ATENDIMENTO
    WHERE CD_PACIENTE = {cd_paciente}
      AND DT_ATENDIMENTO >= DATE '{dt_inicio}'
      AND DT_ATENDIMENTO <= DATE '{dt_fim}'
    """
    
    atend_pd = run_sql(query_atend)
    
    if len(atend_pd) > 0:
        for _, atend in atend_pd.iterrows():
            # Identificar papel: é feto (tem CD_ATENDIMENTO_MAE) ou mãe?
            cd_atendimento_mae_col = atend['CD_ATENDIMENTO_MAE']
            
            if pd.notna(cd_atendimento_mae_col):
                # É FETO
                vinculos_cids_list.append({
                    'cd_paciente_mae': None,
                    'cd_atendimento_mae': int(cd_atendimento_mae_col),
                    'cd_paciente_feto': cd_paciente,
                    'cd_atendimento_feto': atend['CD_ATENDIMENTO'],
                    'papel': 'FETO',
                    'cd_cid10': cd_cid10,
                    'origem': 'CID'
                })
            else:
                # É MÃE
                vinculos_cids_list.append({
                    'cd_paciente_mae': cd_paciente,
                    'cd_atendimento_mae': atend['CD_ATENDIMENTO'],
                    'cd_paciente_feto': None,
                    'cd_atendimento_feto': None,
                    'papel': 'MAE',
                    'cd_cid10': cd_cid10,
                    'origem': 'CID'
                })

if len(vinculos_cids_list) > 0:
    df_cids_vinculos = spark.createDataFrame(pd.DataFrame(vinculos_cids_list))
else:
    schema = T.StructType([
        T.StructField('cd_paciente_mae', T.LongType(), True),
        T.StructField('cd_atendimento_mae', T.LongType(), True),
        T.StructField('cd_paciente_feto', T.LongType(), True),
        T.StructField('cd_atendimento_feto', T.LongType(), True),
        T.StructField('papel', T.StringType(), True),
        T.StructField('cd_cid10', T.StringType(), True),
        T.StructField('origem', T.StringType(), True)
    ])
    df_cids_vinculos = spark.createDataFrame([], schema)

total_vinculos_cids = df_cids_vinculos.count()
print(f"📊 Vínculos identificados nos CIDs:")
print(f"   Total de registros: {total_vinculos_cids:,}")

if total_vinculos_cids > 0:
    print(f"\n📋 Distribuição por papel:")
    df_cids_vinculos.groupBy('papel').count().show()

df_cids_vinculos.createOrReplaceTempView("vw_cids_vinculos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. União e Deduplicação dos Pares (Mãe, Feto)

# COMMAND ----------

# Padronizar estrutura dos laudos
df_laudos_padrao = df_laudos_vinculos.select(
    F.col('cd_paciente_mae'),
    F.col('nm_mae'),
    F.col('cd_atendimento_mae'),
    F.col('cd_paciente_feto'),
    F.col('cd_atendimento_feto'),
    F.lit('LAUDO').alias('origem')
)

# Padronizar estrutura dos CIDs
df_cids_padrao = df_cids_vinculos.select(
    F.col('cd_paciente_mae'),
    F.lit(None).cast(T.StringType()).alias('nm_mae'),  # buscar depois
    F.col('cd_atendimento_mae'),
    F.col('cd_paciente_feto'),
    F.col('cd_atendimento_feto'),
    F.lit('CID').alias('origem')
)

# União
df_uniao = df_laudos_padrao.union(df_cids_padrao)

# Agregar para identificar origem combinada
df_consolidado = df_uniao.groupBy(
    'cd_paciente_mae', 'cd_atendimento_mae', 'cd_paciente_feto', 'cd_atendimento_feto'
).agg(
    F.max('nm_mae').alias('nm_mae'),
    F.collect_set('origem').alias('origens')
)

# Criar flag de origem
df_consolidado = df_consolidado.withColumn(
    'origem_deteccao',
    F.when(F.array_contains('origens', 'LAUDO') & F.array_contains('origens', 'CID'), F.lit('AMBOS'))
     .when(F.array_contains('origens', 'LAUDO'), F.lit('LAUDO'))
     .otherwise(F.lit('CID'))
).drop('origens')

total_pares = df_consolidado.count()
com_feto_total = df_consolidado.filter(F.col('cd_paciente_feto').isNotNull()).count()
sem_feto_total = total_pares - com_feto_total

print("=" * 80)
print("CONSOLIDAÇÃO DOS PARES (MÃE, FETO)")
print("=" * 80)
print(f"Total de pares únicos: {total_pares:,}")
print(f"Com feto identificado: {com_feto_total:,}")
print(f"Sem feto identificado: {sem_feto_total:,}")
print("\n📊 Por origem de detecção:")
df_consolidado.groupBy('origem_deteccao').count().orderBy('origem_deteccao').show()
print("=" * 80)

df_consolidado.createOrReplaceTempView("vw_pares_consolidados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Buscar Nomes das Mães (quando faltante)

# COMMAND ----------

# Buscar nomes das mães que estão faltando
df_consolidado_pd = df_consolidado.toPandas()

# Identificar mães sem nome
maes_sem_nome = df_consolidado_pd[df_consolidado_pd['nm_mae'].isna()]['cd_paciente_mae'].unique()

if len(maes_sem_nome) > 0:
    maes_csv = ", ".join(str(int(x)) for x in maes_sem_nome if pd.notna(x))
    
    if maes_csv:
        query_nomes = f"""
        SELECT CD_PACIENTE, NM_PACIENTE
        FROM RAWZN.RAW_HSP_TB_PACIENTE
        WHERE CD_PACIENTE IN ({maes_csv})
        UNION ALL
        SELECT CD_PACIENTE, NM_PACIENTE
        FROM RAWZN.RAW_PSC_TB_PACIENTE
        WHERE CD_PACIENTE IN ({maes_csv})
        """
        
        nomes_pd = run_sql(query_nomes)
        
        # Mapear nomes
        nomes_map = dict(zip(nomes_pd['CD_PACIENTE'], nomes_pd['NM_PACIENTE']))
        df_consolidado_pd['nm_mae'] = df_consolidado_pd.apply(
            lambda row: nomes_map.get(row['cd_paciente_mae'], row['nm_mae']) if pd.isna(row['nm_mae']) else row['nm_mae'],
            axis=1
        )

df_consolidado_nomes = spark.createDataFrame(df_consolidado_pd)

print("✅ Nomes das mães complementados")

df_consolidado_nomes.createOrReplaceTempView("vw_pares_com_nomes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Checar na Auditoria Oficial

# COMMAND ----------

# Buscar lista de pacientes na auditoria
query_auditoria_maes = """
SELECT DISTINCT CD_PACIENTE
FROM RAWZN.TB_AUDITORIA_OBITO_ITEM
"""

auditoria_pd = run_sql(query_auditoria_maes)
pacientes_auditados = set(auditoria_pd['CD_PACIENTE'].values)

# Marcar quem está na auditoria
df_consolidado_nomes_pd = df_consolidado_nomes.toPandas()

df_consolidado_nomes_pd['mae_na_auditoria'] = df_consolidado_nomes_pd['cd_paciente_mae'].apply(
    lambda x: 'SIM' if pd.notna(x) and int(x) in pacientes_auditados else 'NAO'
)

df_consolidado_nomes_pd['feto_na_auditoria'] = df_consolidado_nomes_pd['cd_paciente_feto'].apply(
    lambda x: 'SIM' if pd.notna(x) and int(x) in pacientes_auditados else 'NAO'
)

df_consolidado_nomes_pd['na_auditoria'] = df_consolidado_nomes_pd.apply(
    lambda row: 'SIM' if row['mae_na_auditoria'] == 'SIM' or row['feto_na_auditoria'] == 'SIM' else 'NAO',
    axis=1
)

df_com_auditoria = spark.createDataFrame(df_consolidado_nomes_pd)

total_na_auditoria = df_com_auditoria.filter(F.col('na_auditoria') == 'SIM').count()
total_nao_auditados = df_com_auditoria.filter(F.col('na_auditoria') == 'NAO').count()

print("=" * 80)
print("CHECAGEM NA AUDITORIA OFICIAL")
print("=" * 80)
print(f"Total de casos: {df_com_auditoria.count():,}")
print(f"Presentes na auditoria: {total_na_auditoria:,}")
print(f"NÃO presentes (subnotificações): {total_nao_auditados:,}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Filtrar Apenas Subnotificações

# COMMAND ----------

df_subnotificacoes = df_com_auditoria.filter(F.col('na_auditoria') == 'NAO')

print(f"🚨 SUBNOTIFICAÇÕES DETECTADAS: {df_subnotificacoes.count():,}")

if df_subnotificacoes.count() > 0:
    print("\n📊 Por origem de detecção:")
    df_subnotificacoes.groupBy('origem_deteccao').count().orderBy('origem_deteccao').show()
    
    print("\n📊 Com/sem feto identificado:")
    df_subnotificacoes.withColumn(
        'feto_identificado',
        F.when(F.col('cd_paciente_feto').isNotNull(), 'COM_FETO').otherwise('SEM_FETO')
    ).groupBy('feto_identificado').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Exportar para Excel

# COMMAND ----------

# Converter para pandas
subnotificacoes_pd = df_subnotificacoes.toPandas()
todos_casos_pd = df_com_auditoria.toPandas()

# Estatísticas
stats_origem = todos_casos_pd.groupby(['origem_deteccao', 'na_auditoria']).size().reset_index(name='total')
stats_feto = todos_casos_pd.groupby([
    'na_auditoria',
    todos_casos_pd['cd_paciente_feto'].notna().map({True: 'COM_FETO', False: 'SEM_FETO'})
]).size().reset_index(name='total')

stats_overview = pd.DataFrame({
    'metrica': [
        'Total de casos detectados',
        'Casos na auditoria',
        'SUBNOTIFICAÇÕES DETECTADAS',
        'Subnotificações com feto identificado',
        'Subnotificações sem feto identificado',
        'Detectados por LAUDO apenas',
        'Detectados por CID apenas',
        'Detectados por AMBOS',
        'Período analisado',
        'Janela temporal',
        'Data de processamento'
    ],
    'valor': [
        len(todos_casos_pd),
        len(todos_casos_pd[todos_casos_pd['na_auditoria'] == 'SIM']),
        len(subnotificacoes_pd),
        len(subnotificacoes_pd[subnotificacoes_pd['cd_paciente_feto'].notna()]),
        len(subnotificacoes_pd[subnotificacoes_pd['cd_paciente_feto'].isna()]),
        len(todos_casos_pd[todos_casos_pd['origem_deteccao'] == 'LAUDO']),
        len(todos_casos_pd[todos_casos_pd['origem_deteccao'] == 'CID']),
        len(todos_casos_pd[todos_casos_pd['origem_deteccao'] == 'AMBOS']),
        f"{PERIODO_INICIO} a {PERIODO_FIM}",
        f"±{JANELA_DIAS} dias",
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ]
})

# Criar Excel
wb = Workbook()
wb.remove(wb.active)

def escrever_aba(nome, dataframe):
    """Escreve um DataFrame em uma aba do Excel com formatação"""
    ws = wb.create_sheet(nome)
    for row in dataframe_to_rows(dataframe, index=False, header=True):
        ws.append(row)
    
    # Formatar cabeçalho
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")
    
    # Ajustar largura das colunas
    for column_cells in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
        adjusted_width = max_length + 2
        ws.column_dimensions[get_column_letter(column_cells[0].column)].width = min(adjusted_width, 60)

# Criar abas
escrever_aba("1_Resumo", stats_overview)
escrever_aba("2_Subnotificacoes", subnotificacoes_pd)
escrever_aba("3_Todos_Casos", todos_casos_pd)
escrever_aba("4_Stats_Origem", stats_origem)
escrever_aba("5_Stats_Feto", stats_feto)

# Salvar
import os
os.makedirs(OUTPUT_PATH, exist_ok=True)
wb.save(EXCEL_PATH)

print("=" * 80)
print("✅ ARQUIVO EXCEL GERADO COM SUCESSO!")
print("=" * 80)
print(f"📁 Local: {EXCEL_PATH}")
print(f"📊 Abas criadas:")
print(f"   1. Resumo Geral")
print(f"   2. Subnotificações ({len(subnotificacoes_pd):,} casos)")
print(f"   3. Todos os Casos ({len(todos_casos_pd):,} registros)")
print(f"   4. Estatísticas por Origem")
print(f"   5. Estatísticas Feto Identificado")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Resumo Final

# COMMAND ----------

print("=" * 80)
print("RESUMO FINAL - ANÁLISE DE SUBNOTIFICAÇÕES")
print("=" * 80)
print(f"🔍 Período analisado: {PERIODO_INICIO} a {PERIODO_FIM}")
print(f"📋 Laudos positivos: {total_laudos:,}")
print(f"🏥 Diagnósticos CID: {total_cids:,}")
print(f"👥 Pares (mãe, feto) únicos: {total_pares:,}")
print(f"✅ Casos na auditoria: {total_na_auditoria:,}")
print(f"🚨 SUBNOTIFICAÇÕES DETECTADAS: {total_nao_auditados:,}")
print(f"📁 Arquivo: {EXCEL_FILENAME}")
print("=" * 80)

