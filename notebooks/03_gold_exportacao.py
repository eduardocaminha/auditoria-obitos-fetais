# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Exportação e Consolidação
# MAGIC 
# MAGIC Lê dados do Silver, consolida e exporta Excel com múltiplas abas.
# MAGIC 
# MAGIC **Execução**: Diária (job Databricks)
# MAGIC **Output**: Excel com abas: Positivos, Estatísticas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
import sys

# Instalar openpyxl se necessário (para Excel)
try:
    import openpyxl
except ImportError:
    import subprocess
    subprocess.check_call(["pip", "install", "openpyxl"])
    import openpyxl

spark = SparkSession.getActiveSession()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

SILVER_TABLE = "innovation_dev.silver.auditoria_obitos_fetais_processado"
OUTPUT_PATH = "/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/outputs"

# Data de processamento
DATA_PROCESSAMENTO = datetime.now().strftime('%Y%m%d')
EXCEL_PATH = f"{OUTPUT_PATH}/obitos_fetais_{DATA_PROCESSAMENTO}.xlsx"

print("=" * 80)
print("CONFIGURAÇÃO GOLD")
print("=" * 80)
print(f"Tabela Silver: {SILVER_TABLE}")
print(f"Arquivo Excel: {EXCEL_PATH}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ler Dados do Silver

# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)

# Filtrar apenas últimos 30 dias para exportação
df_silver = df_silver.filter(
    col("DT_PROCESSAMENTO") >= current_date() - expr("INTERVAL 30 DAYS")
)

print(f"✅ Registros do Silver: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar Dados para Exportação

# COMMAND ----------

# Converter para Pandas (para exportar Excel)
df_pandas = df_silver.toPandas()

# Filtrar apenas positivos
df_positivos = df_pandas[df_pandas['obito_fetal_clinico'] == 1].copy()

# Remover duplicatas por paciente (manter apenas o primeiro caso de cada paciente)
df_positivos = df_positivos.drop_duplicates(
    subset=['CD_PACIENTE'],
    keep='first'
)

print(f"✅ Positivos únicos: {len(df_positivos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gerar Estatísticas

# COMMAND ----------

# Estatísticas gerais
total_laudos = len(df_pandas)
total_positivos = len(df_pandas[df_pandas['obito_fetal_clinico'] == 1])
total_positivos_unicos = len(df_positivos)
percentual_positivos = (total_positivos / total_laudos * 100) if total_laudos > 0 else 0

# Por fonte
stats_fonte = df_pandas.groupby(['FONTE', 'obito_fetal_clinico']).size().reset_index(name='QTD')
stats_fonte_pivot = stats_fonte.pivot_table(
    index='FONTE',
    columns='obito_fetal_clinico',
    values='QTD',
    fill_value=0
)
stats_fonte_pivot.columns = ['Negativo', 'Positivo']
stats_fonte_pivot['Total'] = stats_fonte_pivot.sum(axis=1)
stats_fonte_pivot['% Positivos'] = (stats_fonte_pivot['Positivo'] / stats_fonte_pivot['Total'] * 100).round(2)

# Distribuição de termos detectados
if total_positivos > 0:
    stats_termos = df_pandas[df_pandas['obito_fetal_clinico'] == 1]['termo_detectado'].value_counts().reset_index()
    stats_termos.columns = ['Termo Detectado', 'Quantidade']
    stats_termos['Percentual'] = (stats_termos['Quantidade'] / total_positivos * 100).round(2)
else:
    stats_termos = pd.DataFrame(columns=['Termo Detectado', 'Quantidade', 'Percentual'])

# Por paciente
pacientes_unicos = df_pandas['CD_PACIENTE'].nunique()
pacientes_com_obito = df_positivos['CD_PACIENTE'].nunique() if len(df_positivos) > 0 else 0

# Criar DataFrame de estatísticas
stats_data = {
    'Métrica': [
        'Total de Laudos Processados',
        'Óbitos Fetais Detectados',
        'Óbitos Fetais (Únicos por Paciente)',
        '% de Positivos',
        'Pacientes Únicos (Total)',
        'Pacientes com Óbito Fetal',
        'Data de Processamento'
    ],
    'Valor': [
        total_laudos,
        total_positivos,
        total_positivos_unicos,
        f"{percentual_positivos:.2f}%",
        pacientes_unicos,
        pacientes_com_obito,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ]
}

df_stats = pd.DataFrame(stats_data)

print("✅ Estatísticas geradas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Preparar Dados Positivos para Exportação

# COMMAND ----------

# Selecionar colunas relevantes para exportação
colunas_export = [
    'FONTE',
    'CD_ATENDIMENTO',
    'CD_PACIENTE',
    'NM_PACIENTE',
    'CD_PROCEDIMENTO',
    'NM_PROCEDIMENTO',
    'DT_PROCEDIMENTO_REALIZADO',
    'DS_LAUDO_MEDICO',
    'termo_detectado',
    'DT_PROCESSAMENTO'
]

df_positivos_export = df_positivos[colunas_export].copy()

# Ordenar por data
df_positivos_export = df_positivos_export.sort_values('DT_PROCEDIMENTO_REALIZADO', ascending=False)

print(f"✅ Dados positivos preparados: {len(df_positivos_export)} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exportar para Excel

# COMMAND ----------

from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# Criar workbook
wb = Workbook()
wb.remove(wb.active)  # Remover sheet padrão

# Aba 1: Positivos
ws_positivos = wb.create_sheet("Positivos")
for r in dataframe_to_rows(df_positivos_export, index=False, header=True):
    ws_positivos.append(r)

# Formatar cabeçalho
for cell in ws_positivos[1]:
    cell.font = Font(bold=True)
    cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    cell.font = Font(bold=True, color="FFFFFF")
    cell.alignment = Alignment(horizontal="center", vertical="center")

# Ajustar largura das colunas
for column in ws_positivos.columns:
    max_length = 0
    column_letter = get_column_letter(column[0].column)
    for cell in column:
        try:
            if len(str(cell.value)) > max_length:
                max_length = len(str(cell.value))
        except:
            pass
    adjusted_width = min(max_length + 2, 50)
    ws_positivos.column_dimensions[column_letter].width = adjusted_width

# Aba 2: Estatísticas Gerais
ws_stats = wb.create_sheet("Estatísticas")
for r in dataframe_to_rows(df_stats, index=False, header=True):
    ws_stats.append(r)

# Formatar cabeçalho
for cell in ws_stats[1]:
    cell.font = Font(bold=True)
    cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    cell.font = Font(bold=True, color="FFFFFF")
    cell.alignment = Alignment(horizontal="center", vertical="center")

# Ajustar largura
for column in ws_stats.columns:
    column_letter = get_column_letter(column[0].column)
    ws_stats.column_dimensions[column_letter].width = 40

# Aba 3: Estatísticas por Fonte
ws_fonte = wb.create_sheet("Por Fonte")
for r in dataframe_to_rows(stats_fonte_pivot, index=True, header=True):
    ws_fonte.append(r)

# Formatar cabeçalho
for cell in ws_fonte[1]:
    cell.font = Font(bold=True)
    cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    cell.font = Font(bold=True, color="FFFFFF")
    cell.alignment = Alignment(horizontal="center", vertical="center")

# Ajustar largura
for column in ws_fonte.columns:
    column_letter = get_column_letter(column[0].column)
    ws_fonte.column_dimensions[column_letter].width = 15

# Aba 4: Distribuição de Termos
ws_termos = wb.create_sheet("Termos Detectados")
for r in dataframe_to_rows(stats_termos, index=False, header=True):
    ws_termos.append(r)

# Formatar cabeçalho
for cell in ws_termos[1]:
    cell.font = Font(bold=True)
    cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    cell.font = Font(bold=True, color="FFFFFF")
    cell.alignment = Alignment(horizontal="center", vertical="center")

# Ajustar largura
for column in ws_termos.columns:
    column_letter = get_column_letter(column[0].column)
    ws_termos.column_dimensions[column_letter].width = 30

# Salvar
wb.save(EXCEL_PATH)

print(f"✅ Excel exportado: {EXCEL_PATH}")
print(f"   Abas criadas: Positivos, Estatísticas, Por Fonte, Termos Detectados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumo Final

# COMMAND ----------

print("=" * 80)
print("✅ EXPORTAÇÃO CONCLUÍDA")
print("=" * 80)
print(f"Arquivo: {EXCEL_PATH}")
print(f"Total de laudos: {total_laudos}")
print(f"Óbitos fetais detectados: {total_positivos}")
print(f"Óbitos fetais únicos (por paciente): {total_positivos_unicos}")
print(f"Percentual de positivos: {percentual_positivos:.2f}%")
print("=" * 80)

