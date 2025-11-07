# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Consolidação e Exportação
# MAGIC 
# MAGIC Gera planilha Excel com resultados consolidados a partir da Silver.
# MAGIC Notebook desenhado para execução manual.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import pandas as pd
from datetime import datetime

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

SILVER_TABLE = "innovation_dev.silver.auditoria_obitos_fetais_processado"
OUTPUT_PATH = "/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/outputs"

FILTRAR_POR_PERIODO = True
PERIODO_INICIO = '2025-10-01'
PERIODO_FIM = '2025-11-01'

DATA_PROCESSAMENTO = datetime.now().strftime('%Y%m%d_%H%M%S')
EXCEL_PATH = f"{OUTPUT_PATH}/obitos_fetais_{DATA_PROCESSAMENTO}.xlsx"

print("=" * 80)
print("CONFIGURAÇÃO GOLD")
print("=" * 80)
print(f"Tabela Silver: {SILVER_TABLE}")
print(f"Arquivo de saída: {EXCEL_PATH}")
if FILTRAR_POR_PERIODO:
    print(f"Período filtrado: {PERIODO_INICIO} a {PERIODO_FIM}")
else:
    print("Período filtrado: não (usar todos os dados da Silver)")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ler Silver

# COMMAND ----------

try:
    silver_df = spark.table(SILVER_TABLE)
except AnalysisException:
    raise RuntimeError(f"Tabela Silver {SILVER_TABLE} não encontrada. Execute o processamento da Silver antes do Gold.")

if FILTRAR_POR_PERIODO:
    silver_df = silver_df.filter(
        (F.col('dt_referencia') >= F.to_timestamp(F.lit(PERIODO_INICIO))) &
        (F.col('dt_referencia') < F.to_timestamp(F.lit(PERIODO_FIM)))
    )

total_registros = silver_df.count()
print(f"✅ Registros carregados da Silver: {total_registros:,}")

silver_pd = silver_df.toPandas()

if silver_pd.empty:
    raise RuntimeError("Nenhum registro encontrado para exportação.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar DataFrames auxiliares

# COMMAND ----------

silver_pd['dt_procedimento_realizado'] = pd.to_datetime(silver_pd['dt_procedimento_realizado'])
silver_pd['dt_referencia'] = pd.to_datetime(silver_pd['dt_referencia'])

positivos_pd = silver_pd[silver_pd['obito_fetal_clinico'] == 1].copy()
positivos_unicos_pd = positivos_pd.drop_duplicates(subset=['cd_paciente'], keep='first')

print(f"✅ Positivos totais: {len(positivos_pd):,}")
print(f"✅ Positivos únicos por paciente: {len(positivos_unicos_pd):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estatísticas

# COMMAND ----------

total_laudos = len(silver_pd)
total_positivos = len(positivos_pd)
total_positivos_unicos = len(positivos_unicos_pd)
percentual_positivos = (total_positivos / total_laudos * 100) if total_laudos > 0 else 0

stats_fonte = (
    silver_pd
    .groupby(['fonte', 'obito_fetal_clinico'])
    .size()
    .reset_index(name='quantidade')
    .pivot(index='fonte', columns='obito_fetal_clinico', values='quantidade')
    .fillna(0)
    .rename(columns={0: 'Negativo', 1: 'Positivo'})
)
stats_fonte['Total'] = stats_fonte.sum(axis=1)
stats_fonte['% Positivos'] = (stats_fonte['Positivo'] / stats_fonte['Total'] * 100).round(2)
stats_fonte = stats_fonte.reset_index()

if total_positivos > 0:
    stats_termo = (
        positivos_pd['termo_detectado']
        .value_counts(dropna=False)
        .reset_index()
        .rename(columns={'index': 'termo_detectado', 'termo_detectado': 'quantidade'})
    )
    stats_termo['percentual'] = (stats_termo['quantidade'] / total_positivos * 100).round(2)
else:
    stats_termo = pd.DataFrame(columns=['termo_detectado', 'quantidade', 'percentual'])

pacientes_unicos = silver_pd['cd_paciente'].nunique()
pacientes_com_obito = positivos_unicos_pd['cd_paciente'].nunique()

stats_overview = pd.DataFrame({
    'metrica': [
        'Total de laudos processados',
        'Óbitos fetais detectados',
        'Óbitos fetais (pacientes únicos)',
        '% de positivos',
        'Pacientes únicos (total)',
        'Pacientes com óbito fetal',
        'Data de processamento'
    ],
    'valor': [
        total_laudos,
        total_positivos,
        total_positivos_unicos,
        f"{percentual_positivos:.2f}%",
        pacientes_unicos,
        pacientes_com_obito,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ]
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Preparar abas do Excel

# COMMAND ----------

colunas_export = [
    'fonte',
    'cd_atendimento',
    'cd_paciente',
    'nm_paciente',
    'cd_procedimento',
    'nm_procedimento',
    'dt_procedimento_realizado',
    'termo_detectado',
    'texto_original',
    'dt_referencia'
]

positivos_export = positivos_pd[colunas_export].copy()
positivos_export = positivos_export.sort_values('dt_procedimento_realizado', ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exportar para Excel

# COMMAND ----------

wb = Workbook()
wb.remove(wb.active)

def escrever_aba(nome, dataframe):
    ws = wb.create_sheet(nome)
    for row in dataframe_to_rows(dataframe, index=False, header=True):
        ws.append(row)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")
    for column_cells in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
        adjusted_width = max_length + 2
        ws.column_dimensions[get_column_letter(column_cells[0].column)].width = min(adjusted_width, 60)

escrever_aba("Positivos", positivos_export)
escrever_aba("Positivos Únicos", positivos_unicos_pd[colunas_export])
escrever_aba("Estatísticas", stats_overview)
escrever_aba("Stats por Fonte", stats_fonte)
escrever_aba("Termos Detectados", stats_termo)

wb.save(EXCEL_PATH)
print(f"✅ Arquivo Excel gerado com sucesso: {EXCEL_PATH}")


