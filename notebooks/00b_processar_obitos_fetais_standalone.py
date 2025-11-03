# Databricks notebook source
# MAGIC %md
# MAGIC # Auditoria de Óbitos Fetais
# MAGIC 
# MAGIC Sistema de classificação de laudos de ultrassom obstétrico para detecção de óbitos fetais.
# MAGIC 
# MAGIC **Objetivo:** Identificar laudos com suspeita de óbito fetal usando regras clínicas baseadas em padrões textuais.
# MAGIC 
# MAGIC **Fonte dos dados:** CSV de laudos de radiologia extraídos do datalake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

# CONFIGURAÇÕES - AJUSTAR ANTES DE EXECUTAR
INPUT_CSV_PATH = "/Workspace/Innovation/t_eduardo.caminha/radiologia-extracao-laudos/outputs/laudos_2025-11_2025-11.csv.gz"
OUTPUT_PATH = "/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/outputs"

# Nomes das colunas esperadas no CSV
COLUNA_ATENDIMENTO = "CD_ATENDIMENTO"
COLUNA_LAUDO = "DS_LAUDO_MEDICO"

print("=" * 80)
print("CONFIGURAÇÃO")
print("=" * 80)
print(f"Input CSV: {INPUT_CSV_PATH}")
print(f"Output Path: {OUTPUT_PATH}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregar e Preparar Dados

# COMMAND ----------

import pandas as pd
import unicodedata
import re
from datetime import datetime

# Carregar CSV
print("Carregando CSV...")
df = pd.read_csv(
    INPUT_CSV_PATH,
    compression='gzip',
    sep=';',
    encoding='utf-8'
)

print(f"✅ CSV carregado: {len(df)} registros")

# Verificar colunas disponíveis
print(f"\nColunas disponíveis: {df.columns.tolist()}")

# Verificar se as colunas esperadas existem
colunas_esperadas = [COLUNA_ATENDIMENTO, COLUNA_LAUDO]
for col in colunas_esperadas:
    if col not in df.columns:
        print(f"⚠️  AVISO: Coluna '{col}' não encontrada!")
    else:
        print(f"✅ Coluna '{col}' encontrada")

# Preview dos dados
display(df.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalização de Texto

# COMMAND ----------

def normalize_text(text):
    """
    Normaliza texto para comparação de padrões:
    - Remove acentos
    - Converte para minúsculas
    - Remove espaços múltiplos
    """
    if pd.isna(text):
        return ""
    
    text = str(text).lower()
    
    # Remove acentos
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    
    # Remove espaços múltiplos
    text = re.sub(r'\s+', ' ', text)
    
    return text

# Aplicar normalização
print("Normalizando textos...")
df["texto_norm"] = df[COLUNA_LAUDO].apply(normalize_text)

# Filtro defensivo: remover laudos vazios
df = df[df["texto_norm"].str.strip().str.len() > 0]

print(f"✅ Laudos normalizados: {len(df)} registros válidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Padrões de Classificação

# COMMAND ----------

# Padrões que indicam óbito fetal
# Termos comuns usados por radiologistas na impressão diagnóstica
# Formato: (padrão_regex, descrição_legível)
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

# Padrões de exclusão removidos - abortos já são filtrados naturalmente
# pela verificação de idade gestacional acima de 22 semanas

print("✅ Padrões de classificação carregados")
print(f"   Padrões de óbito: {len(patterns_obito)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Funções Auxiliares

# COMMAND ----------

def extract_semanas(text):
    """
    Extrai semanas gestacionais de um texto
    Suporta formatos: "33 semanas", "33s", "25s01d", "25s 01d"
    Retorna lista de semanas encontradas
    """
    # Padrão único: captura "33 semanas", "33s", "25s01d", "25s 01d"
    pattern = r"(\d{1,2})\s*(?:semanas?|s(?:\s*\d+\s*d)?)"
    matches = re.findall(pattern, text)
    
    # Converter para inteiros e remover duplicatas
    semanas = list(set([int(m) for m in matches if m.isdigit()]))
    
    return semanas

def has_ig_above_22_semanas(text):
    """
    Verifica se o texto menciona idade gestacional acima de 22 semanas
    Retorna True se encontrar pelo menos uma menção >= 22 semanas
    """
    semanas = extract_semanas(text)
    if len(semanas) == 0:
        return False
    
    # Retorna True se qualquer semana encontrada for >= 22
    return any(semana >= 22 for semana in semanas)

def mentions_semanas(text):
    """
    Verifica se o texto menciona semanas gestacionais
    """
    semanas = extract_semanas(text)
    return len(semanas) > 0

print("✅ Funções auxiliares criadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Classificação de Óbitos Fetais

# COMMAND ----------

def classificar_obito_fetal(texto_norm, texto_original):
    """
    Classifica laudo como óbito fetal (1) ou não (0)
    Retorna tupla: (classificação, trecho_capturado)
    
    Lógica:
    1. Verifica se contém padrão de óbito fetal
    2. Verifica se menciona idade gestacional acima de 22 semanas
       (abortos naturalmente ficam de fora, pois são < 22 semanas)
    3. Extrai trecho exato do texto original com contexto
    """
    # Passo 1: Verificar padrões de óbito fetal e capturar match
    match_encontrado = None
    pattern_match = None
    
    for pattern_tuple in patterns_obito:
        pattern = pattern_tuple[0] if isinstance(pattern_tuple, tuple) else pattern_tuple
        
        match = re.search(pattern, texto_norm)
        if match:
            match_encontrado = match
            pattern_match = pattern
            break
    
    if match_encontrado is None:
        return (0, None)
    
    # Passo 2: Verificar se idade gestacional é >= 22 semanas
    # Abortos (geralmente < 22 semanas) são automaticamente excluídos
    if not has_ig_above_22_semanas(texto_norm):
        return (0, None)
    
    # Passo 3: Extrair trecho exato do texto original com contexto
    # Normalizar texto original temporariamente para encontrar posição exata
    texto_original_str = str(texto_original)
    texto_original_norm = normalize_text(texto_original_str)
    
    # Buscar padrão no texto original normalizado para encontrar posição
    match_original_norm = re.search(pattern_match, texto_original_norm)
    
    if match_original_norm:
        # Posição no texto normalizado do original
        pos_inicio_norm = match_original_norm.start()
        pos_fim_norm = match_original_norm.end()
        
        # Estimar posição no texto original (considerando diferença de tamanho por acentos)
        # Aproximação: assumir que a diferença é proporcional
        len_original_norm = len(texto_original_norm)
        len_original = len(texto_original_str)
        
        if len_original_norm > 0:
            # Calcular posição proporcional
            pos_inicio = int((pos_inicio_norm / len_original_norm) * len_original)
            pos_fim = int((pos_fim_norm / len_original_norm) * len_original)
        else:
            pos_inicio = 0
            pos_fim = len_original
        
        # Ajustar para garantir que não saia dos limites
        pos_inicio = max(0, pos_inicio)
        pos_fim = min(len_original, pos_fim)
    else:
        # Fallback: usar posição do match no texto_norm
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
        pos_fim = min(len_original, pos_fim)
    
    # Adicionar contexto: ~50 caracteres antes e depois
    contexto = 50
    inicio_contexto = max(0, pos_inicio - contexto)
    fim_contexto = min(len(texto_original_str), pos_fim + contexto)
    
    trecho_capturado = texto_original_str[inicio_contexto:fim_contexto].strip()
    
    return (1, trecho_capturado)

# Aplicar classificação
print("Classificando laudos...")
resultados = df.apply(
    lambda row: classificar_obito_fetal(row["texto_norm"], row[COLUNA_LAUDO]), 
    axis=1
)
df["obito_fetal_clinico"] = resultados.apply(lambda x: x[0])
df["termo_detectado"] = resultados.apply(lambda x: x[1])

# Estatísticas
total = len(df)
positivos = df["obito_fetal_clinico"].sum()
percentual = (positivos / total * 100) if total > 0 else 0

print("\n" + "=" * 80)
print("RESULTADOS DA CLASSIFICAÇÃO")
print("=" * 80)
print(f"Total de laudos: {total:,}")
print(f"Casos detectados: {positivos:,} ({percentual:.2f}%)")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Análise Detalhada dos Casos Positivos

# COMMAND ----------

# Filtrar casos positivos
df_positivos = df[df["obito_fetal_clinico"] == 1].copy()

if len(df_positivos) > 0:
    print("=" * 80)
    print(f"ANÁLISE DOS {len(df_positivos)} CASOS DETECTADOS")
    print("=" * 80)
    
    # Estatísticas de termos detectados
    print("\n📊 DISTRIBUIÇÃO DOS TERMOS DETECTADOS:")
    termos_count = df_positivos["termo_detectado"].value_counts()
    for termo, count in termos_count.items():
        percentual = (count / len(df_positivos) * 100)
        print(f"   - {termo}: {count} casos ({percentual:.1f}%)")
    print()
    
    # Mostrar primeiros 5 casos
    print("\n📋 PRIMEIROS 5 CASOS:")
    print("\n")
    
    for idx, row in df_positivos.head(5).iterrows():
        print(f"{'='*80}")
        print(f"CD_ATENDIMENTO: {row[COLUNA_ATENDIMENTO]}")
        print(f"TERMO DETECTADO: {row['termo_detectado']}")
        print(f"{'='*80}")
        print(row[COLUNA_LAUDO][:500] + "..." if len(row[COLUNA_LAUDO]) > 500 else row[COLUNA_LAUDO])
        print("\n")
else:
    print("Nenhum caso de óbito fetal detectado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Salvar Resultados

# COMMAND ----------

from pathlib import Path

# Criar diretório de output se não existir
Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

# Nome do arquivo de saída
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"obitos_fetais_detectados_{timestamp}.csv"
output_full_path = f"{OUTPUT_PATH}/{output_filename}"

# Salvar CSV
df.to_csv(
    output_full_path,
    index=False,
    encoding='utf-8-sig',
    sep=';',
    decimal=','
)

print(f"✅ Resultados salvos: {output_full_path}")
print(f"   Total de registros: {len(df):,}")
print(f"   Casos positivos: {positivos:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo Final e Próximos Passos

# COMMAND ----------

print("=" * 80)
print("✅ PROCESSAMENTO CONCLUÍDO")
print("=" * 80)
print(f"\n📊 RESUMO:")
print(f"   - Laudos processados: {total:,}")
print(f"   - Óbitos fetais detectados: {positivos:,} ({percentual:.2f}%)")
print(f"   - Arquivo salvo: {output_full_path}")
print("\n📋 PRÓXIMOS PASSOS:")
print("   1. Revisar amostra dos casos detectados")
print("   2. Validar classificação com auditoria médica")
print("   3. Exportar casos positivos para análise aprofundada")
print("   4. (Opcional) Ajustar padrões se necessário")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Exportar Apenas Casos Positivos (Opcional)

# COMMAND ----------

# Salvar apenas casos positivos para revisão
if len(df_positivos) > 0:
    output_positivos = f"{OUTPUT_PATH}/obitos_fetais_apenas_positivos_{timestamp}.csv"
    
    # Incluir termo_detectado no output dos positivos
    df_positivos[[COLUNA_ATENDIMENTO, COLUNA_LAUDO, "termo_detectado"]].to_csv(
        output_positivos,
        index=False,
        encoding='utf-8-sig',
        sep=';',
        decimal=','
    )
    
    print(f"✅ Casos positivos exportados: {output_positivos}")
    print(f"   Registros: {len(df_positivos):,}")
    print(f"   Colunas incluídas: {COLUNA_ATENDIMENTO}, {COLUNA_LAUDO}, termo_detectado")

