# Auditoria de Óbitos Fetais

Sistema automatizado de classificação de laudos de ultrassom obstétrico para detecção de óbitos fetais usando regras clínicas baseadas em padrões textuais.

## 📋 Objetivo

Identificar laudos de ultrassom obstétrico com suspeita de óbito fetal para fins de auditoria e análise epidemiológica, utilizando pipeline automatizado Bronze/Silver/Gold no Databricks.

## 🏗️ Arquitetura

O projeto segue uma arquitetura em camadas Delta Lake:

- **Bronze**: Ingestão de dados brutos (HSP + PSC)
- **Silver**: Processamento e classificação de óbitos fetais
- **Gold**: Consolidação e exportação de resultados

## 🚀 Pipeline Automatizado (Job Diário)

### 1. Bronze: Ingestão (`01_bronze_ingestao.py`)

- Extrai laudos de ultrassom obstétrico de **HSP** e **PSC** (Union All)
- Filtra por 40 códigos de procedimento específicos
- Inclui informações do paciente (`CD_PACIENTE`, `NM_PACIENTE`)
- Salva em Delta Lake: `innovation_dev.bronze.auditoria_obitos_fetais_raw`
- Processa apenas o dia anterior (configurável)
- Gera chave única `LAUDO_ID` para evitar duplicatas

**Tabela Bronze:**
- `FONTE` (HSP/PSC)
- `CD_ATENDIMENTO`, `CD_OCORRENCIA`, `CD_ORDEM`
- `CD_PROCEDIMENTO`, `NM_PROCEDIMENTO`
- `DS_LAUDO_MEDICO`
- `DT_PROCEDIMENTO_REALIZADO`
- `CD_PACIENTE`, `NM_PACIENTE`
- `LAUDO_ID` (chave única)
- `DT_INGESTAO`

### 2. Silver: Processamento (`02_silver_processamento.py`)

- Lê dados do Bronze
- Normaliza textos (remove acentos, minúsculas)
- Classifica óbitos fetais usando padrões textuais
- Verifica idade gestacional >= 22 semanas
- Extrai trecho exato do texto original com contexto (~50 caracteres)
- Salva em Delta Lake: `innovation_dev.silver.auditoria_obitos_fetais_processado`

**Tabela Silver:**
- Todas as colunas do Bronze
- `texto_normalizado`
- `obito_fetal_clinico` (0/1)
- `termo_detectado` (trecho do texto original)
- `DT_PROCESSAMENTO`

### 3. Gold: Exportação (`03_gold_exportacao.py`)

- Lê dados do Silver (últimos 30 dias)
- Remove duplicatas por paciente (mantém primeiro caso)
- Exporta Excel com múltiplas abas:
  - **Positivos**: Casos únicos por paciente
  - **Estatísticas**: Métricas gerais
  - **Por Fonte**: Distribuição HSP vs PSC
  - **Termos Detectados**: Distribuição dos padrões encontrados

**Output:**
- Arquivo Excel: `obitos_fetais_YYYYMMDD.xlsx`
- Localização: `/Workspace/Innovation/t_eduardo.caminha/auditoria-obitos-fetais/outputs`

## 📊 Lógica de Classificação

### Regras de Triagem

Para ser classificado como óbito fetal, o laudo deve:

1. **Conter padrão de óbito**: termos como:
   - "óbito fetal"
   - "morte fetal"
   - "óbito intrauterino"
   - "feto morto"
   - "sem batimentos cardíacos fetais"
   - "ausência de batimentos cardíacos fetais"
   - "batimentos cardíacos fetais não caracterizados"
   - "sem atividade cardíaca fetal"
   - "feto sem vitalidade"
   - "sem movimentos fetais"
   - "movimentos corpóreos/fetais não caracterizados"
   - "cessação de atividade cardíaca"
   - "morte do feto"

2. **Mencionar idade gestacional >= 22 semanas**: formatos como:
   - "33 semanas", "25 semanas"
   - "25s", "33s"
   - "25s01d", "25s 01d"
   
   **Importante:** Abortos (< 22 semanas) são automaticamente excluídos.

### Padrões de Exclusão

- Removidos (filtrados por IG >= 22 semanas)
- Abortos naturalmente ficam de fora por terem IG < 22 semanas

## 🔧 Utilitários

### Notebooks Manuais

- **`00_extracao_laudos_manual.py`**: Extração manual de períodos específicos
  - Gera CSV para testes
  - Não faz parte do pipeline automatizado

- **`00b_processar_obitos_fetais_standalone.py`**: Processamento standalone de CSV
  - Processa CSV manualmente
  - Para testes e validações
  - Não faz parte do pipeline automatizado

- **`99_limpar_tabelas.py`**: Limpeza de tabelas Delta Lake
  - Deleta Bronze e Silver (IF EXISTS)
  - Útil para resetar pipeline e testes

## 📋 Códigos de Procedimento

O pipeline processa **40 códigos** de procedimento de ultrassom obstétrico:

```
33010110, 33010250, 33010269, 33010285,
33010295, 33010293, 40901238, 40901246,
40901505, 33010390, 33010501, 33020019,
99030250, 99030293, 33010360, 33019061,
33999901, 98409220, 98224063, 98409031,
98409043, 90020251, 33010382, 40901254,
40901289, 40901297, 40901262, 33010307,
40902013, 40901270, 33010609, 40902021,
99030110, 99030111, 98409145, 98409029,
98409033, 98409239, 98409030, 33010375
```

## 📁 Estrutura do Projeto

```
auditoria-obitos-fetais/
├── README.md
└── notebooks/
    ├── 00_extracao_laudos_manual.py          # Extração manual
    ├── 00b_processar_obitos_fetais_standalone.py  # Processamento standalone
    ├── 01_bronze_ingestao.py                  # Bronze: Ingestão
    ├── 02_silver_processamento.py             # Silver: Processamento
    ├── 03_gold_exportacao.py                  # Gold: Exportação
    └── 99_limpar_tabelas.py                    # Limpeza de tabelas
```

## 🔍 Validação

### Próximos Passos Após Detecção

1. **Revisão médica**: todos os casos positivos devem ser validados
2. **Auditoria**: verificar se são casos reais de óbito fetal
3. **Refinamento**: ajustar padrões se necessário
4. **Análise epidemiológica**: realizar análises estatísticas

## ⚠️ Importante

- Este sistema é uma **ferramenta de triagem**, não substitui a avaliação médica
- Todos os casos detectados devem ser revisados por profissionais
- A lógica é conservadora (prioriza evitar falsos positivos)
- Pode haver casos de óbito fetal que não sejam detectados (falsos negativos)
- O pipeline evita duplicatas por paciente (mantém apenas primeiro caso)

## 🎯 Configuração do Job Databricks

### Job Diário Recomendado

1. **Tarefa 1**: `01_bronze_ingestao.py`
   - Agendamento: Diário (00:00)
   - Cluster: Job cluster

2. **Tarefa 2**: `02_silver_processamento.py`
   - Depende de: Tarefa 1
   - Cluster: Job cluster

3. **Tarefa 3**: `03_gold_exportacao.py`
   - Depende de: Tarefa 2
   - Cluster: Job cluster

## 📧 Suporte

Para dúvidas ou ajustes na lógica de classificação, revisar:
- Padrões em `patterns_obito` no notebook Silver
- Funções `extract_semanas()` e `has_ig_above_22_semanas()` para semanas gestacionais
- Configuração de códigos de procedimento em `01_bronze_ingestao.py`
