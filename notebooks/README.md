# Pipeline de Detecção de Óbitos Fetais e Subnotificações

Pipeline completo para detecção de óbitos fetais através de laudos de ultrassom e CIDs diagnósticos, com análise de possíveis subnotificações.

## 📋 Visão Geral

```
┌─────────────────────────────────────────────────────────────────┐
│                    DETECÇÃO DE ÓBITOS FETAIS                    │
└─────────────────────────────────────────────────────────────────┘

FONTE 1: Laudos de Ultrassom                FONTE 2: CIDs Diagnósticos
         (Texto Livre)                            (Códigos Formais)
              │                                          │
              ▼                                          ▼
    ┌──────────────────┐                      ┌──────────────────┐
    │ 01_bronze        │                      │ Query Direta     │
    │ Extrai laudos    │                      │ P95, O36.4, etc  │
    └────────┬─────────┘                      └────────┬─────────┘
             │                                         │
             ▼                                         │
    ┌──────────────────┐                              │
    │ 02_silver        │                              │
    │ Classifica texto │                              │
    │ (óbito fetal?)   │                              │
    └────────┬─────────┘                              │
             │                                         │
             └─────────────┬───────────────────────────┘
                           ▼
               ┌────────────────────────┐
               │ 03_gold_exportacao     │
               │ Relatório de laudos +  │
               └────────────────────────┘
                           │
                           ▼
               ┌────────────────────────┐
               │ 04_gold_subnotificacao │
               │ Busca vínculos mãe-feto│
               │ Cruza com auditoria    │
               │ Detecta subnotificações│
               └────────────────────────┘
```

## 📚 Notebooks

### 1️⃣ `01_bronze_ingestao.py`
**Objetivo:** Extrair laudos de ultrassom obstétrico do datalake (HSP + PSC)

**O que faz:**
- Conecta ao datalake (RAWZN)
- Extrai laudos de ~40 procedimentos obstétricos
- Remove duplicatas por `LAUDO_ID` (FONTE_ATEND_OCOR_ORDEM)
- Grava em Delta: `innovation_dev.bronze.auditoria_obitos_fetais_raw`

**Configuração:**
- `PERIODO_INICIO` / `PERIODO_FIM`: período de extração
- `BRONZE_WRITE_MODE`: overwrite ou append

---

### 2️⃣ `02_silver_processamento.py`
**Objetivo:** Classificar laudos e identificar óbitos fetais

**O que faz:**
- Lê camada Bronze
- Aplica normalização de texto
- Detecta padrões de óbito fetal (14 patterns)
- Valida IG ≥ 22 semanas
- Captura trecho do laudo que menciona óbito
- Remove duplicatas
- Grava em Delta: `innovation_dev.silver.auditoria_obitos_fetais_processado`

**Padrões detectados:**
- "óbito fetal", "morte fetal", "óbito intrauterino"
- "feto morto", "sem batimentos cardíacos fetais"
- "sem atividade cardíaca fetal", "feto sem vitalidade"
- E mais 7 variações...

---

### 3️⃣ `03_gold_exportacao.py`
**Objetivo:** Gerar relatório Excel com laudos positivos

**O que faz:**
- Lê Silver (laudos positivos)
- Filtra por período (opcional)
- Gera estatísticas:
  - Por fonte (HSP/PSC)
  - Por termo detectado
  - Por período (mês)
- Exporta Excel com 6 abas

**Abas do Excel:**
1. Resumo Geral
2. Todos os Óbitos (laudos)
3. Pacientes Únicos
4. Estatísticas por Fonte
5. Estatísticas por Termo
6. Estatísticas por Período

---

### 4️⃣ `04_gold_analise_subnotificacao.py`
**Objetivo:** Detectar subnotificações cruzando laudos, CIDs e auditoria oficial

**O que faz:**

#### FONTE 1: Laudos Positivos (Silver)
- Lê laudos positivos da Silver
- Para cada laudo (paciente = mãe):
  - Busca atendimentos da mãe (±7 dias) via `run_sql`
  - Identifica fetos vinculados (`CD_ATENDIMENTO_MAE`)
  - Gera pares (mãe, feto)

#### FONTE 2: CIDs Diagnósticos (com Bronze)
- Busca direto no Lake diagnósticos com CIDs via `run_sql`:
  - **Núcleo:** P95, O36.4, Z37.1, Z37.4, etc (10 CIDs)
  - **Contexto:** O43.1, O69.*, etc (6 CIDs)
- **Grava em Bronze Delta:** `innovation_dev.bronze.auditoria_obitos_cids`
- Para cada diagnóstico:
  - Busca atendimentos relacionados (±7 dias)
  - Identifica se é feto (tem `CD_ATENDIMENTO_MAE`) ou mãe
  - Gera pares (mãe, feto)

#### União e Deduplicação
- Une todos os pares das duas fontes
- Remove duplicatas por (CD_PACIENTE_MAE, CD_PACIENTE_FETO)
- Marca origem: `LAUDO`, `CID` ou `AMBOS`

#### Checagem na Auditoria
- Verifica se mãe ou feto estão em `TB_AUDITORIA_OBITO_ITEM`
- Filtra apenas **NÃO AUDITADOS** = **SUBNOTIFICAÇÕES**

**Abas do Excel:**
1. Resumo Geral
2. **Subnotificações** (casos NÃO auditados)
3. Todos os Casos
4. Estatísticas por Origem
5. Estatísticas Feto Identificado

---

## 🚀 Como Executar

### Ordem de Execução:

```bash
# 1. Ingestão de laudos (Bronze)
01_bronze_ingestao.py

# 2. Classificação de óbitos (Silver)
02_silver_processamento.py

# 3. Relatório de laudos positivos (opcional)
03_gold_exportacao.py

# 4. Análise de subnotificações (principal)
04_gold_analise_subnotificacao.py
```

### ⚙️ Configurações Importantes:

#### Todos os notebooks:
- `PERIODO_INICIO` / `PERIODO_FIM`: período de análise

#### 04_gold_analise_subnotificacao.py:
- `JANELA_DIAS`: janela temporal para buscar vínculos (padrão: 7)
- `CID10_LIST`: lista de CIDs monitorados (16 códigos)
- `AUDITORIA_TABLE`: tabela da auditoria oficial
- `FORCAR_REPROCESSAMENTO_CID`: True para forçar nova extração dos CIDs (padrão: False)

---

## 📊 Output

### 03_gold_exportacao.py
```
obitos_fetais_YYYYMMDD_HHMMSS.xlsx
├─ Resumo (métricas gerais)
├─ Todos os Óbitos (laudos individuais)
├─ Pacientes Únicos (primeiro óbito por paciente)
├─ Stats por Fonte (HSP vs PSC)
├─ Stats por Termo (padrões detectados)
└─ Stats por Período (tendência temporal)
```

### 04_gold_analise_subnotificacao.py
```
subnotificacoes_YYYYMMDD_HHMMSS.xlsx
├─ Resumo (métricas de subnotificação)
├─ Subnotificações (casos NÃO auditados) ⚠️
├─ Todos os Casos (auditados + não auditados)
├─ Stats por Origem (LAUDO/CID/AMBOS)
└─ Stats Feto (com/sem feto identificado)
```

---

## 🔍 Interpretação dos Resultados

### Possíveis Cenários:

| Cenário | Descrição | Ação |
|---------|-----------|------|
| `origem_deteccao = AMBOS` + `na_auditoria = NAO` | Laudo E CID detectaram, mas não auditado | ⚠️ **Alta prioridade** |
| `origem_deteccao = LAUDO` + `na_auditoria = NAO` | Apenas laudo detectou | Validar com clínico |
| `origem_deteccao = CID` + `na_auditoria = NAO` | Apenas CID detectou | Verificar contexto |
| `cd_paciente_feto = NULL` | Feto não identificado no sistema | Verificar se foi registrado |
| `mae_na_auditoria = SIM` + `feto_na_auditoria = NAO` | Mãe auditada mas feto não | Possível gap no registro |

---

## 📝 Observações

### Padrões de Óbito Fetal
- Requerem **IG ≥ 22 semanas** para serem considerados
- 14 padrões textuais validados clinicamente
- Normalização Unicode para capturar variações

### CIDs Monitorados
- **Núcleo (10):** Alta especificidade para óbito fetal
- **Contexto (6):** Complicações associadas (placenta/cordão)
- Validação por `FL_VALIDADO = 'S'`

### Janela Temporal
- Padrão: **±7 dias** do laudo/diagnóstico
- Captura atendimentos relacionados (pré-natal, parto, pós-parto)
- Ajustável via `JANELA_DIAS`

---

## 🛠️ Manutenção

### Atualizar CIDs:
Edite `CID10_LIST` em `04_gold_analise_subnotificacao.py`

### Atualizar Padrões de Texto:
Edite `patterns_obito` em `02_silver_processamento.py`

### Alterar Período:
Ajuste `PERIODO_INICIO` e `PERIODO_FIM` em cada notebook

---

## 📚 Dependências

- PySpark
- Pandas
- openpyxl (instalado automaticamente)
- Acesso ao datalake (RAWZN)
- Biblioteca interna: `/Workspace/Libraries/Lake`

---

## 🔧 Notas Técnicas

### Uso de `run_sql` vs `spark.sql`

O notebook `04_gold_analise_subnotificacao.py` utiliza `run_sql` (da biblioteca Lake) ao invés de `spark.sql` para evitar **erros de permissão de catalog** ao fazer JOINs complexos entre tabelas Delta e RAWZN.

**Vantagens:**
- ✅ Evita `INSUFFICIENT_PRIVILEGES` em queries complexas
- ✅ Acesso direto ao Lake sem problemas de catalog
- ✅ Mais controle sobre a execução

**Trade-off:**
- ⚠️ Processamento iterativo pode ser mais lento em datasets grandes
- ⚠️ Considere otimizar para produção se volume crescer muito

### Camada Bronze CID

Os CIDs são salvos em Bronze (`innovation_dev.bronze.auditoria_obitos_cids`) para:
- Evitar reprocessamento desnecessário
- Permitir auditoria dos dados extraídos
- Facilitar debugging

Use `FORCAR_REPROCESSAMENTO_CID = True` para forçar nova extração.

---

## 👥 Suporte

Para dúvidas ou sugestões sobre o pipeline, consulte a documentação técnica ou entre em contato com a equipe de Innovation.

