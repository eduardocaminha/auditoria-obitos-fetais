# Auditoria de Óbitos Fetais

Sistema de classificação automática de laudos de ultrassom obstétrico para detecção de óbitos fetais usando regras clínicas baseadas em padrões textuais.

## 📋 Objetivo

Identificar laudos de ultrassom obstétrico com suspeita de óbito fetal para fins de auditoria e análise epidemiológica.

## 🚀 Como Usar

### 1. Executar no Databricks

1. Abrir notebook `notebooks/processar_obitos_fetais.py`
2. Ajustar configurações:
   - `INPUT_CSV_PATH`: caminho do CSV com laudos
   - `OUTPUT_PATH`: onde salvar resultados
3. Executar todas as células

### 2. Formato de Entrada

**CSV de entrada esperado:**
- `CD_ATENDIMENTO`: ID único do atendimento
- `DS_LAUDO_MEDICO`: texto completo do laudo

### 3. Resultado

**CSV de saída contém:**
- Todas as colunas originais do CSV de entrada
- `texto_norm`: texto normalizado (para debug)
- `obito_fetal_clinico`: classificação (1 = óbito fetal detectado, 0 = não detectado)

## 📊 Lógica de Classificação

### Regras de Triagem

Para ser classificado como óbito fetal, o laudo deve:

1. **Conter padrão de óbito**: termos como:
   - "óbito fetal"
   - "morte fetal"
   - "óbito intrauterino"
   - "feto morto"
   - "sem batimentos cardíacos fetais"
   - "sem atividade cardíaca fetal"
   - "feto sem vitalidade"
   - etc.

2. **Mencionar semanas gestacionais**: contextos como:
   - "33 semanas", "25 semanas"
   - "25s", "33s"
   - "25s01d", "25s 01d"

3. **NÃO ser exclusão**: caso mencione:
   - "gestação tópica não evolutiva"
   - Abortos/abortamentos
   - Ovo anembrionado
   - Restos ovulares
   - Saco gestacional vazio
   - etc.

## 🔍 Validação

### Próximos Passos Após Detecção

1. **Revisão médica**: todos os casos positivos devem ser validados
2. **Auditoria**: verificar se são casos reais de óbito fetal
3. **Refinamento**: ajustar padrões se necessário
4. **Análise epidemiológica**: realizar análises estatísticas

## 📁 Estrutura do Projeto

```
.
├── README.md                              # Este arquivo
└── notebooks/
    └── processar_obitos_fetais.py        # Notebook principal
```

## ⚠️ Importante

- Este sistema é uma **ferramenta de triagem**, não substitui a avaliação médica
- Todos os casos detectados devem ser revisados por profissionais
- A lógica é conservadora (prioriza evitar falsos positivos)
- Pode haver casos de óbito fetal que não sejam detectados (falsos negativos)

## 📧 Suporte

Para dúvidas ou ajustes na lógica de classificação, revisar:
- Padrões em `patterns_obito` e `patterns_excluir`
- Funções `extract_semanas()` e `mentions_semanas()` para semanas gestacionais

