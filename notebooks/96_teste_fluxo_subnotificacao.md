## Fluxo do notebook `96_teste_fluxo_subnotificacao.py`

- **Configuração inicial**: define `PERIODO_INICIO`, `PERIODO_FIM`, janela `JANELA_DIAS` e listas de CID10 e procedimentos obstétricos monitorados. Ajusta também a tabela de auditoria (`AUDITORIA_TABLE`) e imprime um resumo dos parâmetros.
- **Bronze**: tenta ler `innovation_dev.bronze.auditoria_obitos_fetais_raw`; se ausente (ou com `FORCAR_REPROCESSAMENTO_BRONZE = True`), executa `query_laudos`, filtra laudos com texto válido e persiste o resultado bruto em Delta para evitar consultas repetidas ao Lake.
- **Extração de laudos**: usa os dados carregados da Bronze (novos ou reaproveitados), converte para Pandas e calcula estatísticas descritivas para conferência rápida.
- **Normalização e classificação**: normaliza o texto (`normalize_text`), extrai idade gestacional (`extract_semanas`), valida se há ≥22 semanas (`has_ig_above_22_semanas`) e procura padrões de óbito (`patterns_obito`). A função `classificar_obito_fetal` devolve flag binária e trecho contextual do laudo.
- **Silver**: remove colunas auxiliares após a classificação, converte os laudos positivos para Spark com schema explícito e grava em `innovation_dev.silver.auditoria_obitos_fetais_processado`.
- **Seleção de laudos positivos**: aplica `classificar_obito_fetal`, cria colunas `obito_fetal_clinico` e `termo_detectado`, filtra laudos positivos e renomeia colunas para minúsculas padronizadas. Converte o resultado para Spark `df_laudos` e adiciona `dt_referencia` a partir de `dt_procedimento_realizado`.
- **Atendimentos maternos (janela ±7 dias)**: cria view `vw_laudos_pos`, constrói `query_atendimentos_mae` combinando atendimentos HSP/PSC e cruza por paciente. A cláusula `BETWEEN` limita `DT_ATENDIMENTO` a `DT_REFERENCIA ± JANELA_DIAS`. Resultado vai para `df_atendimentos_mae` e vira view `vw_atendimentos_mae`.
- **Vínculos mãe–feto**: consulta `RAW_*_TM_ATENDIMENTO` onde existe `CD_ATENDIMENTO_MAE` para identificar atendimentos de fetos. Faz `LEFT JOIN` com `vw_atendimentos_mae`, marca flag `possui_registro_feto` e registra em `vw_vinculos_mae_feto`.
- **Diagnósticos CID10**: monta lista de CIDs (`cid_sql_list`), executa `query_cid` sobre diagnósticos HSP/PSC dentro do período, usa `COALESCE(DT_DIAGNOSTICO, DT_ATENDIMENTO)` como `DT_REFERENCIA` e filtra apenas registros validados. Armazena em `df_cid` e cria view `vw_cid_obito`.
- **Consolidação laudos × CID**: prepara chaves de laudo (`df_laudos_chave`) e diagnóstico (`df_cid_chave`), realiza `full_outer join` por paciente/atendimento, marca presença de dados (`fonte_laudo`, `fonte_cid`) e flag `mae_sem_feto` para laudos sem atendimento de feto vinculado.
- **Checagem na auditoria**: extrai atendimentos únicos (`vw_atendimentos_alvo`), cruza com `AUDITORIA_TABLE` para indicar `na_auditoria = 'SIM'` ou `'NAO'`. Junta resultado ao consolidado formando `df_resultado`.
- **Visões auxiliares**: exibe subconjuntos como mães sem registro de feto e ocorrências com CID sem laudo positivo para facilitar análise manual.
- **Exportação opcional**: fornece função `exportar_para_delta` que salva qualquer DataFrame em Delta Lake (modo `overwrite`) e deixa exemplo comentado para `df_resultado`.
- **Encerramento**: imprime mensagem confirmando conclusão do fluxo.

