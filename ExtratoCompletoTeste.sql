-----------------------------------------------------------------------------------------------------------------------------
-- Criada     : 22/11/2006
-- Responsável: Eduardo Correa
-----------------------------------------------------------------------------------------------------------------------------
-- RPT: R_CAR_Extrato_Cliente.rpt
--      R_CAR_Extrato_Completo.rpt
-----------------------------------------------------------------------------------------------------------------------------
-- Fatos Relevantes:
-- V.07 -- Revisor: 12/11/2010 - Noeliza
-- V.07 -- Auditor: 22/02/2011 - Eliani
-- V.08 -- Revisor: 09/11/2012 - Daniel Nascimento
-- V.08 -- Auditor: 14/11/2012 - Guilherme Chiconato
-- v.09 -- Revisor: 20/06/2013 - Daniel Nascimento
-- v.09 -- Auditor: 21/06/2013 - Guilherme Chiconato
-- v.10 -- Revisor: 10/09/2014 - Caio Ernani Nogarotto
-- V.10 -- Auditor: 19/09/2014 – Jaqueline Silva
-- V.11 -- Revisor: 01/10/2014 - Jaqueline Silva
-- V.11 -- Auditor: 24/11/2014 - Anacris Kosinski
-----------------------------------------------------------------------------------------------------------------------------
-- Parâmetros:
-- v_st_termo       : 'CFTETN' - todos
--                    'C'      - parcelas contratuais
--                    'F'      - parcelas não contratuais
--                    'TE'     - parcelas referentes a termos contratuais
--                    'TN'     - parcelas referentes a termos não contratuais
--                    'CTE'    - parcelas contratuais e termos contratuais
--                    'TETN'   - termos contratuais e termos não contratuais
-- v_tdes           : Desconsiderar termos (valores inteiros separados por '-') recebe conteúdo do Lookup
-- v_tdes1          : Dsconsiderar termos (valores inteiros separados por '-') recebe conteúdo do Edit
-- v_tcon           : Considerar termos (valores inteiros separados por '-') recebe conteúdo do Lookup
-- v_tcon1          : Considerar termos (valores inteiros separados por '-') recebe conteúdo do Edit
-- v_tipoindice     : Usuário optará em qual tipo de índice, para o reajuste dos valores de correção a ser aplicado sobre as parcelas:
--                    "RP"  - Opta-se por NÃO utilizar a tabela CAR_H_PARCELA, onde esta o histórico das modificações realizadas na parcela
--                    "RPS" - Opta-se em utilizar o histórico CAR_H_PARCELA, no momento dos cálculos de correção do valor das parcelas.
--                    Esta opção será disponibilizada na rotina onde se pode voltar no tempo, para saber o valor efetivo da parcela na época (data base).
-- v_data_base      : Data que será utilizado para os cálculos dos reajustes dos valores das parcelas/contrato
-- cod_est          : Código do empreendimento - Obrigatório no Extrato Cliente
-- cod_cto          : Código do contrato - 0 lista todos(somente o extrato Cliente)
-- cod_uni          : Codigo da Unidade para a consulta do contrato (caso não informado a consulta será pelo contrato)
-- cod_blc          : Código do bloco - 0 lista todos(somente o Extrato Cliente)
-- v_descongela     : Descongelar ou não as parcelas com correção anual: 'A' - Descongelar | 'M' - Congelar
-- v_ctrl_cheque    : Controle de Cheques:                               'S' - Considerar  | 'N' - Não Considerar
-- v_tipo_parcela   : Tipo de Parcela: 'S' - Mostra a sigla do tipo de parcela de acordo com o padrão (S - Sinal, M - Mensal, I - Intermediária, C - Conclusão)
--                                     'N' - Mostra a sigla do tipo de parcela de acordo com regra definida pela Trisul.
-- v_descap_tp      : Descapitaliza parcelas TP:              'S' - Descapitalizar     | 'N' - Não Descapitalizar
-- v_ati            : Status de Contratos Ativos:             'A' - Ativos             | 'X' - Desconsiderar
-- v_qui            : Status de Contratos Quitados:           'Q' - Quitado            | 'X' - Desconsiderar
-- v_ina            : Status de Contratos Inadimplentes:      'U' - Inadimplentes      | 'X' - Desconsiderar
-- v_dis            : Status de Contratos Distratados:        'D' - Distratados        | 'X' - Desconsiderar
-- v_ces            : Status de Contratos Cessão de Direitos: 'C' - Cessão de Direitos | 'X' - Desconsiderar
-- v_tra            : Status de Contratos Transferidos:       'T' - Transferidos       | 'X' - Desconsiderar
-- v_descap_sac     : Descapitaliza parcelas SAC :            'S' - Descapitalizar     | 'N' - Não Descapitalizar
-- v_mostra_cheque  : Identificar na listagem das parcelas cheque pré em NEGRITO: 'S' - Mostrar    | 'N' - Não mostrar
-- p_cons_taxa      : Considerar a taxa de capitalização da tela de parâmetros:   'S' - Considerar | 'N' - Não Considerar
-- p_cons_bonif     : Considerar a taxa de bonificação da tela de parâmetros:     'S' - Considerar | 'N' - Não Considerar
-- p_parc_caucao    : Tipo Parcela: 'T' - Todas | 'C' - Caucionadas | 'N' - Não Caucionadas
-- p_vl_corrigido   : Opção de mostrar o valor corrigido sem considerar as descapitalizações sacoc e TP, sendo:
--                    'S' - deverá considerar os parâmetros v_descap_sac e v_descap_tp como desmarcados 'N', de forma fixa.
--                    'N' - deverá considerar os parâmetros v_descap_sac e v_descap_tp confome tela de paramentros do relatorio
-- p_investidor     : Considra o Percentual da Organização, deduzindo o valor referente ao percentual dos investidores: 'S' - Considerar | 'N' - Não Considerar
-- p_cons_confdivida: Considera parcelas de confissão de dívida: 'S' - Considerar | 'N' - Não Considerar
-- p_agn_in_codigo  : Considerar agente específico na emissão ou 0(zero) para considerar todos
-----------------------------------------------------------------------------------------------------------------------------
-- Alterações:
-- 22/11/2006 - Eduardo correa: Redesenvolvida a procedure para retornasse contratos por empreendimento e por blocos. Ch 13370
-- 22/08/2007 - Paulo: Alterado o retorno do campo par_re_valordesconto do select de parcelas abertas para 0. Ch 16992
-- 17/09/2007 - Cleiton: Alterado select que busca o contrato pela unidade, para considerar apenas U e G, sendo por bens
--                       a consulta sera sempre por contrato pois o lookup de unidade nao considera bens. ch. 17521
-- 26/10/2007 - Paulo: Inserido no retorno o campo cto_st_descricao. Ch 17604
-- 11/01/2008 - Cleiton: Alterado campo par_st_obs_cliente VARCHAR2(1000) para cto_st_observacao VARCHAR2(2000),
--                       para consistir com o campo da tabela mgcar.car_contrato que possui 2000. ch. 19135
-- 29/08/2008 - Eduardo Correa: Alterado o campo de retorno cobpar_in_codigo, para que demonstrasse corretamente o código da
--                              parcela de residuo gerada ou a descrição C.Créd. conforme tela do sistema. Ch 23586
-- 06/10/2008 - Eliani: Incluso parâmetro "v_cli_rel" para tratar alguns campos de forma específica para a Constel. Incluso
--                      os campos "par_in_codigoindice" e "par_re_valorindice". Ch. 20655
-- 13/10/2008 - Paulo: inserida a função fnc_car_parcela_de_residuo para tratamento de data de processo do resíduo na parcela. Ch 24723
-- 22/10/2008 - Eduardo Santos: Inserida a funcao mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa, para retornar a defasagem das parcelas
--                              Inserido o campo de retorno agn_st_email para utilização no formato. ch 25092.
-- 31/10/2008 - Paulo Chaves: Inserido o parâmetro v_descongela para descongelar ou não parcelas com correção anual. Ch 25245
-- 10/12/2008 - Paulo Chaves: Corrigida procedure para considerar data de processo no retorno de resíduos. Ch 24723
-- 10/12/2008 - Eduardo Santos: Inserido retorno do campo cto_dt_assinatura, este foi inserido na tabela temporária mgrel.rel_dados_contrato no campo ind_dt_vigencia Ch. 26737
-- 12/12/2008 - Eduardo Santos: Inserido tratamento de cast para os campos cto_ds_tipo e par_st_receita(alias = cus_st_descricao) para que possibilitasse o vinculo dos mesmos nos subrelatórios do formato. ch. 25608
-- 17/12/2008 - Eduardo Santos: Inserido tratamento para controle de cheques, inserido parâmetro v_ctrl_cheque, Ch. 26446.
-- 23/12/2008 - Eduardo Santos: Inserido campo de retorno cnd_st_observacao para informar as observações da proposta. Ch. 26244.
-- 19/05/2009 - Eliani: Retirado o parâmentro "v_cli_rel" e os tratamentos realizadas para a Constel. Desconsiderado o formato da Constel "R_CAR_Extrato_Cliente_0168.rpt". Ch. 20655
-- 19/05/2009 - Paulo Chaves: Alterado o campo par_re_valorjurosbx para parcelas abertas para que retorne o valor descapitalizado quando o parâmetro v_descap_tp estiver marcado. Ch 30239
-- 01/06/2009 - Eduardo Santos: aplicado a variavel descongela na chamada da funcao fnc_car_corrige dentro das funções fnc_dbm_calculaatraso e fnc_dbm_calculamora ch.30460
-- 02/06/2009 - Eliani: Incluso o parâmetro "v_tipo_parcela". Inclusa a tabela "mgdbm.dbm_condicao_item" e os campos "par_ch_receita", "cndit_in_intervalo" nos selects de parcelas. Incluso tratamento de tipo de
--                      parcela (Trisul) no select principal. Ch. 29897
-- 17/06/2009 - Paulo Chaves: Inserido parâmetros para filtro por status de contratos. Ch 30817
-- 13/07/2009 - Cássia: Alterada as chamadas das funções mgdbm.fnc_dbm_calculaatraso e mgdbm.fnc_dbm_calculamora para a MGCAR.PCK_CAR_FNC.FNC_CAR_CALCULAENCARGOS. Ch.12515
-- 10/11/2009 - Eliani: Incluso parâmetro "v_descap_sac" e alteradas as funções que possuem o parâmetro "Descongela", para considerar o novo parâmetro e o "v_descap_tp".
--                      Substituído o campo par_bo_tabelaprice pelo campo par_ch_amortizacao ou pelo par_ch_origem. Incluso o item 1 de Todos os Mod. e o item 7 do Mod.
--                      Carteria do manual de fatos relevantes. Ch. 32972
-- 02/02/2010 - Cássia: Alterado campo de retorno do agn_st_telefone para cto_ds_origem para comportar telefones com mais digitos. Ch.35633
-- 08/03/2010 - Marino: Adicionado novo status "Cheque em Custódia" para a contagem dos Cheques Pré. Ch. 32951
--                      Alterado o filtro de parcelas para considerar corretamente contratos de aluguéis (Fatos Relevantes V02 Item 4 Mód. Carteira)
--                      Alterada a view "mgrel.vw_rel_estrutura_contrato" para a view "mgrel.vw_car_estrutura". (Fatos Relevantes V02 Item 7 Mód. Carteira)
-- 13/05/2010 - Marino: Criado o formato R_CAR_Extrato_Completo_0059.rpt.
--                      Adicionado os campos de retorno "par_ds_parcela", "par_in_intervalo" e "par_ds_intervalo". Ch. 38124.
-- 18/06/2010 - Noeliza: Adicionado o campo de retorno "und_re_areaprivativa". Ch 38488
-- 06/07/2010 - Marino: Substituído o campo "cto_re_valorcontrato" pela função "mgrel.pck_rel_fnc.fnc_car_valor_contrato".
--              Inserido nas funções "mgcar.pck_car_fnc.fnc_car_corrige" o parâmetro VM_ATRASO. Ch. 35671.
-- 11/08/2010 - Noeliza: Criado o formato R_CAR_Extrato_Completo_0067.rpt.
-- 12/11/2010 - Noeliza: Alterado os campos 'valor_quitacao' e 'vl_quit_tot' adicionando o parâmetro v_descongela no cálculo da função para trazer o valor descongelado ou congelado no valor presente. Ch 42202 .
-- 22/02/2011 - Eliani: Readequada procedure ao padrão atual de desenvolvimento. Ch. 32151
-- 21/03/2011 - Alexandre Rieper: Criado parâmetro "v_mostra_cheque" para poder identificar em NEGRITO na listagem das parcelas, as pagas com cheque pré. Ch. 45174
-- 09/05/2011 - Cássia: Adicionados parâmetros de taxas de descapitalização e bonificação. Ch. 45310
-- 28/11/2011 - Janaina: Incluso o parâmetro "p_parc_caucao" como opção para listar no relatório apenas parcelas caucionadas, não caucionadas ou todas. Ch. 43414.
-- 21/11/2011 - Eliani: Incluso o parâmetro "p_vl_corrigido", bem como o tratamento para não descapitalizar as parcelas para o valor corrigido, quando este novo parâmetro estiver marcado. Ch. 50296.
-- 19/06/2012 - Daniel Nascimento: Inserido o parâmetro "v_reajuste" no select de parcelas em aberto, para o campo "par_re_valorjurosbx". Ch.342957
-- 22/06/2012 - Daniel Nascimento: Inserido o select para determinar o Percentual da Organização para os valores dos contratos. Ch.341283
-- 18/09/2012 - Daniel Nascimento: Inscluso o campo de "par_re_valorcorrecao_atr" nos selects de parcelas em aberto e pago para exibir o valor de VM de Atraso. Ch.352007
-- 14/11/2012 - Guilherme Chiconato: Removido o 1=1 e 1=2 do select de cheques. Ch. 358966
-- 02/04/2013 - Guilherme Chiconato: Colocado o NVL fora do sum no select de cheque-pre. Ch. 372944
-- 08/05/2013 - Felipe Zanin: Corrigido os pontos encontrado pelo Cleiton, na validação do relatório Ch PM: 320932.
-- 10/05/2013 - Guilherme Chiconato: Retirados os relatórios de SP da relação dos relatórios que utilizam a procedure. Ch. 372944
-- 28/05/2013 - Guilherme Chiconato: Adicionado o parâmetro considera_confdivida. Ch 395891.
-- 20/06/2013 - Daniel Nascimento: Tratado para não trazer parcelas inativas para o select das datas da primeira e última parcela. Ch. 22922
-- 30/09/2013 - Daniel Nascimento: Incluso NVL no campo "rescob_dt_processo" do "par_re_valorencargos" no select de parcelas abertas para não
--              somar o resíduo de cobrança para quando a data for null. Ch. 51365
-- 19/11/2013 - Anacris Kosinski: Incluído ORDER BY para ordenar as informações por data de vencimento e código da parcela. Ch: 64876.
-- 10/09/2014 - Caio Ernani Nogarotto: Incluídos valores de taxas dos contratos na variável v_vlr_apagar. PM: 133248
-- 19/09/2014 - Jaqueline Silva: * Auditando FR: Alterados os select's INTO's de cheques, onde foi inserida a verificação da geração do resíduo (usando a data base) para considerar ou não o valor na soma dos valores;
--                               * No select INTO "v_vlr_apagar" e no select de parcelas "ABERTAS": Foram excluídos os relacionamentos "AND res.par_in_codigo(+)= par.par_in_codigo", uma vez que o PAR_IN_CODIGO
--                                 deve ser relacionado somente quando queremos retornar os dados da parcela que gerou o resíduo, sendo que parcelas em aberto não geram resíduos de cobrança.
--                               * Retirado o valor de resíduo de cobrança do campo "par_re_valorencargos", uma vez que após alinhar com Asfaw/Valmir, ambos entenderam ser desnecessário somar o resíduo no
--                                 campo de "Encargos" do RPT, visto que este valor tem campo específico para ser apresentado no relatório. Ch.: 136521
-- 21/10/2014 - Jaqueline Silva: Inserido no retorno da PRC o campo cto.cto_re_vlroricontrato para mostrar somente o VO do contrato, sem aditivos.
--                               Devido o Cliente Ecocil já estar no Mega 4, foi retirado o RPT (R_CAR_Extrato_Cliente_0065.rpt) de dependência da PRC. PM 143861
-- 24/11/2014 - Anacris Kosinski: Incluída novamente a chave "AND res.par_in_codigo(+)= par.par_in_codigo" nos selects de parcelas "ABERTAS", pois essa chave
--                                faz parte da chave padrão entre as tabelas "mgcar.car_parcela par" e "mgcar.car_residuo_cobranca". Ch: 155598.
-- 06/01/2015 - Jaqueline Silva: Inserido o parâmetro "p_agn_in_codigo" para poder emitir o relatório (somente o R_CAR_Extrato_Cliente.rpt) por determinado agente. Ch.: 158538
-- 11/06/2015 - Jaqueline Silva: Corrigida chamada da função "fnc_car_total_taxasparcela" no INTO "v_vlr_apagar" para não passar data como parâmetro da função.Issue 1672
-- 21/12/2015 - Caio Ernani Nogarotto: Inseridos nvls, nos parametros do relatório, para no caso do não preenchimento, ser substituido por 0. PINC-2938.
-- 18/03/2016 - Rafael Schulze: Inserido tratamento de resíduo de cobrança no campo rescob_re_valoracobrar (93). PINC-3225
-- 13/05/2016 - André Luiz Carraro: Adicionado função para calcular o valor de resíduo e resíduo corrigido para as parcelas em aberto, conforme o parametro [cto_ch_reajusteanual] da [mgcar.car_contrato]. PINC-3300
-- 14/07/2016 - André Luiz Carraro: Corrigido tratamento de resíduo de cobrança. PINC-3681
-- 24/08/2016 - André Luiz Carraro: Alterado para buscar informações do cliente antigo caso cessão de direitos e data base menor igual a data da cessão. PINC-3803
-- 06/09/2016 - André Luiz Carraro: Corrigo calculo do valor corrigido e bonificação. PINC-3852
-- 08/09/2016 - Alex Xavier: Correção dos campos: Vlr corrigido, Valor presente e Valor quitação. PINC-3852
-- 30/09/2016 - CMS - PWEB-3938 - Foram ajustados os campos par_re_valorcorrigido_desc(72), par_re_valorcorrecaobx(98), valor_quitacao(106), vl_bonificacao(107) para
--              verificar corretamente qual taxa de descapitalização será usada.
-- 07/02/2017 - Alexandre W. Rieper: Considerado na "v_vlr_apagar" o valor de resíduo à gerar corrigido. PINC-4292
-- 09/02/2017 - Alexandre W. Rieper: Desconsiderar no campo "par_re_valorcorrecaobx(98)" os parâmetros de descapitalização TP/SAC, ou seja, deve sempre passar o valor original da parcela. PINC-4318
-- 13/06/2017 - André Luiz Carraro: Corrigido para não descaptalizar o valor atualizado [ par_re_valoratualizado ]. PINC-4534
-- 05/06/2018 - André Luiz Carraro: Alterado para não trazer Resíduo de Cobrança para contrato quitado. POPF-181
-- 08/06/2018 - André Luiz Carraro: Corrigido campo [PAR_RE_VALORENCARGOS] respeitando o parâmetro [v_reajuste]. POPF-196
-- 28/02/2019 - André Luiz Carraro: Adicionado parâmetro [p_par_secur] para considerar ou não parcelas securitizadas. POPF-514
-- 22/03/2019 - André Luiz Carraro: Somado ao valor a pagar e quitação o valor de resíduo corrigido. POPF-546
-- 28/05/2019 - André Luiz Carraro: Criado parâmetro [p_cons_jr_tp_sac] para considerar ou não juros tp/sac separados do VO. POPF-663
-----------------------------------------------------------------------------------------------------------------------------
DECLARE
  v_tab              NUMBER(3);
  v_pad              NUMBER(3);
  v_cod              NUMBER(7);
  v_fil              NUMBER(7);
  v_tau              VARCHAR2(3);
  v_emp              NUMBER(22);
  v_blc              NUMBER(22);
  v_cto              NUMBER(22);
  v_dt_base          DATE;
  v_dt_ini           DATE;         -- data da primeira parcela
  v_dt_fim           DATE;         -- data da última parcela
  v_cheque_pre       NUMBER(7);    -- Indica a quantidade de parcelas pagas com cheques pré
  v_termo            VARCHAR2(10);
  v_des              VARCHAR2(22);
  v_des1             VARCHAR2(22);
  v_con              VARCHAR2(22);
  v_con1             VARCHAR2(22);
  v_const            VARCHAR2(50); -- Variavel para concatenar códigos dos TERMOS A CONSIDERAR da lookup e do objeto Edit da tela de parametros
  v_desct            VARCHAR2(50); -- Variavel para concatenar códigos dos TERMOS A DESCONSIDERAR da lookup e do objeto Edit da tela de parametros
  v_vlr_apagar       NUMBER(22,8);
  v_vlr_pago         NUMBER(22,8);
  v_reajuste         CHAR(1);
  v_ctrl_cheq        CHAR(1);
  v_descapit_tp      CHAR(1);
  v_descapit_sac     CHAR(1);
  v_tp_par           CHAR(1);
  v_ativo            CHAR(1);
  v_quit             CHAR(1);
  v_inadim           CHAR(1);
  v_distr            CHAR(1);
  v_cessao           CHAR(1);
  v_trans            CHAR(1);
  v_cons_taxa        CHAR(1);
  v_cons_bonif       CHAR(1);
  v_mostra_chq       CHAR(1);  -- Ch. 45174
  v_vl_corrigido     CHAR(1);
  v_vlr_cheque_pre   NUMBER(22,8);
  v_consinv          CHAR(1);
  v_perc_org         NUMBER;
  v_conf_divida      CHAR(1);
  v_agn_in_codigo    NUMBER(22,8);
  vs_IndiceProjetado CHAR(1);
  v_par_secur        CHAR(1);
  vnTotalResiduo     NUMBER(22, 8);
  vsConsJrsTPSAC     CHAR(1);

  CURSOR cur_cto_emp IS
    SELECT est.emp_in_codigo
        , est.emp_st_codigo
        , est.emp_st_nome
        , est.etp_in_codigo
        , est.etp_st_codigo
        , est.etp_st_nome
        , est.blo_in_codigo
        , est.blo_st_codigo
        , est.blo_st_nome
        , est.und_in_codigo
        , est.und_st_codigo
        , est.und_st_nome
        , est.und_re_areaprivativa
        --Dados do contrato
        , cto.org_tab_in_codigo  cto_org_tab_in_codigo
        , cto.org_pad_in_codigo  cto_org_pad_in_codigo
        , cto.org_in_codigo      cto_org_in_codigo
        , cto.org_tau_st_codigo  cto_org_tau_st_codigo
        , cto.cto_in_codigo
        , cto.cto_ch_tipo
        , cto.cto_re_juros
        , cto.cto_bo_taxaempr
        , cto.cto_re_mora
        , cto.fil_in_codigo
        , cto.cto_ch_tipomulta
        , DECODE( cto.cto_ch_tipo, 'P', 'Permuta'
                                  , 'V', 'Venda'
                                  , 'M', 'Aluguel'
                                      , cto.cto_ch_tipo) cto_ds_tipo
        , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato( cto.org_tab_in_codigo
                                                        , cto.org_pad_in_codigo
                                                        , cto.org_in_codigo
                                                        , cto.org_tau_st_codigo
                                                        , cto.cto_in_codigo
                                                        , 'D' -- Valor Original do Cto + Termos do tipo Altera o Valor do Contrato
                                                        , v_dt_base
                                                        , ''
                                                        , ''), 0) cto_re_valorcontrato
        , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato( cto.org_tab_in_codigo
                                                        , cto.org_pad_in_codigo
                                                        , cto.org_in_codigo
                                                        , cto.org_tau_st_codigo
                                                        , cto.cto_in_codigo
                                                        , 'O' -- Valor Original do Cto
                                                        , v_dt_base
                                                        , ''
                                                        , ''), 0) cto_re_vlroricontrato
        , cto.cto_re_totalresiduo
        , cto.cto_bo_descprorata
        , cto.cto_dt_cadastro
        , cto.cto_dt_assinatura
        , cto.cto_in_carenciaatraso
        , mgrel.pck_rel_fnc.fnc_car_status_cto_data ( cto.org_tab_in_codigo
                                                    , cto.org_pad_in_codigo
                                                    , cto.org_in_codigo
                                                    , cto.org_tau_st_codigo
                                                    , cto.cto_in_codigo
                                                    , v_dt_base
                                                    , 'N') cto_ch_status
        , TRIM(cto.cto_st_observacao)                      cto_st_observacao
        , cto.cto_re_taxabonif_sac
        , cto.cto_re_taxabonif_tp
        , cto.cto_re_taxaant_sac
        , cto.cto_re_taxaant_tp
        , cto.cto_re_taxaant
        , cto.cto_bo_congjuro
        , TRIM(cnd.cnd_st_observacao)                      cnd_st_observacao
        , cli.agn_tab_in_codigo
        , cli.agn_pad_in_codigo
        , aid.agn_tau_st_codigo
        , DECODE(cto.cto_bo_cessaodir, 'N', aid.agn_in_codigo
                                          , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
                                                                                            , cto.org_pad_in_codigo
                                                                                            , cto.org_in_codigo
                                                                                            , cto.org_tau_st_codigo
                                                                                            , cto.cto_in_codigo
                                                                                            , v_data_base
                                                                                            , 'C'))) agn_in_codigo --PINC-3803
        , agn.agn_st_fantasia
        , DECODE(cto.cto_bo_cessaodir, 'N', agn.agn_st_nome
                                          , mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
                                                                                  , cto.org_pad_in_codigo
                                                                                  , cto.org_in_codigo
                                                                                  , cto.org_tau_st_codigo
                                                                                  , cto.cto_in_codigo
                                                                                  , v_data_base
                                                                                  , 'N')) agn_st_nome --PINC-3803
        , agn.agn_ch_tipopessoafj
        , agn.agn_st_email
        , CAST( NVL( mgrel.pck_rel_glo_fnc.fnc_glo_concatena_telagente( agn.agn_tab_in_codigo
                                                                      , agn.agn_pad_in_codigo
                                                                      , agn.agn_in_codigo
                                                                      , ', '
                                                                      , 'S'
                                                                      , NULL
                                                                      , 3), '-') AS VARCHAR(255)) agn_st_telefone
        -- Endereço do cliente
        , NVL( agn.tpl_st_sigla, '-')       tpl_st_sigla
        , NVL( agn.agn_st_logradouro, '-')  agn_st_logradouro
        , NVL( agn.agn_st_numero, 0)        agn_st_numero
        , NVL( agn.agn_st_complemento, '-') agn_st_complemento
        , NVL( agn.agn_st_bairro, '-')      agn_st_bairro
        , NVL( agn.agn_st_municipio, '-')   agn_st_municipio
        , NVL( agn.uf_st_sigla, '-')        uf_st_sigla
        , NVL( agn.agn_st_cep, '-')         agn_st_cep
        -- Dados do projeto
        , pro.pro_st_extenso
        , pro.pro_st_apelido
        , pro.pro_st_descricao
        , pro.pro_tab_in_codigo
        , pro.pro_pad_in_codigo
        , pro.pro_ide_st_codigo
        , pro.pro_in_reduzido
        -- Dados do centro de custo
        , cus.cus_st_extenso
        , cus.cus_st_apelido
        , cus.cus_st_descricao
        , cus.cus_tab_in_codigo
        , cus.cus_pad_in_codigo
        , cus.cus_ide_st_codigo
        , cus.cus_in_reduzido
        , cto.cto_ch_reajusteanual

    FROM mgcon.con_centro_custo       cus
      , mgglo.glo_projetos           pro
      , mgglo.glo_agentes            agn
      , mgglo.glo_agentes_id         aid
      , mgcar.car_contrato_cliente   cli
      , mgdbm.dbm_condicao           cnd
      , mgrel.vw_car_estrutura       est
      , mgcar.car_contrato           cto

    WHERE cto.org_tab_in_codigo = v_tab
      AND cto.org_pad_in_codigo = v_pad
      AND cto.org_in_codigo     = v_cod
      AND cto.org_tau_st_codigo = v_tau
      AND cto.fil_in_codigo     = v_fil

      AND NVL( est.emp_in_codigo, 0) = DECODE( nvl(v_emp, 0)  , 0, NVL( est.emp_in_codigo, 0), v_emp)
      AND NVL( est.blo_in_codigo, 0) = DECODE( nvl(v_blc, 0)  , 0, NVL( est.blo_in_codigo, 0), v_blc)
      AND cto.cto_in_codigo          = DECODE( nvl(v_cto, 0)  , 0, cto.cto_in_codigo, v_cto)
      AND agn.agn_in_codigo          = DECODE( NVL( v_agn_in_codigo, 0), 0, DECODE(cto.cto_bo_cessaodir, 'N', agn.agn_in_codigo
                                                                                                            , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
                                                                                                                                                            , cto.org_pad_in_codigo
                                                                                                                                                            , cto.org_in_codigo
                                                                                                                                                            , cto.org_tau_st_codigo
                                                                                                                                                            , cto.cto_in_codigo
                                                                                                                                                            , v_data_base
                                                                                                                                                            , 'C'))) -- PINC-3803
                                                                          , NVL( v_agn_in_codigo, 0))

      -- Evita que realiza a consulta para todos empreendimentos
      AND (( nvl(v_emp, 0) <> 0) OR ( nvl(v_blc, 0) <> 0) OR ( nvl(v_cto, 0) <> 0) OR ( nvl(v_agn_in_codigo, 0) <> 0))

      AND mgrel.pck_rel_fnc.fnc_car_status_cto_data( cto.org_tab_in_codigo
                                                  , cto.org_pad_in_codigo
                                                  , cto.org_in_codigo
                                                  , cto.org_tau_st_codigo
                                                  , cto.cto_in_codigo
                                                  , v_dt_base
                                                  , 'N') IN ( v_ativo, v_quit, v_inadim, v_distr, v_cessao, v_trans)

      AND cto.org_tab_in_codigo = est.cto_org_tab_in_codigo
      AND cto.org_pad_in_codigo = est.cto_org_pad_in_codigo
      AND cto.org_in_codigo     = est.cto_org_in_codigo
      AND cto.org_tau_st_codigo = est.cto_org_tau_st_codigo
      AND cto.cto_in_codigo     = est.cto_in_codigo

      AND cto.org_tab_in_codigo = cnd.org_tab_in_codigo
      AND cto.org_pad_in_codigo = cnd.org_pad_in_codigo
      AND cto.org_in_codigo     = cnd.org_in_codigo
      AND cto.org_tau_st_codigo = cnd.org_tau_st_codigo
      AND cto.cnd_in_codigo     = cnd.cnd_in_codigo

      AND cli.org_tab_in_codigo = cto.org_tab_in_codigo
      AND cli.org_pad_in_codigo = cto.org_pad_in_codigo
      AND cli.org_in_codigo     = cto.org_in_codigo
      AND cli.org_tau_st_codigo = cto.org_tau_st_codigo
      AND cli.cto_in_codigo     = cto.cto_in_codigo

      AND aid.agn_tab_in_codigo = cli.agn_tab_in_codigo
      AND aid.agn_pad_in_codigo = cli.agn_pad_in_codigo
      AND aid.agn_in_codigo     = DECODE(cto.cto_bo_cessaodir, 'N', cli.agn_in_codigo
                                                                  , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
                                                                                                                  , cto.org_pad_in_codigo
                                                                                                                  , cto.org_in_codigo
                                                                                                                  , cto.org_tau_st_codigo
                                                                                                                  , cto.cto_in_codigo
                                                                                                                  , v_data_base
                                                                                                                  , 'C'))) -- PINC-3803
      AND aid.agn_tau_st_codigo = cli.agn_tau_st_codigo

      AND agn.agn_tab_in_codigo = aid.agn_tab_in_codigo
      AND agn.agn_pad_in_codigo = aid.agn_pad_in_codigo
      AND agn.agn_in_codigo     = aid.agn_in_codigo

      AND pro.pro_tab_in_codigo = cto.pro_tab_in_codigo
      AND pro.pro_pad_in_codigo = cto.pro_pad_in_codigo
      AND pro.pro_ide_st_codigo = cto.pro_ide_st_codigo
      AND pro.pro_in_reduzido   = cto.pro_in_reduzido

      AND cus.cus_tab_in_codigo = cto.cus_tab_in_codigo
      AND cus.cus_pad_in_codigo = cto.cus_pad_in_codigo
      AND cus.cus_ide_st_codigo = cto.cus_ide_st_codigo
      AND cus.cus_in_reduzido   = cto.cus_in_reduzido;

BEGIN

  v_tab          := org_tab_in_codigo;
  v_pad          := org_pad_in_codigo;
  v_cod          := org_in_codigo;
  v_fil          := fil_in_codigo;
  v_tau          := org_tau_st_codigo;
  v_emp          := cod_est;
  v_cto          := cod_cto;
  v_blc          := cod_blc;
  v_dt_base      := to_date( v_data_base,'dd/mm/yyyy');
  v_termo        := v_st_termo;
  v_des          := v_tdes;
  v_des1         := v_tdes1;
  v_con          := v_tcon;
  v_con1         := v_tcon1;
  v_reajuste     := v_descongela;
  v_ctrl_cheq    := v_ctrl_cheque;
  v_descapit_tp  := v_descap_tp;
  v_descapit_sac := v_descap_sac;
  v_tp_par       := v_tipo_parcela;
  v_ativo        := v_ati;
  v_quit         := v_qui;
  v_inadim       := v_ina;
  v_distr        := v_dis;
  v_cessao       := v_ces;
  v_trans        := v_tra;
  v_cons_taxa    := p_cons_taxa;
  v_cons_bonif   := p_cons_bonif;
  v_vl_corrigido := p_vl_corrigido;
  v_consinv      := p_investidor;
  v_conf_divida  := p_cons_confdivida;
  v_agn_in_codigo:= p_agn_in_codigo;
  v_par_secur    := p_par_secur;
  vsConsJrsTPSAC := p_cons_jr_tp_sac;

  -- Só irei considerar o parâmetro para mostrar cheques caso o "Controla cheques" estiver selecionado. Ch 45174
  IF v_ctrl_cheq = 'S' THEN
    v_mostra_chq := v_mostra_cheque;
  ELSE
    v_mostra_chq := 'N';
  END IF;

  -- concatenação dos parâmetros de termos DESCONSIDERAR
  IF    ( v_des <> '0' AND v_des1 <> '0') THEN
    v_desct := '-'|| v_des  || '-' || v_des1 || '-';
  ELSIF ( v_des <> '0' AND v_des1  = '0') THEN
    v_desct := '-'|| v_des  || '-';
  ELSIF ( v_des =  '0' AND v_des1 <> '0') THEN
    v_desct := '-'|| v_des1 || '-';
  ELSE
    v_desct := '';
  END IF;
  -- concatenação dos parâmetros de termos CONSIDERAR
  IF    ( v_con <> '0' AND v_con1 <> '0') THEN
    v_const := '-'|| v_con  || '-' || v_con1 || '-';
  ELSIF ( v_con <> '0' AND v_con1  = '0') THEN
    v_const := '-'|| v_con  || '-';
  ELSIF ( v_con =  '0' AND v_con1 <> '0') THEN
    v_const := '-'|| v_con1 || '-';
  ELSE
    v_const := '';
  END IF;
  -- Busca o contrato se indicada a unidade
  IF cod_uni > 0 THEN
    BEGIN
      SELECT est.cto_in_codigo
      INTO v_cto
      FROM mgrel.vw_car_estrutura est
      WHERE est.und_in_codigo = cod_uni
        AND est.estrutura IN ( 'G', 'U')

        AND est.cto_org_tab_in_codigo = v_tab
        AND est.cto_org_pad_in_codigo = v_pad
        AND est.cto_org_in_codigo     = v_cod
        AND est.cto_org_tau_st_codigo = v_tau

        AND mgrel.pck_rel_fnc.fnc_car_status_cto_data( est.cto_org_tab_in_codigo
                                                     , est.cto_org_pad_in_codigo
                                                     , est.cto_org_in_codigo
                                                     , est.cto_org_tau_st_codigo
                                                     , est.cto_in_codigo
                                                     , v_dt_base
                                                     , 'N') IN ( 'A', 'U', 'Q');
    EXCEPTION WHEN OTHERS THEN
        v_cto := 0;
    END;
  END IF;

  /* Define se irá calcular o resíduo de cobrança até a data de baixa ou data de vencimento, para parcelas baixadas via banco.
     Parâmetro definido para todos os contratos. PINC-3677 */
  SELECT NVL(pco.pco_ch_reajprojetado, 'B')
  INTO vs_IndiceProjetado
  FROM mgdbm.dbm_parametro_contabilidade pco
  WHERE pco.org_tab_in_codigo = v_tab
    AND pco.org_pad_in_codigo = v_pad
    AND pco.org_in_codigo     = fil_in_codigo
    AND pco.org_tau_st_codigo = v_tau;

  DELETE FROM mgcustom.rel_dados_contrato_api;

  FOR cto IN cur_cto_emp
    LOOP
      -- Percentual da organização
      IF v_consinv = 'S' THEN
         BEGIN
           SELECT (( 100 - DECODE( pco.pco_bo_percbloco, 'S', DECODE( NVL( est.und_bo_consinvestidor, 'N'), 'N', NVL( inv.perc, 0)
                                                                                                               , NVL( pru.perc_und, 0))                                                            , 0)) / 100) perc_org
           INTO v_perc_org
           FROM
                ( SELECT inv.org_tab_in_codigo
                       , inv.org_pad_in_codigo
                       , inv.org_in_codigo
                       , inv.org_tau_st_codigo
                       , inv.est_in_codigo
                       , NVL( SUM( inv.inv_re_perccontabilidade), 100) perc_und
                  FROM mgdbm.dbm_investidor inv
                  WHERE inv.agn_in_codigo <> v_fil
                  GROUP BY inv.org_tab_in_codigo
                         , inv.org_pad_in_codigo
                         , inv.org_in_codigo
                         , inv.org_tau_st_codigo
                         , inv.est_in_codigo) pru
              , ( SELECT inv.org_tab_in_codigo
                       , inv.org_pad_in_codigo
                       , inv.org_in_codigo
                       , inv.org_tau_st_codigo
                       , inv.est_in_codigo
                       , NVL( SUM( inv.inv_re_perccontabilidade), 100) perc
                  FROM mgdbm.dbm_investidor inv
                  WHERE inv.agn_in_codigo <> v_fil
                  GROUP BY inv.org_tab_in_codigo
                         , inv.org_pad_in_codigo
                         , inv.org_in_codigo
                         , inv.org_tau_st_codigo
                         , inv.est_in_codigo)         inv
                  , mgdbm.dbm_parametro_contabilidade pco
                  , mgrel.vw_car_estrutura            est

                WHERE est.org_tab_in_codigo = v_tab
                  AND est.org_pad_in_codigo = v_pad
                  AND est.org_in_codigo     = v_cod
                  AND est.fil_in_codigo     = v_fil
                  AND est.cto_in_codigo     = cto.cto_in_codigo

                  AND pco.org_tab_in_codigo = est.org_tab_in_codigo
                  AND pco.org_pad_in_codigo = est.org_pad_in_codigo
                  AND pco.org_in_codigo     = est.fil_in_codigo
                  AND pco.org_tau_st_codigo = est.org_tau_st_codigo

                  AND inv.org_tab_in_codigo (+)= est.org_tab_in_codigo
                  AND inv.org_pad_in_codigo (+)= est.org_pad_in_codigo
                  AND inv.org_in_codigo     (+)= est.org_in_codigo
                  AND inv.org_tau_st_codigo (+)= est.org_tau_st_codigo
                  AND inv.est_in_codigo     (+)= est.blo_in_codigo

                  AND pru.org_tab_in_codigo (+)= est.org_tab_in_codigo
                  AND pru.org_pad_in_codigo (+)= est.org_pad_in_codigo
                  AND pru.org_in_codigo     (+)= est.org_in_codigo
                  AND pru.org_tau_st_codigo (+)= est.org_tau_st_codigo
                  AND pru.est_in_codigo     (+)= est.und_in_codigo;

               EXCEPTION WHEN OTHERS THEN
                 v_perc_org := 1;
         END;

      ELSE
          v_perc_org := 1;
      END IF;
      -- Busca a quantidade de parcelas pagas com cheque-pre
      BEGIN
        IF v_ctrl_cheq = 'S' THEN
        BEGIN
          SELECT COUNT(*)
              , NVL(( SUM ( ROUND( NVL( par.par_re_valorpago, 0), 4) )
                + SUM ( ROUND( NVL( DECODE( SIGN( res.rescob_dt_processo - v_dt_base), 1, 0, par.par_re_residuocobranca),0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valormulta, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valoratraso, 0), 4) )
                - SUM ( ROUND( NVL( par.par_re_valordesconto, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valorcorrecao_atr, 0), 4) )
                -- taxas adicionais
                + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                                , par.org_pad_in_codigo
                                                                                , par.org_in_codigo
                                                                                , par.org_tau_st_codigo
                                                                                , par.cto_in_codigo
                                                                                , par.par_in_codigo), 0), 4))),0)
          INTO v_cheque_pre
             , v_vlr_cheque_pre
          FROM mgcar.car_parcela par
             , mgcar.car_residuo_cobranca res
          WHERE (( par.par_ch_status    <> 'I' ) OR ( par.par_ch_status = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
            AND TRUNC( par.par_dt_realizacaobx) <= v_dt_base
            AND par.par_ch_receitabaixa = 'C'
            AND ( par.par_ch_status     IN ( 'P', 'D', '1', '2', 'U') OR ( par.par_ch_status = 'A' AND ( TRUNC( par.par_dt_deposito) > v_dt_base OR par.par_dt_deposito IS NULL))) -- Considera cheques pre vencidos apenas com status de aberto, devolvido, depositado ou em custódia, e NÃO CONSIDERA cheques conpensados.
            AND par.org_tab_in_codigo   = cto.cto_org_tab_in_codigo
            AND par.org_pad_in_codigo   = cto.cto_org_pad_in_codigo
            AND par.org_in_codigo       = cto.cto_org_in_codigo
            AND par.org_tau_st_codigo   = cto.cto_org_tau_st_codigo
            AND par.cto_in_codigo       = cto.cto_in_codigo

            AND res.org_tab_in_codigo (+)= par.org_tab_in_codigo
            AND res.org_pad_in_codigo (+)= par.org_pad_in_codigo
            AND res.org_in_codigo     (+)= par.org_in_codigo
            AND res.org_tau_st_codigo (+)= par.org_tau_st_codigo
            AND res.cto_in_codigo     (+)= par.cto_in_codigo
            AND res.par_in_codigo     (+)= par.par_in_codigo
            AND res.rescob_in_codigo  (+)= par.rescob_in_codigo;
        EXCEPTION WHEN OTHERS THEN
          v_cheque_pre := 0;
          v_vlr_cheque_pre := 0;
        END;
        ELSE
        BEGIN
          SELECT COUNT(*)
                , NVL(( SUM ( ROUND( NVL( par.par_re_valorpago, 0), 4) )
                + SUM ( ROUND( NVL( DECODE( SIGN( res.rescob_dt_processo - v_dt_base), 1, 0, par.par_re_residuocobranca),0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valormulta, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valoratraso, 0), 4) )
                - SUM ( ROUND( NVL( par.par_re_valordesconto, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valorcorrecao_atr, 0), 4) )
                -- taxas adicionais
                + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                                , par.org_pad_in_codigo
                                                                                , par.org_in_codigo
                                                                                , par.org_tau_st_codigo
                                                                                , par.cto_in_codigo
                                                                                , par.par_in_codigo), 0), 4))),0)
          INTO v_cheque_pre
             , v_vlr_cheque_pre
          FROM mgcar.car_parcela par
             , mgcar.car_residuo_cobranca res
          WHERE (( par.par_ch_status    <> 'I' ) OR ( par.par_ch_status = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
            AND TRUNC( par.par_dt_baixa)         > v_dt_base -- Apenas considerará cheques pré futuros, os pré vencidos considera como pagos e não contabiliza no contador
            AND TRUNC( par.par_dt_realizacaobx) <= v_dt_base
            AND par.par_ch_receitabaixa = 'C'
            AND par.org_tab_in_codigo   = cto.cto_org_tab_in_codigo
            AND par.org_pad_in_codigo   = cto.cto_org_pad_in_codigo
            AND par.org_in_codigo       = cto.cto_org_in_codigo
            AND par.org_tau_st_codigo   = cto.cto_org_tau_st_codigo
            AND par.cto_in_codigo       = cto.cto_in_codigo

            AND res.org_tab_in_codigo (+)= par.org_tab_in_codigo
            AND res.org_pad_in_codigo (+)= par.org_pad_in_codigo
            AND res.org_in_codigo     (+)= par.org_in_codigo
            AND res.org_tau_st_codigo (+)= par.org_tau_st_codigo
            AND res.cto_in_codigo     (+)= par.cto_in_codigo
            AND res.par_in_codigo     (+)= par.par_in_codigo
            AND res.rescob_in_codigo  (+)= par.rescob_in_codigo;
        EXCEPTION WHEN OTHERS THEN
          v_cheque_pre := 0;
          v_vlr_cheque_pre := 0;
        END;
        END IF;
      END;

      BEGIN
        -- Busca a primeira e ultima data de vencimento das parcelas do contrato
        SELECT MIN( par.par_dt_vencimento)
             , MAX( par.par_dt_vencimento)
        INTO v_dt_ini
           , v_dt_fim
        FROM mgcar.car_parcela par
        WHERE par.org_tab_in_codigo = cto.cto_org_tab_in_codigo
          AND par.org_pad_in_codigo = cto.cto_org_pad_in_codigo
          AND par.org_in_codigo     = cto.cto_org_in_codigo
          AND par.org_tau_st_codigo = cto.cto_org_tau_st_codigo
          AND par.cto_in_codigo     = cto.cto_in_codigo
          AND  (( par.par_ch_status <> 'I') OR ( par.par_ch_status  = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
          AND TRUNC( par.par_dt_geracao) <= v_dt_base;
      EXCEPTION WHEN OTHERS THEN
        v_dt_ini := TO_DATE( NULL, 'dd/mm/yyy');
        v_dt_fim := TO_DATE( NULL, 'dd/mm/yyy');
      END;

      BEGIN
      -- Total a Pagar
        SELECT SUM ( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                           , par.org_pad_in_codigo
                                                           , par.org_in_codigo
                                                           , par.org_tau_st_codigo
                                                           , par.cto_in_codigo
                                                           , par.par_in_codigo
                                                           , v_dt_base
                                                           , v_tipoindice  --'RP'
                                                           , 'A'
                                                           , -1
                                                           , 'S'), 0) )
                    + SUM ( NVL( DECODE( SIGN( par.par_dt_vencimento - v_dt_base), -1, ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos( par.org_tab_in_codigo
                                                                                                                                              , par.org_pad_in_codigo
                                                                                                                                              , par.org_in_codigo
                                                                                                                                              , par.org_tau_st_codigo
                                                                                                                                              , par.cto_in_codigo
                                                                                                                                              , par.par_in_codigo
                                                                                                                                              , v_dt_base
                                                                                                                                              , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                                                                                                                                                             , par.org_pad_in_codigo
                                                                                                                                                                                             , par.org_in_codigo
                                                                                                                                                                                             , par.org_tau_st_codigo
                                                                                                                                                                                             , par.cto_in_codigo
                                                                                                                                                                                             , par.par_in_codigo
                                                                                                                                                                                             , v_dt_base
                                                                                                                                                                                             , v_tipoindice
                                                                                                                                                                                             , v_reajuste
                                                                                                                                                                                             , -1
                                                                                                                                                                                             , 'S'), 0),2)
                                                                                                                                              , 'AM'), 0),2) -- Calcula mora e multa, passando  no parametro o valor 'AM'
                                                                                       + NVL( DECODE( SIGN( res.rescob_dt_processo - v_dt_base), 1, 0, par.par_re_residuocobranca),0)), 0), 0))
                     -- Alteração para adicionar o valor das taxas das parcelas
                    + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                                    , par.org_pad_in_codigo
                                                                                    , par.org_in_codigo
                                                                                    , par.org_tau_st_codigo
                                                                                    , par.cto_in_codigo
                                                                                    , par.par_in_codigo), 0), 2) )
                     -- Considero também o valor de resíduo corrigido. PINC-4292
                    + SUM ( ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduoCorrigido( par.org_tab_in_codigo
                                                                                    , par.org_pad_in_codigo
                                                                                    , par.org_in_codigo
                                                                                    , par.org_tau_st_codigo
                                                                                    , par.cto_in_codigo
                                                                                    , par.par_in_codigo
                                                                                    , v_dt_base), 0), 2) )

         INTO v_vlr_apagar
         FROM mgcar.car_caucao_parcela   cau
            , mgcar.car_residuo_cobranca res
            , mgcar.car_contrato_termo   ctt
            , mgcar.car_parcela          par
         WHERE --PAR
               (( par.par_ch_status <> 'I') OR ( par.par_ch_status  = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
           AND (( par.par_dt_baixa IS NULL) OR ( TRUNC( par.par_dt_baixa) > v_dt_base))
           AND TRUNC( par.par_dt_geracao) <= v_dt_base

           AND (( v_conf_divida  = 'N' AND NVL( par.par_bo_confdivida, 'N') = 'N')
             OR ( v_conf_divida  = 'N' AND ( par.par_bo_confdivida = 'S' AND TRUNC( par.par_dt_baixa) > v_dt_base))
             OR ( v_conf_divida  = 'S'))

           AND ((( p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
             OR (( p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
              OR ( p_parc_caucao  = 'T'))

           --Filtro por tipo de parcela
           AND (( v_termo LIKE '%C%' AND (( par.par_bo_contratual = 'S' AND ctt.tte_in_codigo IS NULL)))
             OR ( v_termo LIKE '%F%' AND (( par.par_bo_contratual = 'N' AND ctt.tte_in_codigo IS NULL)))
             OR ( v_termo LIKE '%T%' AND ctt.tte_in_codigo IS NOT NULL
                 AND ( v_const IS NULL OR v_const LIKE '%-' || ctt.tte_in_codigo || '-%')
                 AND ( v_desct IS NULL OR v_desct NOT LIKE '%-' || ctt.tte_in_codigo || '-%')
                 AND (( v_termo LIKE '%E%' AND par.par_bo_contratual = 'S')
                   OR ( v_termo LIKE '%N%' AND par.par_bo_contratual = 'N'))))

           AND ((( v_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
             OR (( v_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
              OR ( v_par_secur = 'T'))

           AND par.org_tab_in_codigo = cto.cto_org_tab_in_codigo
           AND par.org_pad_in_codigo = cto.cto_org_pad_in_codigo
           AND par.org_in_codigo     = cto.cto_org_in_codigo
           AND par.org_tau_st_codigo = cto.cto_org_tau_st_codigo
           AND par.cto_in_codigo     = cto.cto_in_codigo

           AND ctt.org_tab_in_codigo (+)= par.org_tab_in_codigo
           AND ctt.org_pad_in_codigo (+)= par.org_pad_in_codigo
           AND ctt.org_in_codigo     (+)= par.org_in_codigo
           AND ctt.org_tau_st_codigo (+)= par.org_tau_st_codigo
           AND ctt.cto_in_codigo     (+)= par.cto_in_codigo
           AND ctt.ctt_in_codigo     (+)= par.ctt_in_codigo

           AND res.org_tab_in_codigo (+)= par.org_tab_in_codigo
           AND res.org_pad_in_codigo (+)= par.org_pad_in_codigo
           AND res.org_in_codigo     (+)= par.org_in_codigo
           AND res.org_tau_st_codigo (+)= par.org_tau_st_codigo
           AND res.cto_in_codigo     (+)= par.cto_in_codigo
           AND res.par_in_codigo     (+)= par.par_in_codigo
           AND res.rescob_in_codigo  (+)= par.rescob_in_codigo

           AND cau.org_tab_in_codigo (+)= par.org_tab_in_codigo
           AND cau.org_pad_in_codigo (+)= par.org_pad_in_codigo
           AND cau.org_in_codigo     (+)= par.org_in_codigo
           AND cau.org_tau_st_codigo (+)= par.org_tau_st_codigo
           AND cau.cto_in_codigo     (+)= par.cto_in_codigo
           AND cau.par_in_codigo     (+)= par.par_in_codigo;
      EXCEPTION WHEN OTHERS THEN
        v_vlr_apagar := 0;
      END;

      BEGIN
      -- Total Pago
         SELECT ( SUM ( ROUND( NVL( par.par_re_valorpago, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_residuocobranca, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valormulta, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valoratraso, 0), 4) )
                - SUM ( ROUND( NVL( par.par_re_valordesconto, 0), 4) )
                + SUM ( ROUND( NVL( par.par_re_valorcorrecao_atr, 0), 4) )
                -- taxas adicionais
                + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                                , par.org_pad_in_codigo
                                                                                , par.org_in_codigo
                                                                                , par.org_tau_st_codigo
                                                                                , par.cto_in_codigo
                                                                                , par.par_in_codigo), 0), 4)) )
         INTO v_vlr_pago
         FROM mgcar.car_caucao_parcela      cau
            , mgcar.car_carta_credito_baixa ccb -- tabela para retorno das parcelas que foram pagas com carta de crédito resultante de pagamento a maior
            , mgcar.car_parcela             prr
            , mgcar.car_residuo_cobranca    res
            , mgcar.car_contrato_termo      ctt -- Tabela para relacionamento do filtro de termos
            , mgcar.car_parcela             par
         WHERE --PAR
              par.par_ch_status        <> 'I'
          AND TRUNC( par.par_dt_baixa) <= v_dt_base

          AND (( v_conf_divida  = 'N' AND NVL( par.par_bo_confdivida, 'N') = 'N')
            OR ( v_conf_divida  = 'N' AND ( par.par_bo_confdivida = 'S' AND TRUNC( par.par_dt_baixa) > v_dt_base))
            OR ( v_conf_divida  = 'S'))

          --Filtro por tipo de parcela
           AND (( v_termo LIKE '%C%' AND (( par.par_bo_contratual = 'S' AND ctt.tte_in_codigo IS NULL)))
            OR  ( v_termo LIKE '%F%' AND (( par.par_bo_contratual = 'N' AND ctt.tte_in_codigo IS NULL)))
            OR  ( v_termo LIKE '%T%' AND ctt.tte_in_codigo IS NOT NULL
                 AND ( v_const IS NULL OR v_const LIKE '%-' || ctt.tte_in_codigo || '-%')
                 AND ( v_desct IS NULL OR v_desct NOT LIKE '%-' || ctt.tte_in_codigo || '-%')
                 AND (( v_termo LIKE '%E%' AND par.par_bo_contratual = 'S')
                   OR ( v_termo LIKE '%N%' AND par.par_bo_contratual = 'N'))))

          AND ((( p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
            OR (( p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
             OR ( p_parc_caucao  = 'T'))

          AND ((( v_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
            OR (( v_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
             OR ( v_par_secur = 'T'))

          AND par.org_tab_in_codigo = cto.cto_org_tab_in_codigo
          AND par.org_pad_in_codigo = cto.cto_org_pad_in_codigo
          AND par.org_in_codigo     = cto.cto_org_in_codigo
          AND par.org_tau_st_codigo = cto.cto_org_tau_st_codigo
          AND par.cto_in_codigo     = cto.cto_in_codigo

          AND ctt.org_tab_in_codigo(+)= par.org_tab_in_codigo
          AND ctt.org_pad_in_codigo(+)= par.org_pad_in_codigo
          AND ctt.org_in_codigo    (+)= par.org_in_codigo
          AND ctt.org_tau_st_codigo(+)= par.org_tau_st_codigo
          AND ctt.cto_in_codigo    (+)= par.cto_in_codigo
          AND ctt.ctt_in_codigo    (+)= par.ctt_in_codigo

          AND res.org_tab_in_codigo(+)= par.org_tab_in_codigo
          AND res.org_pad_in_codigo(+)= par.org_pad_in_codigo
          AND res.org_in_codigo    (+)= par.org_in_codigo
          AND res.org_tau_st_codigo(+)= par.org_tau_st_codigo
          AND res.cto_in_codigo    (+)= par.cto_in_codigo
          AND res.par_in_codigo    (+)= par.par_in_codigo

          AND prr.org_tab_in_codigo(+)= res.org_tab_in_codigo
          AND prr.org_pad_in_codigo(+)= res.org_pad_in_codigo
          AND prr.org_in_codigo    (+)= res.org_in_codigo
          AND prr.org_tau_st_codigo(+)= res.org_tau_st_codigo
          AND prr.cto_in_codigo    (+)= res.cto_in_codigo
          AND prr.rescob_in_codigo (+)= res.rescob_in_codigo

          -- join para vincular baixa de parcelas com carta de crédito resultante de pagamento a maior.
          AND ccb.org_tab_in_codigo(+)= par.org_tab_in_codigo
          AND ccb.org_pad_in_codigo(+)= par.org_pad_in_codigo
          AND ccb.org_in_codigo    (+)= par.org_in_codigo
          AND ccb.org_tau_st_codigo(+)= par.org_tau_st_codigo
          AND ccb.cto_in_codigo    (+)= par.cto_in_codigo
          AND ccb.par_in_codigo    (+)= par.par_in_codigo

          AND cau.org_tab_in_codigo (+)= par.org_tab_in_codigo
          AND cau.org_pad_in_codigo (+)= par.org_pad_in_codigo
          AND cau.org_in_codigo     (+)= par.org_in_codigo
          AND cau.org_tau_st_codigo (+)= par.org_tau_st_codigo
          AND cau.cto_in_codigo     (+)= par.cto_in_codigo
          AND cau.par_in_codigo     (+)= par.par_in_codigo;

      EXCEPTION WHEN OTHERS THEN
        v_vlr_pago := 0;
      END;

      mgcar.pck_car_contabil.prc_car_calcula_abatidotpcto(cto.cto_org_tab_in_codigo,
                                                          cto.cto_org_pad_in_codigo,
                                                          cto.cto_org_in_codigo,
                                                          cto.cto_org_tau_st_codigo,
                                                          cto.cto_in_codigo);


      INSERT INTO mgcustom.rel_dados_contrato_api
                ( emp_in_codigo                                       --1
                , emp_st_codigo                                       --2
                , emp_st_nome                                         --3
                , etp_in_codigo                                       --4
                , etp_st_codigo                                       --5
                , etp_st_nome                                         --6
                , blo_in_codigo                                       --7
                , blo_st_codigo                                       --8
                , blo_st_nome                                         --9
                , und_in_codigo                                       --10
                , und_st_codigo                                       --11
                , und_st_nome                                         --12
                , und_re_peso                -- und_re_areaprivativa  --13
                --dados do contrato                                   --
                , org_tab_in_codigo                                   --14
                , org_pad_in_codigo                                   --15
                , org_in_codigo                                       --16
                , org_tau_st_codigo                                   --17
                , cto_in_codigo                                       --18
                , par_in_codigo_pg           --ctc_in_codigo          --19
                , cto_ch_tipo                                         --20
                , cto_ds_tipo                                         --21
                , cto_re_valorcontrato                                --22
                , cto_re_totalresiduo                                 --23
                , pro_ch_anasin              --cto_bo_descprorata     --24
                , cto_dt_cadastro                                     --25
                , ind_dt_vigencia            -- cto_dt_assinatura     --26
                , cto_ch_status                                       --27
                , cto_st_observacao                                   --28
                , oco_st_ocorrencia          -- cnd_st_observacao     --29
                , blo_re_valorm2             --cto_re_taxabonif_sac   --30
                , blo_re_custocom            --cto_re_taxabonif_tp    --31
                , par_re_valor_pagocorrigido --cto_re_taxaant_sac     --32
                , ctc_in_codigo              --cto_re_taxaant_tp      --33
                -- Agente                                             --
                , agn_in_codigo                                       --34
                , agn_tab_in_codigo                                   --35
                , agn_pad_in_codigo                                   --36
                , agn_tau_st_codigo                                   --37
                , agn_st_fantasia                                     --38
                , agn_st_nome                                         --39
                , agn_ch_tipopessoafj                                 --40
                , oco_st_complemento        -- agn.agn_st_emai        --41
                , cto_ds_origem             -- cto.agn_st_telefone    --42
                -- Endereço do cliente                                --
                , tpl_st_sigla                                        --43
                , agn_st_logradouro                                   --44
                , agn_st_numero                                       --45
                , agn_st_complemento                                  --46
                , agn_st_bairro                                       --47
                , agn_st_municipio                                    --48
                , uf_st_sigla                                         --49
                , agn_st_cep                                          --50
                -- Dados do projeto                                   --
                , pro_st_extenso                                      --51
                , pro_st_apelido                                      --52
                , pro_st_descricao                                    --53
                , pro_tab_in_codigo                                   --54
                , pro_pad_in_codigo                                   --55
                , pro_ide_st_codigo                                   --56
                , pro_in_reduzido                                     --57
                -- Dados do centro de custo                           --
                , csf_st_descricao          -- cto.cus_st_extenso     --58
                , cto_ds_status             -- cto.cus_st_apelido     --59
                , par_st_receita            -- cto.cus_st_descricao   --60
                , cto_in_indice             -- cto.cus_tab_in_codigo  --61
                , ocr_in_codigo             -- cto.cus_pad_in_codigo  --62
                , ind_st_nome               -- cto.cus_ide_st_codigo  --63
                , est_org_tab_in_codigo     -- cto.cus.cus_in_reduzido--64
                -- Dados da parcela                                   --
                , par_in_codigo                                       --65
                , par_ch_parcela                                      --66
                , par_ch_status_bx          -- par_ch_receita         --67
                , par_in_codigo_vcd         -- cndit_in_intervalo     --68
                , par_dt_vencimento                                   --69
                , par_re_valororiginal                                --70
                , par_re_valorcorrigido_vcd                           --71
                , par_re_valorantecipacao   --corrigido_desc          --72
                , par_ch_origem                                       --73
                , par_ds_origem                                       --74
                , par_st_observacao                                   --75
                , par_dt_baixa                                        --76
                , cndit_in_codigo          -- par_re_numerodias_atraso--77
                -- Calcula encargos                                   --
                , par_re_valorencargo                                 --78
                , par_re_valordesconto                                --79
                , par_re_residuocobranca                              --80
                -- Taxas adicionais                                   --
                , par_re_valorquitacao_tpvc -- par_re_valortaxas      --81
                -- Calcula o total pago na data de baixa              --
                , par_re_valorpago                                    --82
                --indice                                              --
                , par_ch_status             -- par_ch_reajuste                                      --83
                , par_ds_status             -- par_st_siglaindice                                   --84
                , parcor_in_codigo          -- par_st_defasagem                                     --85
                , cto_dt_entrega            -- par_dt_vigenciaindice                                --86
                , par_re_valor_atual        -- par_re_valoratualizado                               --87
                , par_re_valor_residuo      -- par_re_valorresiduo                                  --88
                , par_re_valorresiduo_cc    -- par_re_valorresiduocorrigido                         --89
                , ant_in_codigo             -- par_in_residuo                                       --90
                , ocs_st_descricao          -- parcor_in_codigo          -- cobpar_in_codigo        --91
                , par_bo_contratual         -- rescob_bo_cobrado                                    --92
                , par_re_valoramortizado    -- rescob_re_valoracobrar                               --93
                , par_re_valor_totalpago    -- par_re_indicacheque_pre                              --94
                , par_re_percentual         -- par_re_valorcheque_pre  Ch. 45174                    --95
                , cto_bo_taxaempr           -- par_bo_confdivida                                    --96
                , par_re_valorjurosbx                                                               --97
                , par_re_valorcorrecaobx                                                            --98
                , par_re_valor_depchave     -- par_re_valorjurosren                                 --99
                , par_re_valormulta                                                                 --100
                , par_re_valoratraso                                                                --101
                , par_re_valorcorrecao_atr                                                          --102
                , ctt_ds_tipo               -- par_st_indica                                        --103
                , parcor_in_indice          -- rescob_in_codigo                                     --104
                , par_re_valorcorrigido_avc -- vlr_corrigido_total                                  --105
                , par_re_credito            -- valor_quitacao                                       --106
                , par_re_jrotpncobrado      -- vl_bonificacao                                       --107
                , par_re_valor_antchave     -- vl_quit_tot                                          --108
                , ctt_dt_emissao            -- data_ult_reaj_anual                                  --109
                , cto_dt_importacao         -- data_primeira_parc                                   --110
                , ent_dt_entrega            -- data_ultima_parc                                     --111
                , ctt_re_valor              -- valor_baixa_ccred                                    --112
                , par_re_valorfinanciado    -- sld_ccred_corrigido                                  --113
                , par_re_saldodevedor       -- v_vlr_apagar                                         --114
                , par_re_saldoquitacao      -- vlr pago                                             --115
                , cto_re_mora               -- perc_org                                             --116
                , par_re_valorquitacao_tpnv -- cto.cto_re_vlroricontrato                            --117
                )
           -- Parcelas pagas
           SELECT cto.emp_in_codigo                     --1
                , cto.emp_st_codigo                     --2
                , cto.emp_st_nome                       --3
                , cto.etp_in_codigo                     --4
                , cto.etp_st_codigo                     --5
                , cto.etp_st_nome                       --6
                , cto.blo_in_codigo                     --7
                , cto.blo_st_codigo                     --8
                , cto.blo_st_nome                       --9
                , cto.und_in_codigo                     --10
                , cto.und_st_codigo                     --11
                , cto.und_st_nome                       --12
                , cto.und_re_areaprivativa              --13
                --Dados do contrato                     --
                , cto.cto_org_tab_in_codigo             --14
                , cto.cto_org_pad_in_codigo             --15
                , cto.cto_org_in_codigo                 --16
                , cto.cto_org_tau_st_codigo             --17
                , cto.cto_in_codigo                     --18
                , cau.ctc_in_codigo                     --19
                , cto.cto_ch_tipo                       --20
                , cto.cto_ds_tipo                       --21
                , cto.cto_re_valorcontrato * NVL( v_perc_org, 1) cto_re_valorcontrato --22
                , cto.cto_re_totalresiduo               --23
                , cto.cto_bo_descprorata                --24
                , cto.cto_dt_cadastro                   --25
                , cto.cto_dt_assinatura                 --26
                , cto.cto_ch_status                     --27
                , cto.cto_st_observacao                 --28
                , cto.cnd_st_observacao                 --29
                , cto.cto_re_taxabonif_sac              --30
                , cto.cto_re_taxabonif_tp               --31
                , cto.cto_re_taxaant_sac                --32
                , cto.cto_re_taxaant_tp                 --33
                -- Agente                               --
                , cto.agn_in_codigo                     --34
                , cto.agn_tab_in_codigo                 --35
                , cto.agn_pad_in_codigo                 --36
                , cto.agn_tau_st_codigo                 --37
                , cto.agn_st_fantasia                   --38
                , cto.agn_st_nome                       --39
                , cto.agn_ch_tipopessoafj               --40
                , cto.agn_st_email                      --41
                , cto.agn_st_telefone                   --42
                -- Endereço do cliente                  --
                , cto.tpl_st_sigla                      --43
                , cto.agn_st_logradouro                 --44
                , cto.agn_st_numero                     --45
                , cto.agn_st_complemento                --46
                , cto.agn_st_bairro                     --47
                , cto.agn_st_municipio                  --48
                , cto.uf_st_sigla                       --49
                , cto.agn_st_cep                        --50
                -- Dados do projeto                     --
                , cto.pro_st_extenso                    --51
                , cto.pro_st_apelido                    --52
                , cto.pro_st_descricao                  --53
                , cto.pro_tab_in_codigo                 --54
                , cto.pro_pad_in_codigo                 --55
                , cto.pro_ide_st_codigo                 --56
                , cto.pro_in_reduzido                   --57
                -- Dados do centro de custo             --
                , cto.cus_st_extenso                    --58
                , cto.cus_st_apelido                    --59
                , cto.cus_st_descricao                  --60
                , cto.cus_tab_in_codigo                 --61
                , cto.cus_pad_in_codigo                 --62
                , cto.cus_ide_st_codigo                 --63
                , cto.cus_in_reduzido                   --64
                -- Dados da parcela                     --
                , par.par_in_codigo                     --65
                , par.par_ch_parcela                    --66
                , par.par_ch_receita                    --67
                , cit.cndit_in_intervalo                --68
                , par.par_dt_vencimento                 --69
                , ROUND( DECODE( vsConsJrsTPSAC, 'S', DECODE( par.pai_par_ch_origem , 'T', tpb.tpr_re_valororiginal
                                                                                    , 'S', par.par_re_vlroriginalsac
                                                                                         , NVL( par.par_re_valororiginal, 0))
                                                    , NVL( par.par_re_valororiginal, 0)), 2) * NVL( v_perc_org, 1)  par_re_valororiginal   --70
                -- Calcula o valor da parcela corrigido até a data de baixa
                , ( ROUND( NVL( par.par_re_valororiginal, 0), 4)
                  + ROUND( NVL( par.par_re_valorjuros, 0), 4)
                  + ROUND( NVL( par.par_re_valorjurosbx, 0), 4)
                  + ROUND( NVL( par.par_re_valorcorrecao, 0), 4)
                  + ROUND( NVL( par.par_re_valorcorrecaobx, 0), 4)
                  + ROUND( NVL( par.par_re_valorjurosren, 0), 4)) * NVL( v_perc_org, 1)    par_re_valorcorrigido   --71

                , ( ROUND( NVL( par.par_re_valororiginal, 0), 4)
                  + ROUND( NVL( par.par_re_valorjuros, 0), 4)
                  + ROUND( NVL( par.par_re_valorjurosbx, 0), 4)
                  + ROUND( NVL( par.par_re_valorcorrecao, 0), 4)
                  + ROUND( NVL( par.par_re_valorcorrecaobx, 0), 4)
                  + ROUND( NVL( par.par_re_valorjurosren, 0), 4)) par_re_valorcorrigido_desc --72

                , par.par_ch_origem                                                           --73
                -- Retorna descrição da origem da parcela
                , mgcar.pck_car_fnc.fnc_car_origemparcela( par.org_tab_in_codigo
                                                         , par.org_pad_in_codigo
                                                         , par.org_in_codigo
                                                         , par.org_tau_st_codigo
                                                         , par.cto_in_codigo
                                                         , par.par_in_codigo
                                                         , par.par_ch_origem
                                                         , par.cnd_in_codigo
                                                         , par.par_ch_amortizacao
                                                         , 1) par_st_origem                  --74
                , SUBSTR( obs.par_st_observacao, 1, 7) par_st_sequencia                      --75
                , par.par_dt_baixa                                                           --76
                , DECODE( SIGN( par.par_dt_baixa - par.par_dt_vencimento), 1, par.par_dt_baixa - par.par_dt_vencimento
                                                                         , 0) par_re_numerodias_atraso   --77
                -- Calcula os encargos
                , ROUND( NVL( par.par_re_valormulta, 0), 4) + ROUND( NVL( par.par_re_valoratraso, 0), 4) * NVL( v_perc_org, 1) par_re_valorencargos   --78
                , ROUND( NVL( par.par_re_valordesconto, 0), 4) * NVL( v_perc_org, 1)    par_re_valordesconto                  --79
                , ROUND( NVL( par.par_re_residuocobranca, 0), 4) * NVL( v_perc_org, 1)  par_re_residuocobranca                --80
                -- Taxas Adicionais
                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                          , par.org_pad_in_codigo
                                                                          , par.org_in_codigo
                                                                          , par.org_tau_st_codigo
                                                                          , par.cto_in_codigo
                                                                          , par.par_in_codigo), 0), 4) par_re_valortaxas          --81
                -- Calcula o total pago na data de baixa
                , ( ROUND( NVL( par.par_re_valorpago, 0), 4)
                  + ROUND( NVL( par.par_re_residuocobranca, 0), 4)
                  + ROUND( NVL( par.par_re_valormulta, 0), 4)
                  + ROUND( NVL( par.par_re_valoratraso, 0), 4)
                  - ROUND( NVL( par.par_re_valordesconto, 0), 4)
                  + ROUND( NVL( par.par_re_valorcorrecao_atr, 0), 4)
                  -- taxas adicionais
                  + ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                            , par.org_pad_in_codigo
                                                                            , par.org_in_codigo
                                                                            , par.org_tau_st_codigo
                                                                            , par.cto_in_codigo
                                                                            , par.par_in_codigo), 0), 4)) * NVL( v_perc_org, 1) par_re_valorpago --82
                , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo
                                                             , par.par_dt_baixa
                                                             , 'R') par_ch_reajuste                       --83
                , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo
                                                             , par.par_dt_baixa
                                                             , 'S') par_st_siglaindice                    --84
                , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo
                                                             , par.par_dt_baixa
                                                             , 'F') par_st_defasagem                      --85
                , TO_DATE( mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                                      , par.org_pad_in_codigo
                                                                      , par.org_in_codigo
                                                                      , par.org_tau_st_codigo
                                                                      , par.cto_in_codigo
                                                                      , par.par_in_codigo
                                                                      , par.par_dt_baixa
                                                                      , 'D'), 'DD/MM/YYYY') par_dt_vigenciaindice        --86
                -- Corrige valor parcela até data de baixa
                , NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                        , par.org_pad_in_codigo
                                                        , par.org_in_codigo
                                                        , par.org_tau_st_codigo
                                                        , par.cto_in_codigo
                                                        , par.par_in_codigo
                                                        , par.par_dt_baixa
                                                        , v_tipoindice  --'RP'
                                                        , 'A'
                                                        , -1
                                                        , 'S'), 0) * NVL( v_perc_org, 1)    par_re_valoratualizado               --87
                -- Calculado o valor do resíduo a gerar sem correcao
                , ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduo( par.org_tab_in_codigo
                                                                           , par.org_pad_in_codigo
                                                                           , par.org_in_codigo
                                                                           , par.org_tau_st_codigo
                                                                           , par.cto_in_codigo
                                                                           , par.par_in_codigo
                                                                           , v_dt_base), 0), 2) * NVL( v_perc_org, 1) par_re_valorresiduo    --88
                -- Calculado o valor do resíduo a gerar corrigido
                , ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduoCorrigido( par.org_tab_in_codigo
                                                                                    , par.org_pad_in_codigo
                                                                                    , par.org_in_codigo
                                                                                    , par.org_tau_st_codigo
                                                                                    , par.cto_in_codigo
                                                                                    , par.par_in_codigo
                                                                                    , v_dt_base), 0), 2) * NVL( v_perc_org, 1) par_re_valorresiduocorrigido   --89
                , TRIM( par.par_in_residuo) par_in_residuo                                                                 --90
                , DECODE( SIGN( mgrel.pck_rel_fnc.fnc_car_parcela_de_residuo( par.org_tab_in_codigo
                                                                            , par.org_pad_in_codigo
                                                                            , par.org_in_codigo
                                                                            , par.org_tau_st_codigo
                                                                            , par.cto_in_codigo
                                                                            , par.par_in_codigo
                                                                            , 'O') - v_dt_base), 1, '', DECODE( res.rescob_in_codigo, NULL, '', DECODE( prr.par_in_codigo, NULL, 'C.Créd.', to_char( prr.par_in_codigo)))) cobpar_in_codigo --91
                , res.rescob_bo_cobrado                                                                                     --92
                -- Se a parcela for igual a parcela retornada pela função, chamo a função para retornar o valor do resíduo a gerar
                , CASE
                    WHEN cto.cto_ch_status <> 'Q' THEN
                      DECODE(NVL(res.rescob_bo_cobrado, 'N'), 'N', (DECODE(NVL(res.rescob_re_valoracobrar, 0), 0, (DECODE(mgcar.pck_car_fnc.fnc_car_valorcorrecao(par.org_tab_in_codigo
                                                                                                                                                                , par.org_pad_in_codigo
                                                                                                                                                                , par.org_in_codigo
                                                                                                                                                                , par.org_tau_st_codigo
                                                                                                                                                                , par.cto_in_codigo
                                                                                                                                                                , par.par_in_codigo
                                                                                                                                                                , decode(par.par_ch_receitabaixa, 'B', DECODE(vs_IndiceProjetado, 'B', par.par_dt_baixa, par.par_dt_vencimento), par.par_dt_baixa)
                                                                                                                                                                , 'RP'
                                                                                                                                                                , 'D'
                                                                                                                                                                , -1) - res.rescob_re_correcaocobrada, 0, 0
                                                                                                                                                                                                        , (DECODE(par.par_in_codigo, mgrel.pck_rel_fnc.fnc_car_saldo_rescobcontrato( cto.cto_org_tab_in_codigo
                                                                                                                                                                                                                                                                                   , cto.cto_org_pad_in_codigo
                                                                                                                                                                                                                                                                                   , cto.cto_org_in_codigo
                                                                                                                                                                                                                                                                                   , cto.cto_org_tau_st_codigo
                                                                                                                                                                                                                                                                                   , cto.cto_in_codigo
                                                                                                                                                                                                                                                                                   , par.par_in_codigo
                                                                                                                                                                                                                                                                                   , DECODE( SIGN( v_dt_base - res.rescob_dt_processo), 1, res.rescob_dt_processo - 1
                                                                                                                                                                                                                                                                                                                                      , 0, res.rescob_dt_processo - 1
                                                                                                                                                                                                                                                                                                                                         , v_dt_base)
                                                                                                                                                                                                                                                                                   , 'P'), mgrel.pck_rel_fnc.fnc_car_saldo_rescobcontrato( cto.cto_org_tab_in_codigo
                                                                                                                                                                                                                                                                                                                                         , cto.cto_org_pad_in_codigo
                                                                                                                                                                                                                                                                                                                                         , cto.cto_org_in_codigo
                                                                                                                                                                                                                                                                                                                                         , cto.cto_org_tau_st_codigo
                                                                                                                                                                                                                                                                                                                                         , cto.cto_in_codigo
                                                                                                                                                                                                                                                                                                                                         , par.par_in_codigo
                                                                                                                                                                                                                                                                                                                                         , DECODE( SIGN( v_dt_base - res.rescob_dt_processo), 1, res.rescob_dt_processo-1
                                                                                                                                                                                                                                                                                                                                                                                            , 0, res.rescob_dt_processo-1
                                                                                                                                                                                                                                                                                                                                                                                               , v_dt_base)
                                                                                                                                                                                                                                                                                                                                         , 'V')
                                                                                                                                                                                                                                                                                         , 0)) * NVL(v_perc_org, 1)))
                                                                                                                , 0))
                                                                 , 0)
                    ELSE 0
                  END rescob_re_valoracobrar --93
                , v_cheque_pre           par_re_indicacheque_pre                                                              --94
                , v_vlr_cheque_pre       par_re_valorcheque_pre   -- Ch. 45174                                                --95
                , par.par_bo_confdivida                                                                                       --96

                -- Inserido o PAR_RE_VALORJUROS que esta faltando na somatória
                -- (chamado 3016-FFB) em 03/12
                , NVL( par.par_re_valorjuros, 0)
                + NVL( par.par_re_valorjurosbx, 0)
                + DECODE( vsConsJrsTPSAC, 'S', DECODE( par.pai_par_ch_origem, 'T', NVL( tpb.tpr_re_jurostp, 0)
                                                                            , 'S', par.par_re_valororiginal - par.par_re_vlroriginalsac
                                                                                 , 0)
                                             , 0) par_re_valorjurosbx                                                         --97
                -- Inserido o PAR_RE_VALORCORRECAO que esta faltando na somatória
                -- (chamado 2156-Plaenge) em 05/09
                , NVL( par.par_re_valorcorrecao, 0) + NVL( par.par_re_valorcorrecaobx, 0) par_re_valorcorrecaobx              --98

                , NVL( par.par_re_valorjurosren, 0)     par_re_valorjurosren                                                  --99
                , NVL( par.par_re_valormulta   , 0)     par_re_valormulta                                                     --100
                , NVL( par.par_re_valoratraso  , 0)     par_re_valoratraso                                                    --101
                , NVL( par.par_re_valorcorrecao_atr, 0) par_re_valorcorrecao_atr                                              --102
                , 'BAIXADA'                             par_st_indica                                                         --103
                -- Dados da parcela de resíduo de cobrança. (verifica se a parcela já recebeu o resíduo na data base)
                , DECODE( SIGN( res.rescob_dt_processo - v_dt_base), 1, TO_NUMBER( NULL)
                                                                      , prr.par_in_codigo)  rescob_in_codigo                  --104

                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                              , par.org_pad_in_codigo
                                                              , par.org_in_codigo
                                                              , par.org_tau_st_codigo
                                                              , par.cto_in_codigo
                                                              , par.par_in_codigo
                                                              , v_dt_base
                                                              , 'RP'
                                                              , v_reajuste
                                                              , -1
                                                              , 'N'), 0), 2) vlr_corrigido_total --105
                , 0 valor_quitacao                                                               --106
                , 0 vl_bonificacao                                                               --107

                --Calcula calor de quitação geral para parcelas pagas tambem ch8714
                , mgrel.pck_rel_fnc.fnc_car_valor_parcelaquitacao( par.org_tab_in_codigo
                                                                 , par.org_pad_in_codigo
                                                                 , par.org_in_codigo
                                                                 , par.org_tau_st_codigo
                                                                 , par.cto_in_codigo
                                                                 , par.par_in_codigo
                                                                 , v_dt_base
                                                                 , par.par_ch_status
                                                                 , par.par_dt_status
                                                                 , par.par_dt_geracao
                                                                 , par.par_dt_vencimento
                                                                 , par.par_dt_baixa
                                                                 , par.par_ch_amortizacao
                                                                 ,'N'
                                                                 , v_tipoindice
                                                                 , -1
                                                                 ,'S'
                                                                 , v_reajuste) vl_quit_tot --108
                 -- Data do último reajuste anual, se não houver reajueste, retorna a data do contrato
                , mgrel.pck_rel_fnc.fnc_car_data_ult_reajuste( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo) data_ult_reaj_anual--109
                , v_dt_ini data_primeira_parc                                                        --110
                , v_dt_fim data_ultima_parc                                                          --111
                , ROUND( NVL( DECODE( ccb.ccr_in_codigo, NULL, 0, ccb.ccrb_re_valorbaixa), 0), 2) valor_baixa_ccred --112
                -- Saldo da(s) carta(s) de Crédito do Cliente
                , ROUND( NVL( MGREL.pck_rel_fnc.fnc_car_calcula_sld_cc_agente( cto.agn_tab_in_codigo
                                                                             , cto.agn_pad_in_codigo
                                                                             , cto.agn_in_codigo
                                                                             , v_dt_base), 0), 2) * NVL( v_perc_org, 1) sld_ccred_corrigido --113
                , v_vlr_apagar  --114
                , v_vlr_pago    --115
                , v_perc_org    --116
                , cto.cto_re_vlroricontrato * NVL( v_perc_org, 1) cto_re_vlroricontrato  --117

           FROM mgcar.car_caucao_parcela      cau
              , mgcar.car_parcela_observacao  obs
              , mgcar.car_carta_credito_baixa ccb  -- tabela para retorno das parcelas que foram pagas com carta de crédito resultante de pagamento a maior
              , mgcar.car_parcela             prr
              , mgcar.car_residuo_cobranca    res
              , mgcar.car_contrato_termo      ctt  -- Tabela para relacionamento do filtro de termos
              , mgdbm.dbm_condicao_item       cit
              , mgcar.car_parcela             par
              , mgcar.car_tabelaprice_baixa   tpb
           WHERE par.par_ch_status        <> 'I'
             AND TRUNC( par.par_dt_baixa) <= v_dt_base

             AND (( v_conf_divida  = 'N' AND NVL( par.par_bo_confdivida, 'N') = 'N')
               OR ( v_conf_divida  = 'N' AND ( par.par_bo_confdivida = 'S' AND TRUNC( par.par_dt_baixa) > v_dt_base))
               OR ( v_conf_divida  = 'S'))

             --Filtro por tipo de parcela
             AND (( v_termo LIKE '%C%' AND (( par.par_bo_contratual = 'S' AND ctt.tte_in_codigo IS NULL)))
               OR ( v_termo LIKE '%F%' AND (( par.par_bo_contratual = 'N' AND ctt.tte_in_codigo IS NULL)))
               OR ( v_termo LIKE '%T%' AND ctt.tte_in_codigo IS NOT NULL
                   AND ( v_const IS NULL OR v_const LIKE '%-' || ctt.tte_in_codigo || '-%')
                   AND ( v_desct IS NULL OR v_desct NOT LIKE '%-' || ctt.tte_in_codigo || '-%')
                   AND (( v_termo LIKE '%E%' AND par.par_bo_contratual = 'S')
                     OR ( v_termo LIKE '%N%' AND par.par_bo_contratual = 'N'))))

             AND ((( p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
               OR (( p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
                OR ( p_parc_caucao  = 'T'))

             AND ((( v_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
               OR (( v_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
                OR ( v_par_secur = 'T'))

             AND par.org_tab_in_codigo = cto.cto_org_tab_in_codigo
             AND par.org_pad_in_codigo = cto.cto_org_pad_in_codigo
             AND par.org_in_codigo     = cto.cto_org_in_codigo
             AND par.org_tau_st_codigo = cto.cto_org_tau_st_codigo
             AND par.cto_in_codigo     = cto.cto_in_codigo

             AND par.org_tab_in_codigo = cit.org_tab_in_codigo
             AND par.org_pad_in_codigo = cit.org_pad_in_codigo
             AND par.org_in_codigo     = cit.org_in_codigo
             AND par.org_tau_st_codigo = cit.org_tau_st_codigo
             AND par.cnd_in_codigo     = cit.cnd_in_codigo
             AND par.cndit_in_codigo   = cit.cndit_in_codigo

             AND ctt.org_tab_in_codigo(+)= par.org_tab_in_codigo
             AND ctt.org_pad_in_codigo(+)= par.org_pad_in_codigo
             AND ctt.org_in_codigo    (+)= par.org_in_codigo
             AND ctt.org_tau_st_codigo(+)= par.org_tau_st_codigo
             AND ctt.cto_in_codigo    (+)= par.cto_in_codigo
             AND ctt.ctt_in_codigo    (+)= par.ctt_in_codigo

             AND res.org_tab_in_codigo(+)= par.org_tab_in_codigo
             AND res.org_pad_in_codigo(+)= par.org_pad_in_codigo
             AND res.org_in_codigo    (+)= par.org_in_codigo
             AND res.org_tau_st_codigo(+)= par.org_tau_st_codigo
             AND res.cto_in_codigo    (+)= par.cto_in_codigo
             AND res.par_in_codigo    (+)= par.par_in_codigo

             AND prr.org_tab_in_codigo(+)= res.org_tab_in_codigo
             AND prr.org_pad_in_codigo(+)= res.org_pad_in_codigo
             AND prr.org_in_codigo    (+)= res.org_in_codigo
             AND prr.org_tau_st_codigo(+)= res.org_tau_st_codigo
             AND prr.cto_in_codigo    (+)= res.cto_in_codigo
             AND prr.rescob_in_codigo (+)= res.rescob_in_codigo

             AND ccb.org_tab_in_codigo(+)= par.org_tab_in_codigo
             AND ccb.org_pad_in_codigo(+)= par.org_pad_in_codigo
             AND ccb.org_in_codigo    (+)= par.org_in_codigo
             AND ccb.org_tau_st_codigo(+)= par.org_tau_st_codigo
             AND ccb.cto_in_codigo    (+)= par.cto_in_codigo
             AND ccb.par_in_codigo    (+)= par.par_in_codigo

             AND obs.org_tab_in_codigo (+)= par.org_tab_in_codigo
             AND obs.org_pad_in_codigo (+)= par.org_pad_in_codigo
             AND obs.org_in_codigo     (+)= par.org_in_codigo
             AND obs.org_tau_st_codigo (+)= par.org_tau_st_codigo
             AND obs.cto_in_codigo     (+)= par.cto_in_codigo
             AND obs.par_in_codigo     (+)= par.par_in_codigo

             AND cau.org_tab_in_codigo (+)= par.org_tab_in_codigo
             AND cau.org_pad_in_codigo (+)= par.org_pad_in_codigo
             AND cau.org_in_codigo     (+)= par.org_in_codigo
             AND cau.org_tau_st_codigo (+)= par.org_tau_st_codigo
             AND cau.cto_in_codigo     (+)= par.cto_in_codigo
             AND cau.par_in_codigo     (+)= par.par_in_codigo

             AND tpb.org_tab_in_codigo (+)= par.org_tab_in_codigo
             AND tpb.org_pad_in_codigo (+)= par.org_pad_in_codigo
             AND tpb.org_in_codigo     (+)= par.org_in_codigo
             AND tpb.org_tau_st_codigo (+)= par.org_tau_st_codigo
             AND tpb.cto_in_codigo     (+)= par.cto_in_codigo
             AND tpb.par_in_codigo     (+)= par.par_in_codigo

         UNION ALL

           -- ABERTAS
           SELECT cto.emp_in_codigo               --1
                , cto.emp_st_codigo               --2
                , cto.emp_st_nome                 --3
                , cto.etp_in_codigo               --4
                , cto.etp_st_codigo               --5
                , cto.etp_st_nome                 --6
                , cto.blo_in_codigo               --7
                , cto.blo_st_codigo               --8
                , cto.blo_st_nome                 --9
                , cto.und_in_codigo               --10
                , cto.und_st_codigo               --11
                , cto.und_st_nome                 --12
                , cto.und_re_areaprivativa        --13
                --Dados do contrato               --
                , cto.cto_org_tab_in_codigo       --14
                , cto.cto_org_pad_in_codigo       --15
                , cto.cto_org_in_codigo           --16
                , cto.cto_org_tau_st_codigo       --17
                , cto.cto_in_codigo               --18
                , cau.ctc_in_codigo               --19
                , cto.cto_ch_tipo                 --20
                , cto.cto_ds_tipo                 --21
                , cto.cto_re_valorcontrato * NVL( v_perc_org, 1) cto_re_valorcontrato    --22
                , cto.cto_re_totalresiduo         --23
                , cto.cto_bo_descprorata          --24
                , cto.cto_dt_cadastro             --25
                , cto.cto_dt_assinatura           --26
                , cto.cto_ch_status               --27
                , cto.cto_st_observacao           --28
                , cto.cnd_st_observacao           --29
                , cto.cto_re_taxabonif_sac        --30
                , cto.cto_re_taxabonif_tp         --31
                , cto.cto_re_taxaant_sac          --32
                , cto.cto_re_taxaant_tp           --33
                --dados do agente                 --
                , cto.agn_in_codigo               --34
                , cto.agn_tab_in_codigo           --35
                , cto.agn_pad_in_codigo           --36
                , cto.agn_tau_st_codigo           --37
                , cto.agn_st_fantasia             --38
                , cto.agn_st_nome                 --39
                , cto.agn_ch_tipopessoafj         --40
                , cto.agn_st_email                --41
                , cto.agn_st_telefone             --42
                -- Endereço do cliente            --
                , cto.tpl_st_sigla                --43
                , cto.agn_st_logradouro           --44
                , cto.agn_st_numero               --45
                , cto.agn_st_complemento          --46
                , cto.agn_st_bairro               --47
                , cto.agn_st_municipio            --48
                , cto.uf_st_sigla                 --49
                , cto.agn_st_cep                  --50
                -- Dados do projeto               --
                , cto.pro_st_extenso              --51
                , cto.pro_st_apelido              --52
                , cto.pro_st_descricao            --53
                , cto.pro_tab_in_codigo           --54
                , cto.pro_pad_in_codigo           --55
                , cto.pro_ide_st_codigo           --56
                , cto.pro_in_reduzido             --57
                -- Dados do centro de custo       --
                , cto.cus_st_extenso              --58
                , cto.cus_st_apelido              --59
                , cto.cus_st_descricao            --60
                , cto.cus_tab_in_codigo           --61
                , cto.cus_pad_in_codigo           --62
                , cto.cus_ide_st_codigo           --63
                , cto.cus_in_reduzido             --64
                -- Dados da parcela               --
                , par.par_in_codigo               --65
                , par.par_ch_parcela              --66
                , par.par_ch_receita              --67
                , cit.cndit_in_intervalo          --68
                , par.par_dt_vencimento           --69
                , ROUND( DECODE( vsConsJrsTPSAC, 'S', DECODE( par.pai_par_ch_origem , 'T', tpb.tpr_re_valororiginal
                                                                                    , 'S', par.par_re_vlroriginalsac
                                                                                         , NVL( par.par_re_valororiginal, 0))
                                                    , NVL( par.par_re_valororiginal, 0)), 2) * NVL( v_perc_org, 1)  par_re_valororiginal       --70
                -- Calcula o valor da parcela corrigido até a data base ou de acordo com a definição da Constel
                , NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                        , par.org_pad_in_codigo
                                                        , par.org_in_codigo
                                                        , par.org_tau_st_codigo
                                                        , par.cto_in_codigo
                                                        , par.par_in_codigo
                                                        , v_dt_base
                                                        , v_tipoindice
                                                        , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                      , par.org_pad_in_codigo
                                                                                                      , par.org_in_codigo
                                                                                                      , par.org_tau_st_codigo
                                                                                                      , par.cto_in_codigo
                                                                                                      , par.par_in_codigo), 'T', DECODE( v_vl_corrigido, 'S', 'M', DECODE( v_descapit_tp, 'S', 'TA' ,'M'))  -- TP
                                                                                                                          , 'S', DECODE( v_vl_corrigido, 'S', 'M', DECODE( v_descapit_sac, 'S', 'SA', 'M')) -- SAC
                                                                                                                               , v_reajuste)
                                                        , -1
                                                        , 'N'), 0) * NVL( v_perc_org, 1) par_re_valorcorrigido --71

                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                               , par.org_pad_in_codigo
                                                               , par.org_in_codigo
                                                               , par.org_tau_st_codigo
                                                               , par.cto_in_codigo
                                                               , par.par_in_codigo
                                                               , v_dt_base
                                                               , v_tipoindice
                                                               , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                             , par.org_pad_in_codigo
                                                                                                             , par.org_in_codigo
                                                                                                             , par.org_tau_st_codigo
                                                                                                             , par.cto_in_codigo
                                                                                                             , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', 'SA', 'M')
                                                                                                                                 , 'T', DECODE( v_descapit_tp , 'S', 'TA', 'M')
                                                                                                                                 , v_reajuste)
                                                               , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                       , par.org_pad_in_codigo
                                                                                                                                       , par.org_in_codigo
                                                                                                                                       , par.org_tau_st_codigo
                                                                                                                                       , par.cto_in_codigo
                                                                                                                                       , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', nvl(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                           , 'T', DECODE( v_descapit_tp , 'S', nvl(cto.cto_re_taxaant_tp, 0) , 0)
                                                                                                                                                                , nvl(cto.cto_re_taxaant, 0))
                                                                                         , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                       , par.org_pad_in_codigo
                                                                                                                                       , par.org_in_codigo
                                                                                                                                       , par.org_tau_st_codigo
                                                                                                                                       , par.cto_in_codigo
                                                                                                                                       , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', -1, 0)
                                                                                                                                                           , 'T', DECODE( v_descapit_tp , 'S', -1 , 0)
                                                                                                                                                                , 0))
                                                               , 'N'), 0), 2) par_re_valorcorrigido_desc      --72
                 , par.par_ch_origem             --73
                -- Retorna descrição da origem da parcela
                , mgcar.pck_car_fnc.fnc_car_origemparcela( par.org_tab_in_codigo
                                                         , par.org_pad_in_codigo
                                                         , par.org_in_codigo
                                                         , par.org_tau_st_codigo
                                                         , par.cto_in_codigo
                                                         , par.par_in_codigo
                                                         , par.par_ch_origem
                                                         , par.cnd_in_codigo
                                                         , par.par_ch_amortizacao
                                                         , 1) par_st_origem                 --74
                , SUBSTR( obs.par_st_observacao, 1, 7)        par_st_sequencia              --75
                , TO_DATE( NULL, 'dd/mm/yyyy')                par_dt_baixa                  --76
                , DECODE( SIGN( v_dt_base - par.par_dt_vencimento), 1, v_dt_base - par.par_dt_vencimento
                                                                  , 0) par_re_numerodias_atraso   --77
                -- Calcula os encargos
                , NVL( DECODE( SIGN( par.par_dt_vencimento - v_dt_base), -1, ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos( par.org_tab_in_codigo
                                                                                                                                    , par.org_pad_in_codigo
                                                                                                                                    , par.org_in_codigo
                                                                                                                                    , par.org_tau_st_codigo
                                                                                                                                    , par.cto_in_codigo
                                                                                                                                    , par.par_in_codigo
                                                                                                                                    , v_dt_base
                                                                                                                                    , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                                                                                                                                                   , par.org_pad_in_codigo
                                                                                                                                                                                   , par.org_in_codigo
                                                                                                                                                                                   , par.org_tau_st_codigo
                                                                                                                                                                                   , par.cto_in_codigo
                                                                                                                                                                                   , par.par_in_codigo
                                                                                                                                                                                   , v_dt_base
                                                                                                                                                                                   , v_tipoindice
                                                                                                                                                                                   , DECODE( par.par_ch_origem, 'T', DECODE( v_descapit_tp, 'S', DECODE(v_reajuste, 'A', 'TA', 'TM'), v_reajuste)  -- TP
                                                                                                                                                                                                              , 'S', DECODE( v_descapit_sac, 'S', DECODE(v_reajuste, 'A', 'SA', 'SM'), v_reajuste) -- SAC
                                                                                                                                                                                                              , v_reajuste)
                                                                                                                                                                                   , -1
                                                                                                                                                                                   , 'S'), 0), 2)
                                                                                                                                    , 'AM'), 0), 2) -- Calcula mora e multa, passando  no parametro o valor 'AM'
                                                                             ), 0), 0) * NVL( v_perc_org, 1) par_re_valorencargos          --78
                , 0   par_re_valordesconto           --79
                , ROUND( NVL( DECODE( SIGN( mgrel.pck_rel_fnc.fnc_car_parcela_de_residuo( par.org_tab_in_codigo
                                                                                        , par.org_pad_in_codigo
                                                                                        , par.org_in_codigo
                                                                                        , par.org_tau_st_codigo
                                                                                        , par.cto_in_codigo
                                                                                        , par.par_in_codigo
                                                                                        , 'D') - v_dt_base), 1, 0, par.par_re_residuocobranca),0),4) * NVL( v_perc_org, 1) par_re_residuocobranca     --80
                -- Taxas Adicionais
                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                          , par.org_pad_in_codigo
                                                                          , par.org_in_codigo
                                                                          , par.org_tau_st_codigo
                                                                          , par.cto_in_codigo
                                                                          , par.par_in_codigo), 0), 4) par_re_valortaxas    --81
                -- Calcula o total pago na data de baixa
                , 0   par_re_valorpago     --82
                , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo
                                                             , v_dt_base
                                                             , 'R') par_ch_reajuste    --83
                , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo
                                                             , v_dt_base
                                                             , 'S') par_st_siglaindice  --84
                , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo
                                                             , v_dt_base
                                                             , 'F') par_st_defasagem    --85
                , TO_DATE(mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa( par.org_tab_in_codigo
                                                                     , par.org_pad_in_codigo
                                                                     , par.org_in_codigo
                                                                     , par.org_tau_st_codigo
                                                                     , par.cto_in_codigo
                                                                     , par.par_in_codigo
                                                                     , v_dt_base
                                                                     , 'D'), 'DD/MM/YYYY') par_dt_vigenciaindice   --86
                -- Corrige valor parcela até data base
               , NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                       , par.org_pad_in_codigo
                                                       , par.org_in_codigo
                                                       , par.org_tau_st_codigo
                                                       , par.cto_in_codigo
                                                       , par.par_in_codigo
                                                       , v_dt_base
                                                       , v_tipoindice  --'RP'
                                                       , 'A'
                                                       , -1
                                                       , 'S'), 0) * NVL( v_perc_org, 1)       par_re_valoratualizado   --87
                -- Calculado o valor do resíduo a gerar sem correcao
                , CASE
                   WHEN (cto.cto_ch_reajusteanual = 'V' AND par.par_dt_vencimento < v_dt_base) THEN
                     ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduo( par.org_tab_in_codigo
                                                                              , par.org_pad_in_codigo
                                                                              , par.org_in_codigo
                                                                              , par.org_tau_st_codigo
                                                                              , par.cto_in_codigo
                                                                              , par.par_in_codigo
                                                                              , v_dt_base), 0), 2) * NVL(v_perc_org, 1)
                   ELSE 0
                  END par_re_valorresiduo                       --88

                -- Calculado o valor do resíduo a gerar CORRIGIDO
                , CASE
                   WHEN (cto.cto_ch_reajusteanual = 'V' AND par.par_dt_vencimento < v_dt_base) THEN
                     ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduoCorrigido( par.org_tab_in_codigo
                                                                                       , par.org_pad_in_codigo
                                                                                       , par.org_in_codigo
                                                                                       , par.org_tau_st_codigo
                                                                                       , par.cto_in_codigo
                                                                                       , par.par_in_codigo
                                                                                       , v_dt_base), 0), 2) * NVL(v_perc_org, 1)
                   ELSE 0
                 END par_re_valorresiduocorrigido               --89
                , TO_CHAR( NULL)   par_in_residuo               --90
                , ''               cobpar_in_codigo             --91
                , TO_CHAR( NULL)   rescob_bo_cobrado            --92
                , 0                rescob_re_valoracobrar       --93
                , v_cheque_pre     par_re_indicacheque_pre      --94
                , v_vlr_cheque_pre par_re_valorcheque_pre       --95 -- Ch. 45174
                , par.par_bo_confdivida                         --96
                -- Simula juros na data base
                -- Inserido o PAR_RE_VALORJUROS que esta faltando na somatória
                -- (chamado 3016-FFB) em 03/12
                , ( NVL( par.par_re_valorjuros, 0)
                  + NVL( mgcar.pck_car_fnc.fnc_car_valorjuros( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo
                                                             , v_dt_base
                                                             , v_tipoindice  --'RP'
                                                             , DECODE( par.par_ch_origem, 'T', DECODE( v_descapit_tp, 'S', 'TA', v_reajuste)  -- TP
                                                                                        , 'S', DECODE( v_descapit_sac, 'S', 'SA', v_reajuste) -- SAC
                                                                                             , v_reajuste)
                                                             , -1), 0)
                  + DECODE( vsConsJrsTPSAC, 'S', DECODE( par.pai_par_ch_origem, 'T', NVL( tpb.tpr_re_jurostp, 0)
                                                                              , 'S', par.par_re_valororiginal - par.par_re_vlroriginalsac
                                                                                   , 0)
                                               , 0)) par_re_valorjurosbx  --97
                -- Simula correcao na data base
                -- Inserido o PAR_RE_VALORCORRECAO que esta faltanndo na somatória
                -- (chamado 2156-Plaenge) em 05/09
                , ( NVL( par.par_re_valorcorrecao, 0)
                  + NVL( mgcar.pck_car_fnc.fnc_car_valorcorrecao( par.org_tab_in_codigo
                                                                , par.org_pad_in_codigo
                                                                , par.org_in_codigo
                                                                , par.org_tau_st_codigo
                                                                , par.cto_in_codigo
                                                                , par.par_in_codigo
                                                                , v_dt_base
                                                                , v_tipoindice  --'RP'
                                                                , v_reajuste  -- sempre considerar o valor original da parcela. PINC-4318
                                                                , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                        , par.org_pad_in_codigo
                                                                                                                                        , par.org_in_codigo
                                                                                                                                        , par.org_tau_st_codigo
                                                                                                                                        , par.cto_in_codigo
                                                                                                                                        , par.par_in_codigo), 'S', DECODE( v_descapit_sac  , 'S', nvl(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                            , 'T', DECODE( v_descapit_tp , 'S', nvl(cto.cto_re_taxaant_tp, 0), 0)
                                                                                                                                                                 , nvl(cto.cto_re_taxaant, 0))
                                                                                          , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                        , par.org_pad_in_codigo
                                                                                                                                        , par.org_in_codigo
                                                                                                                                        , par.org_tau_st_codigo
                                                                                                                                        , par.cto_in_codigo
                                                                                                                                        , par.par_in_codigo), 'S', DECODE( v_descapit_sac  , 'S', -1, 0)
                                                                                                                                                            , 'T', DECODE( v_descapit_tp , 'S', -1, 0)
                                                                                                                                                                 , 0))
                                                                 ), 0)) par_re_valorcorrecaobx   --98

                , NVL( par.par_re_valorjurosren, 0)    par_re_valorjurosren   --99
                -- Calcula valor da multa
                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos( par.org_tab_in_codigo
                                                                       , par.org_pad_in_codigo
                                                                       , par.org_in_codigo
                                                                       , par.org_tau_st_codigo
                                                                       , par.cto_in_codigo
                                                                       , par.par_in_codigo
                                                                       , v_dt_base
                                                                       , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                                                                                      , par.org_pad_in_codigo
                                                                                                                      , par.org_in_codigo
                                                                                                                      , par.org_tau_st_codigo
                                                                                                                      , par.cto_in_codigo
                                                                                                                      , par.par_in_codigo
                                                                                                                      , v_dt_base
                                                                                                                      , v_tipoindice
                                                                                                                      , DECODE( par.par_ch_origem, 'T', DECODE( v_descapit_tp, 'S', 'TA', v_reajuste)   -- TP
                                                                                                                                                 , 'S', DECODE( v_descapit_sac, 'S', 'SA', v_reajuste) -- SAC
                                                                                                                                                      , v_reajuste)
                                                                                                                      , -1
                                                                                                                      , 'S'), 0),2)
                                                                       , 'M'), 0),2) par_re_valormulta -- Calcular  multa, passando  no parametro o valor 'M'    --100

                 -- Calcula valor do atraso
                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos( par.org_tab_in_codigo
                                                                       , par.org_pad_in_codigo
                                                                       , par.org_in_codigo
                                                                       , par.org_tau_st_codigo
                                                                       , par.cto_in_codigo
                                                                       , par.par_in_codigo
                                                                       , v_dt_base
                                                                       , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                                                                                      , par.org_pad_in_codigo
                                                                                                                      , par.org_in_codigo
                                                                                                                      , par.org_tau_st_codigo
                                                                                                                      , par.cto_in_codigo
                                                                                                                      , par.par_in_codigo
                                                                                                                      , v_dt_base
                                                                                                                      , v_tipoindice
                                                                                                                      , DECODE( par.par_ch_origem, 'T', DECODE( v_descapit_tp, 'S', 'TA', v_reajuste)   -- TP
                                                                                                                                                 , 'S', DECODE( v_descapit_sac, 'S', 'SA', v_reajuste) -- SAC
                                                                                                                                                      , v_reajuste)
                                                                                                                      , -1
                                                                                                                      , 'S'), 0), 2)
                                                                       , 'A'), 0),2) par_re_valoratraso -- Calcular  valor atraso, passando  no parametro o valor 'A' ---101
                , ROUND( mgcar.pck_car_fnc.fnc_car_calculaencargos( par.org_tab_in_codigo
                                                                  , par.org_pad_in_codigo
                                                                  , par.org_in_codigo
                                                                  , par.org_tau_st_codigo
                                                                  , par.cto_in_codigo
                                                                  , par.par_in_codigo
                                                                  , v_dt_base
                                                                  , NVL( par.par_re_valororiginal, 0)
                                                                  + NVL( par.par_re_valorjuros, 0)
                                                                  + NVL( par.par_re_valorcorrecao, 0)
                                                                  + ROUND( mgcar.pck_car_fnc.fnc_car_valorcorrecao( par.org_tab_in_codigo
                                                                                                                  , par.org_pad_in_codigo
                                                                                                                  , par.org_in_codigo
                                                                                                                  , par.org_tau_st_codigo
                                                                                                                  , par.cto_in_codigo
                                                                                                                  , par.par_in_codigo
                                                                                                                  , v_dt_base
                                                                                                                  , 'RP'
                                                                                                                  , 'M'
                                                                                                                  , -1), 2)
                                                                  + ROUND( mgcar.pck_car_fnc.fnc_car_valorjuros( par.org_tab_in_codigo
                                                                                                               , par.org_pad_in_codigo
                                                                                                               , par.org_in_codigo
                                                                                                               , par.org_tau_st_codigo
                                                                                                               , par.cto_in_codigo
                                                                                                               , par.par_in_codigo
                                                                                                               , v_dt_base
                                                                                                               , 'RP'
                                                                                                               , 'M'
                                                                                                               , -1), 2)
                                                                  , 'C'
                                                                  , 'N'
                                                                  , 0), 2) par_re_valorcorrecao_atr --102
                , 'ABERTA' par_st_indica                                                            --103
                -- Dados da parcela de resíduo de cobrança.
                , TO_NUMBER( NULL)                  rescob_in_codigo                                --104

                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                               , par.org_pad_in_codigo
                                                               , par.org_in_codigo
                                                               , par.org_tau_st_codigo
                                                               , par.cto_in_codigo
                                                               , par.par_in_codigo
                                                               , v_dt_base
                                                               , 'RP'
                                                               , v_reajuste
                                                               , -1
                                                               , 'N'), 0), 2) vlr_corrigido_total  --105


                 -- Valor para quitação
                , NVL( mgrel.pck_rel_fnc.fnc_car_valor_parcelaquitacao( par.org_tab_in_codigo
                                                                      , par.org_pad_in_codigo
                                                                      , par.org_in_codigo
                                                                      , par.org_tau_st_codigo
                                                                      , par.cto_in_codigo
                                                                      , par.par_in_codigo
                                                                      , v_dt_base
                                                                      , par.par_ch_status
                                                                      , par.par_dt_status
                                                                      , par.par_dt_geracao
                                                                      , par.par_dt_vencimento
                                                                      , par.par_dt_baixa
                                                                      , par.par_ch_amortizacao
                                                                      , 'S'
                                                                      , v_tipoindice
                                                                      , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                              , par.org_pad_in_codigo
                                                                                                                                              , par.org_in_codigo
                                                                                                                                              , par.org_tau_st_codigo
                                                                                                                                              , par.cto_in_codigo
                                                                                                                                              , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', NVL(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                                  , 'T', DECODE( v_descapit_tp , 'S', NVL(cto.cto_re_taxaant_tp, 0), 0)
                                                                                                                                                                       , NVL( cto.cto_re_taxaant, 0))
                                                                                                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                              , par.org_pad_in_codigo
                                                                                                                                              , par.org_in_codigo
                                                                                                                                              , par.org_tau_st_codigo
                                                                                                                                              , par.cto_in_codigo
                                                                                                                                              , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', -1, 0)
                                                                                                                                                                  , 'T', DECODE( v_descapit_tp, 'S', -1, 0)
                                                                                                                                                                       , 0))
                                                                      , 'S'
                                                                      , v_reajuste),0 ) * NVL( v_perc_org, 1)  valor_quitacao --106

                 -- vl_bonificacao
                , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_renda_postecipada( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                                                                            , par.org_pad_in_codigo
                                                                                                            , par.org_in_codigo
                                                                                                            , par.org_tau_st_codigo
                                                                                                            , par.cto_in_codigo
                                                                                                            , par.par_in_codigo
                                                                                                            , v_dt_base
                                                                                                            , 'RP'
                                                                                                            , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                                          , par.org_pad_in_codigo
                                                                                                                                                          , par.org_in_codigo
                                                                                                                                                          , par.org_tau_st_codigo
                                                                                                                                                          , par.cto_in_codigo
                                                                                                                                                          , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', 'SA', 'M')
                                                                                                                                                                              , 'T', DECODE( v_descapit_tp,  'S', 'TA', 'M')
                                                                                                                                                                                   , v_reajuste)
                                                                                                            , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                                                                    , par.org_pad_in_codigo
                                                                                                                                                                                    , par.org_in_codigo
                                                                                                                                                                                    , par.org_tau_st_codigo
                                                                                                                                                                                    , par.cto_in_codigo
                                                                                                                                                                                    , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', nvl(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                                                                        , 'T', DECODE( v_descapit_tp, 'S', nvl(cto.cto_re_taxaant_tp, 0), 0)
                                                                                                                                                                                                             , nvl(cto.cto_re_taxaant, 0))
                                                                                                                                      , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                                                                                    , par.org_pad_in_codigo
                                                                                                                                                                                    , par.org_in_codigo
                                                                                                                                                                                    , par.org_tau_st_codigo
                                                                                                                                                                                    , par.cto_in_codigo
                                                                                                                                                                                    , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', -1, 0)
                                                                                                                                                                                                        , 'T', DECODE( v_descapit_tp, 'S', -1, 0)
                                                                                                                                                                                                             , 0)))
                                                                         , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem ( par.org_tab_in_codigo
                                                                                                                       , par.org_pad_in_codigo
                                                                                                                       , par.org_in_codigo
                                                                                                                       , par.org_tau_st_codigo
                                                                                                                       , par.cto_in_codigo
                                                                                                                       , par.par_in_codigo), 'S', DECODE( v_cons_bonif, 'S', nvl(cto.cto_re_taxabonif_sac, 0), 0)
                                                                                                                                           , 'T', DECODE( v_cons_bonif, 'S', nvl(cto.cto_re_taxabonif_tp, 0) , 0)
                                                                                                                                                , DECODE( v_cons_taxa , 'S', nvl(cto.cto_re_taxaant, 0)      , 0))
                                                                                                                                                -- para parcelas sacoc é calculada a taxa de antecipação na função
                                                                                                                                                -- de bonificação
                                                                         , v_dt_base
                                                                         , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem( par.org_tab_in_codigo
                                                                                                                      , par.org_pad_in_codigo
                                                                                                                      , par.org_in_codigo
                                                                                                                      , par.org_tau_st_codigo
                                                                                                                      , par.cto_in_codigo
                                                                                                                      , par.par_in_codigo), 'S', cit.cndit_dt_referenciatp
                                                                                                                                          , 'T', cit.cndit_dt_referenciatp
                                                                                                                                               , par.par_dt_vencimento)
                                                                         , 'C' -- COMPOSTO
                                                                         , cto.cto_bo_descprorata
                                                                         , 'N'
                                                                         , 360
                                                                         , DECODE(par.par_ch_amortizacao, 'S', par.org_tab_in_codigo, 0)
                                                                         , DECODE(par.par_ch_amortizacao, 'S', par.org_pad_in_codigo, 0)
                                                                         , DECODE(par.par_ch_amortizacao, 'S', cto.fil_in_codigo, 0)), 0), 2) vl_bonificacao --107

                --Calcula calor de quitação geral para parcelas pagas tambem ch8714
                , mgrel.pck_rel_fnc.fnc_car_valor_parcelaquitacao( par.org_tab_in_codigo
                                                                 , par.org_pad_in_codigo
                                                                 , par.org_in_codigo
                                                                 , par.org_tau_st_codigo
                                                                 , par.cto_in_codigo
                                                                 , par.par_in_codigo
                                                                 , v_dt_base
                                                                 , par.par_ch_status
                                                                 , par.par_dt_status
                                                                 , par.par_dt_geracao
                                                                 , par.par_dt_vencimento
                                                                 , par.par_dt_baixa
                                                                 , par.par_ch_amortizacao
                                                                 , 'N'
                                                                 , v_tipoindice
                                                                 , -1
                                                                 , 'S'
                                                                 , v_reajuste) vl_quit_tot  --108
                 -- Data do último reajuste anual, se não houver reajueste, retorna a data do contrato
                , mgrel.pck_rel_fnc.fnc_car_data_ult_reajuste( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo) data_ult_reaj_anual --109
                , v_dt_ini data_primeira_parc                                                         --110
                , v_dt_fim data_ultima_parc                                                           --111
                , 0 valor_baixa_ccred                                                                 --112
                -- Saldo da(s) carta(s) de Crédito do Cliente
                , ROUND( NVL( mgrel.pck_rel_fnc.fnc_car_calcula_sld_cc_agente( cto.agn_tab_in_codigo
                                                                             , cto.agn_pad_in_codigo
                                                                             , cto.agn_in_codigo
                                                                             , v_dt_base), 0), 2) * NVL( v_perc_org, 1) sld_ccred_corrigido --113
                , v_vlr_apagar                                                                                                              --114
                , v_vlr_pago                                                                                                                --115
                , v_perc_org                                                                                                                --116
                , cto.cto_re_vlroricontrato * NVL( v_perc_org, 1) cto_re_vlroricontrato                                                     --117
             FROM mgcar.car_caucao_parcela     cau
                , mgcar.car_parcela_observacao obs
                , mgcar.car_residuo_cobranca   res
                , mgcar.car_contrato_termo     ctt -- Tabela para relacionamento do filtro de termos
                , mgdbm.dbm_condicao_item      cit
                , mgcar.car_parcela            par
                , mgcar.car_tabelaprice_baixa  tpb
            WHERE --PAR
                  ( ( par.par_ch_status <> 'I')
                 OR ( par.par_ch_status  = 'I'
                  AND TRUNC( par.par_dt_status) > v_dt_base))
              AND (( par.par_dt_baixa IS NULL) OR ( TRUNC( par.par_dt_baixa) > v_dt_base))
              AND TRUNC( par.par_dt_geracao) <= v_dt_base

              AND (( v_conf_divida  = 'N' AND NVL( par.par_bo_confdivida, 'N') = 'N')
                OR ( v_conf_divida  = 'N' AND ( par.par_bo_confdivida = 'S' AND TRUNC( par.par_dt_baixa) > v_dt_base))
                OR ( v_conf_divida  = 'S'))

              --Filtro por tipo de parcela
              AND (( v_termo LIKE '%C%' AND (( par.par_bo_contratual = 'S' AND ctt.tte_in_codigo IS NULL)))
                OR ( v_termo LIKE '%F%' AND ( ( par.par_bo_contratual = 'N' AND ctt.tte_in_codigo IS NULL)))
                OR ( v_termo LIKE '%T%' AND ctt.tte_in_codigo IS NOT NULL
                    AND ( v_const IS NULL OR v_const LIKE '%-' || ctt.tte_in_codigo || '-%')
                    AND ( v_desct IS NULL OR v_desct NOT LIKE '%-' || ctt.tte_in_codigo || '-%')
                    AND (( v_termo LIKE '%E%' AND par.par_bo_contratual = 'S')
                      OR ( v_termo LIKE '%N%' AND par.par_bo_contratual = 'N') ) ) )

              AND ((( p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
                OR (( p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
                 OR ( p_parc_caucao  = 'T'))

              AND ((( v_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
                OR (( v_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
                 OR ( v_par_secur = 'T'))

              AND par.org_tab_in_codigo = cto.cto_org_tab_in_codigo
              AND par.org_pad_in_codigo = cto.cto_org_pad_in_codigo
              AND par.org_in_codigo     = cto.cto_org_in_codigo
              AND par.org_tau_st_codigo = cto.cto_org_tau_st_codigo
              AND par.cto_in_codigo     = cto.cto_in_codigo

              AND par.org_tab_in_codigo = cit.org_tab_in_codigo
              AND par.org_pad_in_codigo = cit.org_pad_in_codigo
              AND par.org_in_codigo     = cit.org_in_codigo
              AND par.org_tau_st_codigo = cit.org_tau_st_codigo
              AND par.cnd_in_codigo     = cit.cnd_in_codigo
              AND par.cndit_in_codigo   = cit.cndit_in_codigo

              AND ctt.org_tab_in_codigo (+)= par.org_tab_in_codigo
              AND ctt.org_pad_in_codigo (+)= par.org_pad_in_codigo
              AND ctt.org_in_codigo     (+)= par.org_in_codigo
              AND ctt.org_tau_st_codigo (+)= par.org_tau_st_codigo
              AND ctt.cto_in_codigo     (+)= par.cto_in_codigo
              AND ctt.ctt_in_codigo     (+)= par.ctt_in_codigo

              AND res.org_tab_in_codigo (+)= par.org_tab_in_codigo
              AND res.org_pad_in_codigo (+)= par.org_pad_in_codigo
              AND res.org_in_codigo     (+)= par.org_in_codigo
              AND res.org_tau_st_codigo (+)= par.org_tau_st_codigo
              AND res.cto_in_codigo     (+)= par.cto_in_codigo
              AND res.par_in_codigo     (+)= par.par_in_codigo
              AND res.rescob_in_codigo  (+)= par.rescob_in_codigo

              AND obs.org_tab_in_codigo (+)= par.org_tab_in_codigo
              AND obs.org_pad_in_codigo (+)= par.org_pad_in_codigo
              AND obs.org_in_codigo     (+)= par.org_in_codigo
              AND obs.org_tau_st_codigo (+)= par.org_tau_st_codigo
              AND obs.cto_in_codigo     (+)= par.cto_in_codigo
              AND obs.par_in_codigo     (+)= par.par_in_codigo

              AND cau.org_tab_in_codigo (+)= par.org_tab_in_codigo
              AND cau.org_pad_in_codigo (+)= par.org_pad_in_codigo
              AND cau.org_in_codigo     (+)= par.org_in_codigo
              AND cau.org_tau_st_codigo (+)= par.org_tau_st_codigo
              AND cau.cto_in_codigo     (+)= par.cto_in_codigo
              AND cau.par_in_codigo     (+)= par.par_in_codigo

             AND tpb.org_tab_in_codigo (+)= par.org_tab_in_codigo
             AND tpb.org_pad_in_codigo (+)= par.org_pad_in_codigo
             AND tpb.org_in_codigo     (+)= par.org_in_codigo
             AND tpb.org_tau_st_codigo (+)= par.org_tau_st_codigo
             AND tpb.cto_in_codigo     (+)= par.cto_in_codigo
             AND tpb.par_in_codigo     (+)= par.par_in_codigo ;

    END LOOP;
  COMMIT;

  BEGIN
    SELECT SUM( NVL(rel.par_re_valorresiduo_cc, 0))
    INTO vnTotalResiduo
    FROM mgcustom.rel_dados_contrato_api rel
       , ( SELECT par.org_tab_in_codigo
                , par.org_pad_in_codigo
                , par.org_in_codigo
                , par.org_tau_st_codigo
                , par.cto_in_codigo
                , par.par_in_codigo

           FROM mgcar.car_parcela par
           WHERE (( par.par_ch_status <> 'I' )
               OR ( par.par_ch_status = 'I'
                AND TRUNC ( par.par_dt_status) > v_dt_base))

             AND TRUNC( par.par_dt_realizacaobx) <= v_dt_base
             AND par.par_ch_receitabaixa  = 'C'

             -- Considera cheques pre vencidos apenas com status de aberto, devolvido, depositado ou em custódia, e NÃO CONSIDERA cheques conpensados.
             AND ( par.par_ch_status IN( 'P', 'D', '1', '2', 'U')
               OR ( par.par_ch_status = 'A'
              AND ( TRUNC ( par.par_dt_deposito) > v_dt_base
                 OR par.par_dt_deposito IS NULL)))

             AND ( v_ctrl_cheq = 'S'
               AND v_mostra_chq = 'S')) chq

    WHERE rel.org_tab_in_codigo = chq.org_tab_in_codigo(+)
      AND rel.org_pad_in_codigo = chq.org_pad_in_codigo(+)
      AND rel.org_in_codigo     = chq.org_in_codigo    (+)
      AND rel.org_tau_st_codigo = chq.org_tau_st_codigo(+)
      AND rel.cto_in_codigo     = chq.cto_in_codigo    (+)
      AND rel.par_in_codigo     = chq.par_in_codigo    (+)

    GROUP BY 1;
  EXCEPTION
    WHEN OTHERS THEN
      vnTotalResiduo := 0;
  END;

  OPEN retorno FOR
    SELECT rel.org_tab_in_codigo                           est_org_tab_in_codigo
         , rel.org_pad_in_codigo                           est_org_pad_in_codigo
         , rel.org_in_codigo                               est_org_in_codigo
         , rel.org_tau_st_codigo                           est_org_tau_st_codigo
         , rel.emp_in_codigo
         , rel.emp_st_codigo
         , rel.emp_st_nome
         , rel.etp_in_codigo
         , rel.etp_st_codigo
         , rel.etp_st_nome
         , rel.blo_in_codigo
         , rel.blo_st_codigo
         , rel.blo_st_nome
         , rel.und_in_codigo
         , rel.und_st_codigo
         , rel.und_st_nome
         , rel.und_re_peso                                     und_re_areaprivativa
         -- Dados do contrato
         , rel.cto_in_codigo
         , rel.par_in_codigo_pg                                ctc_in_codigo
         , rel.cto_ch_tipo
         , CAST( rel.cto_ds_tipo AS VARCHAR(200))              cto_ds_tipo
         , rel.cto_re_valorcontrato
         , rel.cto_re_totalresiduo
         , rel.cto_dt_cadastro
         , rel.ind_dt_vigencia                                 cto_dt_assinatura
         , rel.cto_ch_status
         , rel.cto_st_observacao
         , TRIM( CAST( rel.oco_st_ocorrencia AS VARCHAR(255))) cnd_st_observacao
         --Dados do agente
         , rel.agn_in_codigo
         , rel.agn_tab_in_codigo
         , rel.agn_pad_in_codigo
         , rel.agn_tau_st_codigo
         , rel.agn_st_fantasia
         , rel.agn_st_nome
         , rel.agn_ch_tipopessoafj
         , CAST( rel.oco_st_complemento AS VARCHAR(200))       agn_st_email
         , CAST( rel.cto_ds_origem AS VARCHAR(255))            agn_st_telefone
         , rel.tpl_st_sigla
         , rel.agn_st_logradouro
         , rel.agn_st_numero
         , rel.agn_st_complemento
         , rel.agn_st_bairro
         , rel.agn_st_municipio
         , rel.uf_st_sigla
         , rel.agn_st_cep
         -- Dados do projeto
         , rel.pro_st_extenso
         , rel.pro_st_apelido
         , rel.pro_st_descricao
         , rel.pro_tab_in_codigo
         , rel.pro_pad_in_codigo
         , rel.pro_ide_st_codigo
         , rel.pro_in_reduzido
         -- Dados Centro de Custo
         , rel.csf_st_descricao                               cus_st_extenso
         , rel.cto_ds_status                                  cus_st_apelido
         , CAST( rel.par_st_receita AS VARCHAR(100))          cus_st_descricao
         , rel.cto_in_indice                                  cus_tab_in_codigo
         , rel.ocr_in_codigo                                  cus_pad_in_codigo
         , rel.ind_st_nome                                    cus_ide_st_codigo
         , rel.est_org_tab_in_codigo                          cus_in_reduzido
         -- Dados Parcelas
         , rel.par_in_codigo
         , DECODE( v_tp_par, 'N', DECODE( rel.par_ch_parcela, 'C', 'H' -- Conclusão
                                                             , 'I', DECODE( rel.par_ch_status_bx, 'G', 'FG' -- FGTS
                                                                                                , 'F', 'F' -- Financiamento
                                                                                                , 'C', DECODE( rel.par_in_codigo_vcd, 12, 'A'  -- anual
                                                                                                                                , 6 , 'SE' -- semestral
                                                                                                                                , rel.par_ch_parcela))-- 'I'

                                                                  , rel.par_ch_parcela)
                                ,  rel.par_ch_parcela) par_ch_parcela

         , DECODE( v_tp_par, 'N', DECODE( rel.par_ch_parcela, 'C', 'Habite-se' -- Conclusão
                                                        , 'I', DECODE( rel.par_ch_status_bx, 'G', 'FGTS' -- FGTS
                                                                                           , 'F', 'Financiamento' -- Financiamento
                                                                                           , 'C', DECODE( rel.par_in_codigo_vcd, 12, 'Anual'  -- anual
                                                                                                                               , 6 , 'Semestral' -- semestral
                                                                                                                               , 'Intermediária'))-- 'I'
                                                        , rel.par_ch_parcela)
                                , DECODE( rel.par_ch_parcela, 'I', 'Intermediária'
                                                            , 'M', 'Mensal'
                                                            , 'C', 'Conclusão'
                                                            , 'R', 'Resíduo'
                                                            , 'S', 'Sinal'
                                                            , 'B', 'Res. Cobrança'
                                                            , 'T', 'Taxa'
                                                            , rel.par_ch_parcela)) par_ds_parcela
         , rel.par_in_codigo_vcd                                               par_in_intervalo
         , DECODE( rel.par_in_codigo_vcd, 1,  'Mensal'
                                        , 2,  'Bimestral'
                                        , 3,  'Trimestral'
                                        , 4,  'Quadrimensal'
                                        , 5,  'Quinquimestral'
                                        , 6,  'Semestral'
                                        , 7,  'Septuamestral'
                                        , 8,  'Octamensal'
                                        , 9,  'Nonamestral'
                                        , 10, 'Decamestral'
                                        , 11, 'Andecamestral'
                                        , 12, 'Anual'
                                        , 24, 'Bianual'
                                        , 36, 'Trianual')     par_ds_intervalo
         , rel.par_dt_vencimento
         , rel.par_re_valororiginal
         , rel.par_re_valorcorrigido_vcd                      par_re_valorcorrigido
         , rel.par_re_valorantecipacao                        par_re_valorcorrigido_desc
         , rel.par_ch_origem
         , rel.par_ds_origem                                  par_st_origem
         , rel.par_st_observacao                              par_st_sequencia
         , rel.par_dt_baixa
         , rel.cndit_in_codigo                                par_re_numerodias_atraso
         , rel.par_re_valorencargo                            par_re_valorencargos
         , rel.par_re_valordesconto
         , rel.par_re_residuocobranca
         , rel.par_re_valorquitacao_tpvc                      par_re_valortaxas
         , rel.par_re_valorpago
         , rel.par_ch_status                                  par_ch_reajuste
         , CAST( rel.par_ds_status AS VARCHAR(200))           par_st_siglaindice
         , rel.parcor_in_codigo                               par_st_defasagem
         , rel.cto_dt_entrega                                 par_dt_vigenciaindice
         , rel.par_re_valor_atual                             par_re_valoratualizado
         , rel.par_re_valor_residuo                           par_re_valorresiduo
         , rel.par_re_valorresiduo_cc                         par_re_valorresiduocorrigido
         , rel.ant_in_codigo                                  par_in_residuo
         , rel.ocs_st_descricao                               cobpar_in_codigo
         , rel.par_bo_contratual                              rescob_bo_cobrado
         , rel.par_re_valoramortizado                         rescob_re_valoracobrar
         , rel.par_re_valor_totalpago                         par_re_indicacheque_pre
         , rel.par_re_percentual                          par_re_valorcheque_pre   -- Ch. 45174
         , rel.cto_bo_taxaempr                                par_bo_confdivida
         , rel.par_re_valorjurosbx
         , rel.par_re_valorcorrecaobx
         , rel.par_re_valor_depchave                          par_re_valorjurosren
         , rel.par_re_valormulta
         , rel.par_re_valoratraso
         , rel.par_re_valorcorrecao_atr
         , CAST( rel.ctt_ds_tipo AS VARCHAR(200))             par_st_indica
         , rel.parcor_in_indice                               rescob_in_codigo
         , rel.par_re_valorcorrigido_avc                      vlr_corrigido_total
         , rel.par_re_credito                                 valor_quitacao
         , rel.par_re_jrotpncobrado                           vl_bonificacao
         , rel.par_re_valor_antchave                          vl_quit_tot
         , rel.ctt_dt_emissao                                 data_ult_reaj_anual
         , rel.cto_dt_importacao                              data_primeira_parc
         , rel.ent_dt_entrega                                 data_ultima_parc
         , rel.ctt_re_valor                                   valor_baixa_ccred
         , rel.par_re_valorfinanciado                         sld_ccred_corrigido
         , NVL( rel.par_re_saldodevedor, 0) + vnTotalResiduo  vlr_apagar
         , NVL( rel.par_re_saldoquitacao, 0)                  vlr_pago
         , chq.chq_pre   -- indica se a parcela foi paga com cheque pré. Ch. 45174
         , rel.cto_re_mora                                    perc_org
         , rel.par_re_valorquitacao_tpnv                      cto_re_vlroricontrato
         , mfi.MFI_ST_NOSSONUMERO                             NOSSO_NUMERO
    FROM mgcustom.rel_dados_contrato_api rel
    left join (
      SELECT par.org_tab_in_codigo
          , par.org_pad_in_codigo
          , par.org_in_codigo
          , par.org_tau_st_codigo
          , par.cto_in_codigo
          , par.par_in_codigo
          , 'X' chq_pre
      FROM mgcar.car_parcela par
      WHERE (( par.par_ch_status <> 'I' ) OR ( par.par_ch_status = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
        AND TRUNC( par.par_dt_realizacaobx) <= v_dt_base
        AND par.par_ch_receitabaixa  = 'C'
        -- Considera cheques pre vencidos apenas com status de aberto, devolvido, depositado ou em custódia, e NÃO CONSIDERA cheques conpensados.
        AND ( par.par_ch_status IN ( 'P', 'D', '1', '2', 'U') OR ( par.par_ch_status = 'A' AND ( TRUNC( par.par_dt_deposito) > v_dt_base OR par.par_dt_deposito IS NULL)))
        AND ( v_ctrl_cheq = 'S' AND v_mostra_chq = 'S')
    ) chq
      on (rel.org_tab_in_codigo = chq.org_tab_in_codigo
      AND rel.org_pad_in_codigo = chq.org_pad_in_codigo
      AND rel.org_in_codigo     = chq.org_in_codigo
      AND rel.org_tau_st_codigo = chq.org_tau_st_codigo
      AND rel.cto_in_codigo     = chq.cto_in_codigo
      AND rel.par_in_codigo     = chq.par_in_codigo)
    left join MGCAR.CAR_DOCUMENTO_FINANCEIRO dfi
      on (rel.org_tab_in_codigo = dfi.org_tab_in_codigo
      AND rel.org_pad_in_codigo = dfi.org_pad_in_codigo
      AND rel.org_in_codigo     = dfi.org_in_codigo
      AND rel.org_tau_st_codigo = dfi.org_tau_st_codigo
      AND rel.cto_in_codigo     = dfi.cto_in_codigo
      AND rel.par_in_codigo     = dfi.par_in_codigo)
    left join MGCAR.CAR_MOVIMENTO_FINANCEIRO mfi
      on (dfi.dfi_in_codigo = mfi.dfi_in_codigo)

   ORDER BY par_dt_vencimento, rel.par_in_codigo;

END PRC_CAR_EXTRATO_COMPLETO_EMP;
