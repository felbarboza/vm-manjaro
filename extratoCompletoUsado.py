EMP_ST_NOME	 rel.emp_st_nome
CTO_IN_CODIGO	rel.cto_in_codigo
CTO_RE_VALORCONTRATO	rel.cto_re_valorcontrato
CTO_DT_CADASTRO	rel.cto_dt_cadastro
CTO_DT_ASSINATURA	rel.ind_dt_vigencia    cto_dt_assinatura
CTO_CH_STATUS	rel.cto_ch_status
AGN_ST_NOME	rel.agn_st_nome
CUS_ST_DESCRICAO CAST( rel.par_st_receita AS VARCHAR(100))   cus_st_descricao
CUS_IN_REDUZIDO rel.est_org_tab_in_codigo   cus_in_reduzido
PAR_IN_CODIGO rel.par_in_codigo
PAR_CH_PARCELA DECODE( v_tp_par, 'N', DECODE( rel.par_ch_parcela, 'C', 'H' -- Conclusão
                                                             , 'I', DECODE( rel.par_ch_status_bx, 'G', 'FG' -- FGTS
                                                                                                , 'F', 'F' -- Financiamento
                                                                                                , 'C', DECODE( rel.par_in_codigo_vcd, 12, 'A'  -- anual
                                                                                                                                , 6 , 'SE' -- semestral
                                                                                                                                , rel.par_ch_parcela))-- 'I'

                                                                  , rel.par_ch_parcela)
                                ,  rel.par_ch_parcela) par_ch_parcela
PAR_DT_VENCIMENTO rel.par_dt_vencimento
PAR_RE_VALORORIGINAL rel.par_re_valororiginal
PAR_RE_VALORCORRIGIDO rel.par_re_valorcorrigido_vcd  par_re_valorcorrigido
PAR_ST_ORIGEM rel.par_ds_origem   par_st_origem
PAR_ST_OBSERVACAO rel.par_st_observacao 
PAR_DT_BAIXA rel.par_dt_baixa
PAR_RE_NUMERODIAS_ATRASO rel.cndit_in_codigo par_re_numerodias_atraso
PAR_RE_VALORENCARGOS rel.par_re_valorencargo
PAR_RE_VALORDESCONTO rel.par_re_valordesconto
PAR_RE_RESIDUOCOBRANCA rel.par_re_residuocobranca
PAR_RE_VALORTAXAS rel.par_re_valorquitacao_tpvc  par_re_valortaxas
PAR_RE_VALORPAGO rel.par_re_valorpago
PAR_CH_REAJUSTE rel.par_ch_status   par_ch_reajuste
PAR_ST_SIGLAINDICE CAST( rel.par_ds_status AS VARCHAR(200)) par_st_siglaindice
PAR_ST_DEFASAGEM rel.parcor_in_codigo    par_st_defasagem
PAR_DT_VIGENCIAINDICE rel.cto_dt_entrega  par_dt_vigenciaindice
PAR_RE_VALORATUALIZADO rel.par_re_valor_atual  par_re_valoratualizado
PAR_RE_VALORRESIDUO rel.par_re_valor_residuo    par_re_valorresiduo
PAR_RE_VALORRESIDUOCORRIGIDO rel.par_re_valorresiduo_cc   par_re_valorresiduocorrigido
PAR_IN_RESIDUO rel.ant_in_codigo    par_in_residuo
COBPAR_IN_CODIGO rel.ocs_st_descricao  cobpar_in_codigo
RESCOB_RE_VALORACOBRAR rel.par_re_valoramortizado  rescob_re_valoracobrar
PAR_BO_CONFDIVIDA rel.cto_bo_taxaempr  par_bo_confdivida
VALOR_QUITACAO rel.par_re_credito                                 valor_quitacao
DATA_ULT_REAJ_ANUAL rel.ctt_dt_emissao                                 data_ult_reaj_anual
DATA_PRIMEIRA_PARC rel.cto_dt_importacao                              data_primeira_parc
DATA_ULTIMA_PARC rel.ent_dt_entrega                                 data_ultima_parc
VALOR_BAIXA_CCRED rel.ctt_re_valor                                   valor_baixa_ccred
SLD_CCRED_CORRIGIDO rel.par_re_valorfinanciado                         sld_ccred_corrigido
CTO_RE_MORA rel.cto_re_mora 
CTO_RE_VLRORICONTRATO rel.par_re_valorquitacao_tpnv                      cto_re_vlroricontrato
NOSSO_NUMERO mfi.MFI_ST_NOSSONUMERO                             NOSSO_NUMERO

