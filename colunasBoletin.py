rel.cto_in_codigo                             cto_in_codigo
, rel.fil_in_codigo                             fil_in_codigo
, rel.agn_st_fantasia                           fil_st_nome
, rel.emp_in_codigo                             emp_in_codigo
, rel.emp_st_codigo                             emp_st_codigo
, rel.emp_st_nome                               emp_st_nome
, rel.blo_in_codigo                             blo_in_codigo
, rel.blo_st_nome                               blo_st_nome
, rel.und_st_codigo                             und_st_codigo
, rel.par_ch_origem                             ctoenv_ch_origem
, rel.agn_ch_tipopessoafj                               tipo_cliente
, rel.agn_st_nome                               agn_st_nome
, rel.par_in_codigo                             par_in_codigo
, rel.par_dt_vencimento                         par_dt_vencimento
, rel.par_dt_geracao                            par_dt_movimento
, rel.ctt_dt_emissao                            par_dt_realizacaobx
, rel.par_re_valororiginal                      par_re_valororiginal
, rel.par_re_valorpago                          par_re_valorpago
, rel.par_re_valorencargo                       par_re_valortaxas
, rel.par_re_valormulta                         par_re_valormulta
, rel.par_re_valoratraso                        par_re_valoratraso
, rel.par_re_valor_corrigido                    vl_corrigido
, rel.par_re_valorjurostp                       vl_juro
, rel.par_re_valorjurosren                      par_re_valorjurosren
, rel.par_re_valor_pagocorrigido                vl_pago
, rel.par_re_valoramortizado                    par_re_descontosemantecip
, rel.par_re_valorantecipacao                   par_re_descontoantecip
, rel.par_re_residuocobranca                    par_re_residuocobranca
, rel.par_dt_baixa                              par_dt_baixa
, rel.ind_ch_tipo                               par_bo_confdivida
, rel.ind_st_nome                               par_st_agencia
, rel.par_ch_status_bx                          par_ch_receitabaixa
, rel.oco_in_codigo                             ban_in_numero
, rel.agn_st_complemento                        par_st_conta
, rel.par_re_credito                            par_re_credito
, rel.ctt_re_valor                              tpr_re_abatido
, rel.cto_re_valorcontrato                      tpr_re_jurostp
, rel.cto_re_totalresiduo                       ant_re_vmjrotpncob
, rel.par_re_valorcorrecao_atr                  par_re_valorcorrecao_atr
, rel.pro_in_reduzido                           pro_in_reduzido
, rel.pro_st_descricao                          pro_st_descricao
, CAST( rel.par_st_observacao AS VARCHAR2(255)) par_st_observacao
, rel.tte_in_codigo                             tte_in_codigo
, rel.agn_st_bairro                             tte_st_descricao
, rel.cto_ch_origem                             identificador
, rel.par_re_valor_atraso                       pla_re_jrotp
, rel.est_org_in_codigo                         inv_in_codigo
, CAST( rel.cto_ds_origem AS VARCHAR2(255))     inv_st_nome
, rel.agn_in_codigo                             agn_in_codigo