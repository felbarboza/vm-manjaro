  WHERE cto.org_tab_in_codigo = DECODE( :v_emitir_por, 'E', :v_tab, cto.org_tab_in_codigo)
    AND cto.org_pad_in_codigo = DECODE( :v_emitir_por, 'E', :v_pad, cto.org_pad_in_codigo)
    AND cto.org_tau_st_codigo = DECODE( :v_emitir_por, 'E', :v_tau, cto.org_tau_st_codigo)

    AND cto.org_in_codigo = :v_cod
    AND cto.cto_in_codigo = :v_cto

    AND pro.pro_pad_in_codigo = DECODE( :v_emitir_por, 'P', :v_pad, pro.pro_pad_in_codigo)
    AND pro.pro_st_extenso    = DECODE( :v_emitir_por, 'P', :v_proj, pro.pro_st_extenso)

    AND DECODE( :v_emitir_por, 'E', DECODE(NVL(est.emp_in_codigo, 0), DECODE( :v_cod_emp, 0, NVL(est.emp_in_codigo, 0), :v_cod_emp), 1
                                                                   , 0, 1
                                                                   , 0 )
                            , 'P', 1 ) = 1

    AND DECODE( :v_emitir_por, 'E', DECODE(NVL(est.blo_in_codigo, 0), DECODE( 0, 0, NVL(est.blo_in_codigo, 0), 0), 1
                                                                   , 0, 1
                                                                   , 0 )
                            , 'P', 1 ) = 1

    AND DECODE( :v_emitir_por, 'E', DECODE(NVL(est.und_in_codigo, 0), DECODE( 0, 0, NVL(est.und_in_codigo, 0), 0), 1
                                                                   , 0 , 1
                                                                   , 0 )
                            , 'P', 1 ) = 1

    AND NVL( mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc@life( est.org_tab_in_codigo
                                                              , est.org_pad_in_codigo
                                                              , est.org_in_codigo
                                                              , est.org_tau_st_codigo
                                                              , est.emp_in_codigo
                                                              , :v_sinc
                                                              ), 0) = NVL( est.emp_in_codigo, 0)

    AND est.ctoenv_ch_origem IN ( :v_cto_unid, :v_cto_gara, :v_cto_bens)

    AND cla.csf_in_codigo = DECODE( :v_csf_cto, 0, cla.csf_in_codigo, :v_csf_cto)

    AND mgrel.pck_rel_fnc.fnc_car_status_cto_data@life( cto.org_tab_in_codigo
                                                 , cto.org_pad_in_codigo
                                                 , cto.org_in_codigo
                                                 , cto.org_tau_st_codigo
                                                 , cto.cto_in_codigo
                                                 , TRUNC(DECODE( :v_cons_sts_atual, 'N', :v_dt_fim, SYSDATE))
                                                 , 'N'
                                                 , 'S') IN ( :v_cto_ativ, :v_cto_inad, :v_cto_dist, :v_cto_tran, :v_cto_cesd, :v_cto_quit)

    AND cto.cto_ch_tipo IN ( :v_cto_venda, :v_cto_permuta, :v_cto_aluguel)

    AND DECODE( :v_tp_dt_cto, 'C', cto.cto_dt_cadastro, cto.cto_dt_assinatura) >= :v_dt_assi
    AND DECODE( :v_tp_dt_cto, 'C', cto.cto_dt_cadastro, cto.cto_dt_assinatura) <= :v_dt_assf

    AND est.cto_org_tab_in_codigo = cto.org_tab_in_codigo
    AND est.cto_org_pad_in_codigo = cto.org_pad_in_codigo
    AND est.cto_org_in_codigo     = cto.org_in_codigo
    AND est.cto_org_tau_st_codigo = cto.org_tau_st_codigo
    AND est.cto_in_codigo         = cto.cto_in_codigo

    AND ccl.org_tab_in_codigo = cto.org_tab_in_codigo
    AND ccl.org_pad_in_codigo = cto.org_pad_in_codigo
    AND ccl.org_in_codigo     = cto.org_in_codigo
    AND ccl.org_tau_st_codigo = cto.org_tau_st_codigo
    AND ccl.cto_in_codigo     = cto.cto_in_codigo

    AND cto.org_tab_in_codigo = age.org_tab_in_codigo
    AND cto.org_pad_in_codigo = age.org_pad_in_codigo
    AND cto.org_in_codigo     = age.org_in_codigo
    AND cto.org_tau_st_codigo = age.org_tau_st_codigo
    AND cto.cto_in_codigo     = age.cto_in_codigo

    AND cto.org_tab_in_codigo = aif.agn_tab_in_codigo
    AND cto.org_pad_in_codigo = aif.agn_pad_in_codigo
    AND cto.fil_in_codigo     = aif.agn_in_codigo
    AND cto.org_tau_st_codigo = aif.agn_tau_st_codigo

    AND aif.agn_tab_in_codigo = agf.agn_tab_in_codigo
    AND aif.agn_pad_in_codigo = agf.agn_pad_in_codigo
    AND aif.agn_in_codigo     = agf.agn_in_codigo

    AND pro.pro_tab_in_codigo = cto.pro_tab_in_codigo
    AND pro.pro_pad_in_codigo = cto.pro_pad_in_codigo
    AND pro.pro_ide_st_codigo = cto.pro_ide_st_codigo
    AND pro.pro_in_reduzido   = cto.pro_in_reduzido

    AND ccc.cus_tab_in_codigo = cto.cus_tab_in_codigo
    AND ccc.cus_pad_in_codigo = cto.cus_pad_in_codigo
    AND ccc.cus_ide_st_codigo = cto.cus_ide_st_codigo
    AND ccc.cus_in_reduzido   = cto.cus_in_reduzido

    AND cla.csf_in_codigo = cto.csf_in_codigo

    AND ent.org_tab_in_codigo (+)= est.org_tab_in_codigo
    AND ent.org_pad_in_codigo (+)= est.org_pad_in_codigo
    AND ent.org_in_codigo     (+)= est.org_in_codigo
    AND ent.org_tau_st_codigo (+)= est.org_tau_st_codigo
    AND ent.est_in_codigo     (+)= est.blo_in_codigo

    AND ((ent.ent_dt_cadastro = (SELECT MAX(eob.ent_dt_cadastro)
                                 FROM mgdbm.dbm_entrega_obra@life eob
                                 WHERE eob.org_tab_in_codigo = ent.org_tab_in_codigo
                                   AND eob.org_pad_in_codigo = ent.org_pad_in_codigo
                                   AND eob.org_in_codigo     = ent.org_in_codigo
                                   AND eob.org_tau_st_codigo = ent.org_tau_st_codigo
                                   AND eob.est_in_codigo     = ent.est_in_codigo))
         OR (ent.ent_dt_cadastro IS NULL))

    AND pco.org_tab_in_codigo (+)= est.org_tab_in_codigo
    AND pco.org_pad_in_codigo (+)= est.org_pad_in_codigo
    AND pco.org_in_codigo     (+)= est.fil_in_codigo
    AND pco.org_tau_st_codigo (+)= est.org_tau_st_codigo