SELECT cto.org_tab_in_codigo
       , cto.org_pad_in_codigo
       , cto.org_in_codigo
       , cto.org_tau_st_codigo
       , cto.cto_in_codigo
       , cto.fil_in_codigo
       , agf.agn_st_nome fil_st_nome
       , mgrel.pck_rel_fnc.fnc_car_status_cto_data@life( cto.org_tab_in_codigo
                                                  , cto.org_pad_in_codigo
                                                  , cto.org_in_codigo
                                                  , cto.org_tau_st_codigo
                                                  , cto.cto_in_codigo
                                                  , :v_dt_fim
                                                  , 'N'
                                                  , 'S') cto_ds_status
       , cto.cto_ch_classificacao
       , cto.cto_dt_classificacao
       , cto.cto_dt_cadastro
       , est.emp_in_codigo
       , est.emp_st_codigo
       , est.emp_st_nome
       , est.blo_in_codigo
       , est.blo_st_codigo
       , est.blo_st_nome
       , est.und_in_codigo
       , est.und_st_codigo
       , DECODE(NVL(est.und_bo_consinvestidor, 'N'), 'S', est.und_in_codigo, est.blo_in_codigo) und_blo_codigo
       , est.ctoenv_ch_origem
       --, aid.agn_st_codigoalt
       , age.tipo
       , DECODE(est.estrutura, 'B', age.agn_st_nome || ' - ' || est.emp_st_nome, age.agn_st_nome) agn_st_nome
       , age.agn_in_codigo
       , age.cto_dt_ini
       , NVL(age.cto_dt_fim, :v_data_fim) cto_dt_fim
       , ent.ent_dt_entrega
       , pro.pro_pad_in_codigo
       , pro.pro_tab_in_codigo
       , pro.pro_ide_st_codigo
       , pro.pro_in_reduzido
       , pro.pro_st_extenso
       , pro.pro_st_apelido
       , pro.pro_st_descricao
       , DECODE( pco.emp_ch_referencia, 'P', pro.pro_st_descricao, ccc.cus_st_descricao) proj_cc
       , ccc.cus_st_extenso
       , cla.csf_in_codigo
       , cla.csf_st_descricao
       , NVL(pco.pco_bo_jrotpsaldovo, 'N') pco_bo_jrotpsaldovo

  FROM mgdbm.dbm_parametro_contabilidade@life pco
     , mgdbm.dbm_classificacao@life           cla
     , mgcon.con_centro_custo@life            ccc
     , mgglo.glo_projetos@life                pro
     , mgdbm.dbm_entrega_obra@life            ent
     , mgglo.glo_agentes@life                 agf
     , mgglo.glo_agentes_id@life              aif
     , (SELECT cto.org_tab_in_codigo
             , cto.org_pad_in_codigo
             , cto.org_in_codigo
             , cto.org_tau_st_codigo
             , cto.cto_in_codigo
             , agg.agn_st_nome
             , agg.agn_in_codigo
             , ctt.cto_dt_assinatura
             , ctt.ctr_in_codigo
             , ctt.ctr_dt_cadastro
             , 'C'                  tipo -- Clientes de cessão de direitos
             , NVL((SELECT cc.ctr_dt_processo
                      FROM mgcar.car_cliente_transferido@life cc
                     WHERE cc.ctr_in_codigo = (SELECT MAX(ctr.ctr_in_codigo)
                                                 FROM mgcar.car_cliente_transferido@life ctr
                                                WHERE ctr.org_tab_in_codigo = ctt.org_tab_in_codigo
                                                  AND ctr.org_pad_in_codigo = ctt.org_pad_in_codigo
                                                  AND ctr.org_in_codigo     = ctt.org_in_codigo
                                                  AND ctr.org_tau_st_codigo = ctt.org_tau_st_codigo
                                                  AND ctr.cto_in_codigo     = ctt.cto_in_codigo
                                                  AND ctr.ctr_in_codigo     < ctt.ctr_in_codigo)
                       AND cc.cto_in_codigo = ctt.cto_in_codigo), cto.cto_dt_cadastro ) cto_dt_ini
               , (ctt.ctr_dt_processo) -1 cto_dt_fim
            FROM mgglo.glo_agentes@life             agg
             , mgglo.glo_agentes_id@life          aid
             , mgcar.car_cliente_transferido@life ctt
             , mgcar.car_contrato@life            cto
            WHERE cto.org_tab_in_codigo = :v_tab
            AND cto.org_pad_in_codigo = :v_pad
            AND cto.org_in_codigo     = :v_cod
            AND cto.org_tau_st_codigo = :v_tau
            AND cto.cto_in_codigo     = :v_cto

            AND :v_cons_cessao = 'S' -- Considero clientes de cessão de direitos

            AND cto.org_tab_in_codigo = ctt.org_tab_in_codigo
            AND cto.org_pad_in_codigo = ctt.org_pad_in_codigo
            AND cto.org_in_codigo     = ctt.org_in_codigo
            AND cto.org_tau_st_codigo = ctt.org_tau_st_codigo
            AND cto.cto_in_codigo     = ctt.cto_in_codigo

            AND ctt.agn_tab_in_codigo = aid.agn_tab_in_codigo
            AND ctt.agn_pad_in_codigo = aid.agn_pad_in_codigo
            AND ctt.agn_in_codigo     = aid.agn_in_codigo
            AND ctt.agn_tau_st_codigo = aid.agn_tau_st_codigo

            AND agg.agn_tab_in_codigo = aid.agn_tab_in_codigo
            AND agg.agn_pad_in_codigo = aid.agn_pad_in_codigo
            AND agg.agn_in_codigo     = aid.agn_in_codigo
            UNION ALL
            SELECT cto.org_tab_in_codigo
              , cto.org_pad_in_codigo
              , cto.org_in_codigo
              , cto.org_tau_st_codigo
              , cto.cto_in_codigo
              , aag.agn_st_nome
              , aag.agn_in_codigo
              , cto.cto_dt_assinatura
              , NULL ctr_in_codigo
              , NULL ctr_dt_cadastro
              , 'A'                    tipo
              , NVL((SELECT max(ctr.ctr_dt_processo)
                   FROM mgcar.car_cliente_transferido@life ctr
                  WHERE ctr.org_tab_in_codigo = cte.org_tab_in_codigo
                    AND ctr.org_pad_in_codigo = cte.org_pad_in_codigo
                    AND ctr.org_in_codigo     = cte.org_in_codigo
                    AND ctr.org_tau_st_codigo = cte.org_tau_st_codigo
                    AND ctr.cto_in_codigo     = cte.cto_in_codigo), :v_dt_ini) cto_dt_ini
              , :v_dt_fim cto_dt_fim
            FROM mgglo.glo_agentes@life          aag
             , mgglo.glo_agentes_id@life       ida
             , mgcar.car_contrato_cliente@life cte
             , mgcar.car_contrato@life         cto
            WHERE cto.org_tab_in_codigo = :v_tab
            AND cto.org_pad_in_codigo = :v_pad
            AND cto.org_in_codigo     = :v_cod
            AND cto.org_tau_st_codigo = :v_tau
            AND cto.cto_in_codigo     = :v_cto

            AND cto.org_tab_in_codigo = cte.org_tab_in_codigo
            AND cto.org_pad_in_codigo = cte.org_pad_in_codigo
            AND cto.org_in_codigo     = cte.org_in_codigo
            AND cto.org_tau_st_codigo = cte.org_tau_st_codigo
            AND cto.cto_in_codigo     = cte.cto_in_codigo

            AND cte.agn_tab_in_codigo = ida.agn_tab_in_codigo
            AND cte.agn_pad_in_codigo = ida.agn_pad_in_codigo
            AND cte.agn_in_codigo     = ida.agn_in_codigo
            AND cte.agn_tau_st_codigo = ida.agn_tau_st_codigo

            AND aag.agn_tab_in_codigo = ida.agn_tab_in_codigo
            AND aag.agn_pad_in_codigo = ida.agn_pad_in_codigo
            AND aag.agn_in_codigo     = ida.agn_in_codigo) age

     , mgcar.car_contrato_cliente@life        ccl
     , mgrel.vw_car_estrutura@life            est
     , mgcar.car_contrato@life                cto