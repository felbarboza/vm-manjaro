import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt

query = '''SELECT cto.org_tab_in_codigo
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
'''
cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
con = cx_Oracle.connect("life", "lli8339e", "megacloud")
cursor = con.cursor()

v_data_fim = '26-01-2021'
v_data_ini = '20-01-2020'
v_dt_assi= '01-01-2000'
v_dt_assf= '01-01-2100'

data_base_query = '''SELECT to_date(:v_dt_base, 'dd/mm/yyyy')
                        FROM dual'''

v_dt_fim = cursor.execute(data_base_query, v_dt_base=v_data_fim).fetchall()[0][0]
v_dt_ini = cursor.execute(data_base_query, v_dt_base=v_data_ini).fetchall()[0][0]
v_dt_assi = cursor.execute(data_base_query, v_dt_base=v_dt_assi).fetchall()[0][0]
v_dt_assf = cursor.execute(data_base_query, v_dt_base=v_dt_assf).fetchall()[0][0]
print(v_dt_fim, v_dt_ini)


cursor_params = { 'v_dt_fim': v_dt_fim,
'v_data_fim': v_data_fim,
'v_tab': 53,
'v_pad': 1,
'v_cod': 55,
'v_tau': 'G',
'v_cto': 2250,
'v_cons_cessao': 'N',
'v_dt_ini': v_dt_ini,
'v_emitir_por': 'E',
'v_proj': 0,
'v_cod_emp': 2172,
'v_sinc': 'T',
'v_cto_unid':'U',
'v_cto_gara':'G',
'v_cto_bens': 'B',
'v_csf_cto': 0,
'v_cons_sts_atual':'N',
'v_cto_ativ': 'A',
'v_cto_inad': 'U',
'v_cto_dist': 'D',
'v_cto_tran': 'X',
'v_cto_cesd': 'X',
'v_cto_quit': 'Q',
'v_cto_venda': 'V',
'v_cto_permuta': 'P',
'v_cto_aluguel': 'M',
'v_tp_dt_cto': 'C' ,
'v_dt_assi': v_dt_assi,
'v_dt_assf': v_dt_assf }

cursor_contratos = pd.read_sql(query, con, params=cursor_params)
cursor_contratos.to_excel('./testeCursorBoletim.xlsx')
# v_dt_fim
# v_data_fim
# v_tab
# v_pad
# v_cod
# v_tau
# v_cto
# v_cons_cessao = 'S'
# v_dt_ini
# v_emitir_por = 'E'
# v_proj = 0
# v_cod_emp
# v_sinc = 'S' sim 'N' nao 'T' todos - tem q ver
# v_cto_unid ='U' ou 'X'
# v_cto_gara ='G' ou 'X'
# v_cto_bens = 'B' ou 'X'
# v_csf_cto = 0
# v_cons_sts_atual se for 'N' pega a data_fim senao pega a data de hoje, tem q ver
# v_cto_ativ = 'A'
# v_cto_inad = 'U'
# v_cto_dist = 'D'
# v_cto_tran = 'X'
# v_cto_cesd = 'X'
# v_cto_quit = 'Q'
# v_cto_venda = 'V'
# v_cto_permuta = 'P'
# v_cto_aluguel = 'M'
# v_tp_dt_cto = 'C' para data de cadastro ou 'X' para data de assinatura
# v_dt_assi = periodo inicial da data de assinatura do contrato
# v_dt_assf = periodo final de data de assinatura do contrato