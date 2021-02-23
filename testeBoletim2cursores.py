import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt

def gera_boletim_recebimento(fil_in_codigo, emp_in_codigo, data_fim, data_ini):

  print('--Extraindo Boletim de recebimento--')
  time_inicio = dt.datetime.today()

  query_inv=''' SELECT est.org_tab_in_codigo
        , est.org_pad_in_codigo
        , est.org_in_codigo
        , est.org_tau_st_codigo
        , agn.agn_st_nome   inv_st_nome
        , 'I' tipo
        , agn.agn_in_codigo inv_in_codigo
        , est.cto_in_codigo
        , agn.agn_ch_tipopessoafj org_ch_tipopessoafj
        , DECODE( agn.agn_ch_tipopessoafj, 'F', pfi.agn_st_cpf
                                          , 'J', agn.agn_st_cgc) org_st_cgc

        , TRIM(agn.tpl_st_sigla || ' ' || agn.agn_st_logradouro || ', ' || agn.agn_st_numero ||
              ', ' || agn.agn_st_complemento) org_st_endereco
        , TRIM(agn.agn_st_municipio || ' - ' || agn.agn_st_cep || ' - ' || agn.uf_st_sigla)   org_st_cidade
        , agn.agn_in_codigo

    FROM mgglo.glo_pessoa_fisica@life pfi
      , mgglo.glo_agentes@life       agn
      , mgglo.glo_agentes_id@life    aid
      , mgdbm.dbm_investidor@life    inv
      , (SELECT :tab_in_codigo  tab_in_codigo
              , :pad_in_codigo  pad_in_codigo
              , DECODE(agn.agn_bo_consolidador, 'E', agn.agn_in_codigo, agn.pai_agn_in_codigo) org_in_codigo
              , agn.agn_in_codigo fil_in_codigo

          FROM mgglo.glo_agentes@life      agn
          WHERE  agn.agn_tab_in_codigo = :tab_in_codigo
            AND agn.agn_pad_in_codigo = :pad_in_codigo
            AND agn.agn_in_codigo     = :fil_in_codigo
            AND agn.agn_bo_consolidador IN ('E', 'F')) ati
      , mgrel.vw_car_estrutura@life  est
      -- Filial Ativa

    WHERE est.org_tab_in_codigo = DECODE( :v_emitir_por, 'E', :v_tab, est.org_tab_in_codigo)
      AND est.org_pad_in_codigo = DECODE( :v_emitir_por, 'E', :v_pad, est.org_pad_in_codigo)
      AND est.org_tau_st_codigo = DECODE( :v_emitir_por, 'E', :v_tau, est.org_tau_st_codigo)
      AND est.cto_in_codigo = DECODE( :v_cod, 0, est.cto_in_codigo, :v_cod)
      
      AND mgrel.pck_rel_fnc.fnc_car_status_cto_data@life( est.org_tab_in_codigo
                                                  , est.org_pad_in_codigo
                                                  , est.org_in_codigo
                                                  , est.org_tau_st_codigo
                                                  , est.cto_in_codigo
                                                  , TRUNC(DECODE( :v_cons_sts_atual, 'N', :v_dt_fim, SYSDATE))
                                                  , 'N'
                                                  , 'S') IN ( :v_cto_ativ, :v_cto_inad, :v_cto_dist, :v_cto_tran, :v_cto_cesd, :v_cto_quit)

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

      AND inv.agn_in_codigo = DECODE( :v_inv, 0, inv.agn_in_codigo, :v_inv)

      AND :v_emitir_inv = 'S'
      AND inv.agn_tau_st_codigo = 'O'
      AND inv.inv_ch_tipoparticipacao = 'I'

      AND est.org_tab_in_codigo = ati.tab_in_codigo
      AND est.org_pad_in_codigo = ati.pad_in_codigo
      AND est.org_in_codigo     = ati.org_in_codigo
      AND est.fil_in_codigo = ati.fil_in_codigo

      AND est.org_tab_in_codigo = inv.org_tab_in_codigo
      AND est.org_pad_in_codigo = inv.org_pad_in_codigo
      AND est.org_in_codigo     = inv.org_in_codigo
      AND est.org_tau_st_codigo = inv.org_tau_st_codigo
      AND DECODE( est.und_bo_consinvestidor, 'S', est.und_in_codigo, est.blo_in_codigo) = inv.est_in_codigo

      AND inv.agn_tab_in_codigo = aid.agn_tab_in_codigo
      AND inv.agn_pad_in_codigo = aid.agn_pad_in_codigo
      AND inv.agn_in_codigo     = aid.agn_in_codigo
      AND inv.agn_tau_st_codigo = aid.agn_tau_st_codigo

      AND aid.agn_tab_in_codigo = agn.agn_tab_in_codigo
      AND aid.agn_pad_in_codigo = agn.agn_pad_in_codigo
      AND aid.agn_in_codigo     = agn.agn_in_codigo

      AND agn.agn_tab_in_codigo = pfi.agn_tab_in_codigo(+)
      AND agn.agn_pad_in_codigo = pfi.agn_pad_in_codigo(+)
      AND agn.agn_in_codigo     = pfi.agn_in_codigo    (+)
      AND pfi.agn_ch_tipo    (+)= 'P'

    UNION ALL

    SELECT cto.org_tab_in_codigo
        , cto.org_pad_in_codigo
        , cto.org_in_codigo
        , cto.org_tau_st_codigo
        , oag.agn_st_nome   inv_st_nome
        , 'F' tipo
        , oag.agn_in_codigo inv_in_codigo
        , cto.cto_in_codigo
        , 'J' org_ch_tipopessoafj
        , oag.agn_st_cgc    org_st_cgc
        , TRIM(oag.tpl_st_sigla || ' ' || oag.agn_st_logradouro || ', ' || oag.agn_st_numero ||
              ', ' || oag.agn_st_complemento) org_st_endereco
        , TRIM(oag.agn_st_municipio || ' - ' || oag.agn_st_cep || ' - ' || oag.uf_st_sigla)   org_st_cidade
        , 0 agn_in_codigo

    FROM mgglo.glo_agentes@life            oag -- CNPJ da filial
      , mgglo.glo_agentes_id@life         oai -- filial
      -- Filial Ativa
      , (SELECT :tab_in_codigo  tab_in_codigo
              , :pad_in_codigo  pad_in_codigo
              , DECODE(agn.agn_bo_consolidador, 'E', agn.agn_in_codigo, agn.pai_agn_in_codigo) org_in_codigo
              , agn.agn_in_codigo fil_in_codigo

          FROM  mgglo.glo_agentes@life      agn
          WHERE agn.agn_tab_in_codigo = :tab_in_codigo
            AND agn.agn_pad_in_codigo = :pad_in_codigo
            AND agn.agn_in_codigo     = :fil_in_codigo
            AND agn.agn_bo_consolidador IN ('E', 'F')) ati
      , mgcar.car_contrato@life           cto

    WHERE cto.org_tab_in_codigo = DECODE( :v_emitir_por, 'E', :v_tab, cto.org_tab_in_codigo)
      AND cto.org_pad_in_codigo = DECODE( :v_emitir_por, 'E', :v_pad, cto.org_pad_in_codigo)
      AND cto.org_tau_st_codigo = DECODE( :v_emitir_por, 'E', :v_tau, cto.org_tau_st_codigo)
      AND cto.cto_in_codigo = DECODE( :v_cod, 0, cto.cto_in_codigo, :v_cod)


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

      AND ( :v_inv = 0 OR
          ( :v_inv <> 0 AND :v_emitir_inv = 'N'))


      AND cto.org_tab_in_codigo = ati.tab_in_codigo
      AND cto.org_pad_in_codigo = ati.pad_in_codigo
      AND cto.org_in_codigo     = ati.org_in_codigo
      AND cto.fil_in_codigo = ati.fil_in_codigo

      AND cto.org_tab_in_codigo = oai.agn_tab_in_codigo
      AND cto.org_pad_in_codigo = oai.agn_pad_in_codigo
      AND cto.fil_in_codigo     = oai.agn_in_codigo
      AND cto.org_tau_st_codigo = oai.agn_tau_st_codigo

      AND oai.agn_tab_in_codigo = oag.agn_tab_in_codigo
      AND oai.agn_pad_in_codigo = oag.agn_pad_in_codigo
      AND oai.agn_in_codigo     = oag.agn_in_codigo'''


  cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
  con = cx_Oracle.connect("life", "lli8339e", "megacloud")
  cursor = con.cursor()
  v_dt_assi= '01-01-2000'
  v_dt_assf= '01-01-2100'

  data_base_query = '''SELECT to_date(:v_dt_base, 'dd/mm/yyyy')
                          FROM dual'''

  v_dt_fim = cursor.execute(data_base_query, v_dt_base=data_fim).fetchall()[0][0]
  v_dt_ini = cursor.execute(data_base_query, v_dt_base=data_ini).fetchall()[0][0]
  v_dt_assi = cursor.execute(data_base_query, v_dt_base=v_dt_assi).fetchall()[0][0]
  v_dt_assf = cursor.execute(data_base_query, v_dt_base=v_dt_assf).fetchall()[0][0]

  cursor_inv_params = { 'v_dt_fim': v_dt_fim,
  'v_tab': 53,
  'v_pad': 1,
  'v_cod': 0,
  'v_tau': 'G',
  'v_inv' :0,
  'tab_in_codigo': 53,
  'pad_in_codigo': 1,
  'fil_in_codigo': fil_in_codigo,
  'v_emitir_inv':'S',
  'v_emitir_por': 'E',
  'v_cod_emp': emp_in_codigo,
  'v_sinc': 'T',
  'v_cto_unid':'U',
  'v_cto_gara':'G',
  'v_cto_bens': 'B',
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

  cursor_inv = pd.read_sql(query_inv, con, params=cursor_inv_params)

  query_contrato = '''SELECT cto.org_tab_in_codigo
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
  contratoDf = pd.DataFrame()

  for linha in cursor_inv.index:
    contrato = cursor_inv.at[linha, 'CTO_IN_CODIGO'].item()
    cursor_contrato_params = { 'v_dt_fim': v_dt_fim,
    'v_data_fim': data_fim,
    'v_tab': 53,
    'v_pad': 1,
    'v_cod': fil_in_codigo-1,
    'v_tau': 'G',
    'v_cto': contrato,
    'v_cons_cessao': 'N',
    'v_dt_ini': v_dt_ini,
    'v_emitir_por': 'E',
    'v_proj': 0,
    'v_cod_emp': emp_in_codigo,
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

    temp = pd.read_sql(query_contrato, con, params=cursor_contrato_params)

    if(linha==0):
      contratoDf = temp
    else:
      contratoDf = pd.concat([contratoDf, temp])
  contratoDf = contratoDf.reset_index(drop=True)
  contratoDf.to_excel('./testeCursorContrato.xlsx')
  query_parcelas = '''SELECT DISTINCT par.org_tab_in_codigo
                , par.org_pad_in_codigo
                , par.org_in_codigo
                , par.org_tau_st_codigo
                , par.cto_in_codigo
                , par.par_in_codigo
                , par.par_dt_vencimento
                , par.par_dt_movimento
                , par.par_dt_realizacaobx
                , par.par_ch_status
                , ROUND(NVL(par.par_re_valororiginal, 0), 2) par_re_valororiginal
                , ROUND(NVL(par.par_re_valorpago, 0), 2) par_re_valorpago
                , ROUND(NVL(mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                        , par.org_pad_in_codigo
                                                                        , par.org_in_codigo
                                                                        , par.org_tau_st_codigo
                                                                        , par.cto_in_codigo
                                                                        , par.par_in_codigo), 0), 2)par_re_valortaxas
                , ROUND(NVL(par.par_re_valormulta, 0), 2)                                           par_re_valormulta
                , ROUND(NVL(par.par_re_valoratraso, 0), 2)                                          par_re_valoratraso
                , ROUND(NVL(par.par_re_valorjuros, 0), 2)                                           par_re_valorjuros
                , ROUND(NVL(par.par_re_valorjurosbx, 0), 2)                                         par_re_valorjurosbx
                , ROUND(NVL(par.par_re_valorcorrecao, 0), 2)                                        par_re_valorcorrecao
                , ROUND((NVL(par.par_re_valorcorrecao, 0) + NVL(par.par_re_valorcorrecaobx, 0)), 2) vl_corrigido
                , DECODE(SIGN(par.par_dt_baixa - to_date(:v_ent_dt_entrega, 'dd/mm/yyyy')), 1, ROUND(NVL(mgcar.pck_car_fnc.fnc_car_valorcorrecao@life( par.org_tab_in_codigo
                                                                                                                          , par.org_pad_in_codigo
                                                                                                                          , par.org_in_codigo
                                                                                                                          , par.org_tau_st_codigo
                                                                                                                          , par.cto_in_codigo
                                                                                                                          , par.par_in_codigo
                                                                                                                          , to_date(:v_ent_dt_entrega, 'dd/mm/yyyy')
                                                                                                                          , 'RP'
                                                                                                                          , 'A'
                                                                                                                          , -1
                                                                                                                          , 'S'), 0), 2)
                                                                       , ROUND(( NVL(par.par_re_valorcorrecao, 0) + NVL(par.par_re_valorcorrecaobx, 0)), 2)) vmpago_antes_entrega
                , ROUND((NVL(par.par_re_valorjuros, 0) + NVL(par.par_re_valorjurosbx, 0)), 2) vl_juro
                , ROUND(NVL(par.par_re_valorjurosren, 0), 2)                                  par_re_valorjurosren
                , ROUND((NVL(par.par_re_valorpago, 0)
                                                     + NVL(par.par_re_valormulta, 0)
                                                     + NVL(par.par_re_valoratraso, 0)
                                                     + NVL(par.par_re_residuocobranca, 0)
                                                     - NVL(par.par_re_valordesconto, 0)
                                                     + NVL(par.par_re_valorcorrecao_atr, 0)
                                                     + NVL(mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                                                       , par.org_pad_in_codigo
                                                                                                       , par.org_in_codigo
                                                                                                       , par.org_tau_st_codigo
                                                                                                       , par.cto_in_codigo
                                                                                                       , par.par_in_codigo), 0)), 2) vl_pago
                , ROUND(NVL(par.par_re_valorcorrecaobx, 0), 2) par_re_valorcorrecaobx
                , ROUND(NVL(par.par_re_valordesconto, 0), 2)   par_re_valordesconto
                , DECODE ( mgcar.pck_car_fnc.fnc_car_paiorigem@life( par.org_tab_in_codigo
                                                              , par.org_pad_in_codigo
                                                              , par.org_in_codigo
                                                              , par.org_tau_st_codigo
                                                              , par.cto_in_codigo
                                                              , par.par_in_codigo) , 'T', ROUND( NVL(par.par_re_valordesconto, 0) - ((NVL(par.par_re_jrotpncobrado, 0) + NVL(par.par_re_vmjrotpncob, 0))), 2)
                                                                                   , 'S', ROUND( NVL(par.par_re_valordesconto, 0) - ((NVL(par.par_re_jrotpncobrado, 0) + NVL(par.par_re_vmjrotpncob, 0))), 2)
                                                                                        , ROUND(( NVL(par.par_re_valordesconto, 0) + DECODE((SIGN(NVL(par.par_re_valorjurosren, 0))), -1, NVL((par.par_re_valorjurosren * -1), 0), 0)), 2)) par_re_descontosemantecip
                ,  DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life( par.org_tab_in_codigo
                                                              , par.org_pad_in_codigo
                                                              , par.org_in_codigo
                                                              , par.org_tau_st_codigo
                                                              , par.cto_in_codigo
                                                              , par.par_in_codigo) , 'T', DECODE(:v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_jrotpncobrado, 0), 2)), ROUND(NVL(par.par_re_jrotpncobrado, 0), 2))
                                                                                   , 'S', DECODE(:v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_jrotpncobrado, 0), 2)), ROUND(NVL(par.par_re_jrotpncobrado, 0), 2))
                                                                                        , 0) par_re_descontoantecip
                , ROUND(NVL(par.par_re_residuocobranca, 0), 2) par_re_residuocobranca
                , ROUND(NVL(par.par_re_jrotpncobrado, 0), 2) par_re_jrotpncobrado
                , par.par_dt_baixa
                , par.par_bo_contratual
                , par.par_ch_parcela
                , par.par_bo_confdivida
                , par.par_st_agencia
                , par.par_ch_receitabaixa
                , DECODE(par.par_ch_receitabaixa, 'C', 3
                                                , 'D', 11
                                                , 'E', 7
                                                , 'K', 4
                                                , 'T', 5
                                                , 'B', 1
                                                , 'P', 2
                                                , 'H', 4
                                                , 'O', 5
                                                , 'A', 6
                                                , 'S', 8
                                                , 'R', 12
                                                , par.par_ch_receitabaixa) ord_ch_receita
                ,  DECODE(par.par_ch_receitabaixa, 'C', 'Cheque'
                                                 , 'D', 'Caixa Geral'
                                                 , 'E', 'Bens'
                                                 , 'K', 'Carta de Crédito'
                                                 , 'T', 'Permuta'
                                                 , 'B', 'Bl. Bancário'
                                                 , 'P', 'Depósito'
                                                 , 'H', 'TED'
                                                 , 'O', 'DOC'
                                                 , 'A', 'Cobrança Bancária'
                                                 , 'S', 'Securitização'
                                                 , 'R', 'Repasse'
                                                 , par.par_ch_receitabaixa) desc_con_receita
                , DECODE(par.par_ch_receitabaixa, 'C', 'Cheque'
                                                , 'D', 'Dinheiro'
                                                , 'E', 'Bens'
                                                , 'K', 'Carta de Crédito'
                                                , 'T', 'Permuta'
                                                , 'B', 'Banco'
                                                , 'P', 'Depósito - Banco'
                                                , 'H', 'TED'
                                                , 'O', 'DOC'
                                                , 'A', 'Boleto Avulso'
                                                , 'S', 'Securitização'
                                                , 'R', 'Repasse'
                                                , par.par_ch_receitabaixa) desc_ch_receitabaixa
                , DECODE(par.par_ch_receitabaixa ,'B', bol.ban_in_numero
                                                 , ban.ban_in_numero) ban_in_numero
                , DECODE(par.par_ch_receitabaixa ,'B', bol.ban_st_nome
                                                 , ban.ban_st_nome) ban_st_nome
                , DECODE(par.par_ch_receitabaixa ,'B', bol.conta
                                                 , par.par_st_conta) par_st_conta
                , ROUND(NVL(par.par_re_credito, 0), 2) par_re_credito
                , cau.ctc_in_codigo
                , ROUND(DECODE(tpb.tpr_re_abatido, NULL, NVL(par.par_re_valororiginal, 0), NVL(tpb.tpr_re_abatido, 0)), 2) tpr_re_abatido

                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo) , 'S', ROUND(NVL((par.par_re_valororiginal - par.par_re_vlroriginalsac), 0), 2)
                                                                                  , 'T', ROUND(NVL(tpb.tpr_re_jurostp, 0), 2)
                                                                                       , 0) tpr_re_jurostp

                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo) , 'T', DECODE(:v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_vmjrotpncob, 0), 2)), ROUND(NVL(par.par_re_vmjrotpncob, 0), 2))
                                                                                  , 'S', DECODE(:v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_vmjrotpncob, 0), 2)), ROUND(NVL(par.par_re_vmjrotpncob, 0), 2))
                                                                                       , 0) ant_re_vmjrotpncob
                , ROUND(NVL(par.par_re_valorcorrecao_atr,0), 2) par_re_valorcorrecao_atr
                , bol.agencia agencia_con
                , obs.par_st_observacao
                , cob.hcob_st_descricao
                , ter.tte_in_codigo
                , ter.ctt_ch_tipo
                , tpt.tte_st_descricao
                , tpt.tte_ch_tipo
                , cta.agn_in_codigo cta_in_codigo
                , cta.agn_st_nome   cta_st_nome
                , DECODE(sign(trunc(SYSDATE) - NVL(par.par_dt_deposito, par.par_dt_baixa)), -1, 'F', DECODE(:v_contrl_cheque, 'S', DECODE(par.par_ch_status, 'D', 'I'
                                                                                                                                                          , 'P', 'I'
                                                                                                                                                          , '1', 'I'
                                                                                                                                                          , 'U', 'I'
                                                                                                                                                          , 'N')
                                                                                                                            , 'N')) identificador

                , SUBSTR( mgcar.pck_car_fnc.fnc_car_origemparcela@life( par.org_tab_in_codigo
                                                                 , par.org_pad_in_codigo
                                                                 , par.org_in_codigo
                                                                 , par.org_tau_st_codigo
                                                                 , par.cto_in_codigo
                                                                 , par.par_in_codigo
                                                                 , par.par_ch_origem
                                                                 , par.cnd_in_codigo
                                                                 , par.par_ch_amortizacao
                                                                 , 1), 1, 50) operacao
                , NVL ( cpl.pla_re_jrotp, 0 ) pla_re_jrotp

    FROM ( SELECT pla.org_tab_in_codigo
              , pla.org_pad_in_codigo
              , pla.org_in_codigo
              , pla.org_tau_st_codigo
              , pla.cto_in_codigo
              , pla.par_in_codigo
              , SUM(NVL(pla.pla_re_jrotp,0)) pla_re_jrotp
         FROM mgcar.car_contrato_planilha@life pla
         WHERE trunc(pla.pla_dt_movimento) <= :v_dt_fim
         GROUP BY pla.org_tab_in_codigo
                , pla.org_pad_in_codigo
                , pla.org_in_codigo
                , pla.org_tau_st_codigo
                , pla.cto_in_codigo
                , pla.par_in_codigo )                          cpl
     , mgglo.glo_agentes@life                                       cta
     , mgcar.car_integra_movimento@life                             itm
     , mgcar.car_movimento_rateio@life                              mra
     , ( SELECT mvv.org_tab_in_codigo
              , mvv.org_pad_in_codigo
              , mvv.org_in_codigo
              , mvv.org_tau_st_codigo
              , mvv.cto_in_codigo
              , mvv.par_in_codigo
              , MIN(mvv.mov_in_codigo)	mov_in_codigo
              , MIN(mvv.mra_in_codigo)  mra_in_codigo
         FROM mgcar.car_movimento_parcela@life mvv
            , mgcar.car_movimento_rateio@life  maa
            , mgcar.car_integra_movimento@life imm
         WHERE (mvv.mpa_bo_ativa     = 'S' OR mvv.mpa_bo_ativa IS NULL)
           AND mvv.org_tab_in_codigo = maa.org_tab_in_codigo
           AND mvv.org_pad_in_codigo = maa.org_pad_in_codigo
           AND mvv.org_in_codigo     = maa.org_in_codigo
           AND mvv.org_tau_st_codigo = maa.org_tau_st_codigo
           AND mvv.mov_in_codigo     = maa.mov_in_codigo
           AND mvv.mra_in_codigo     = maa.mra_in_codigo

           AND maa.org_tab_in_codigo = imm.org_tab_in_codigo
           AND maa.org_pad_in_codigo = imm.org_pad_in_codigo
           AND maa.org_in_codigo     = imm.org_in_codigo
           AND maa.org_tau_st_codigo = imm.org_tau_st_codigo
           AND maa.mov_in_codigo     = imm.mov_in_codigo
         GROUP BY mvv.org_tab_in_codigo
                , mvv.org_pad_in_codigo
                , mvv.org_in_codigo
                , mvv.org_tau_st_codigo
                , mvv.cto_in_codigo
                , mvv.par_in_codigo)                           mov
     , mgcar.car_tipo_termo@life                                    tpt
     , mgcar.car_contrato_termo@life                                ter
     , mgfin.fin_hbkcobranca@life                                   cob
     , mgcar.car_parcela_destino@life                               des
     , ( SELECT apa.org_tab_in_codigo
              , apa.org_pad_in_codigo
              , apa.org_in_codigo
              , apa.org_tau_st_codigo
              , apa.cto_in_codigo
              , apa.par_in_codigo
              , atp.ant_re_bonificacao
              , atp.ant_in_parcelas
              , atp.ant_re_jrotpncobrado
        FROM mgcar.car_antecipacao@life         atp
           , mgcar.car_antecipacao_parcela@life apa
        WHERE NVL(atp.ant_ch_status, 'A') = 'A'
          AND atp.org_tab_in_codigo       = apa.org_tab_in_codigo
          AND atp.org_pad_in_codigo       = apa.org_pad_in_codigo
          AND atp.org_in_codigo           = apa.org_in_codigo
          AND atp.org_tau_st_codigo       = apa.org_tau_st_codigo
          AND atp.cto_in_codigo           = apa.cto_in_codigo
          AND atp.ant_in_codigo           = apa.ant_in_codigo) ant
     , mgcar.car_caucao_parcela@life                                cau
     , mgcar.car_tabelaprice_baixa@life                             tpb
     , mgglo.glo_banco@life                                         ban
     , mgcar.car_parcela_observacao@life                            obs
     , (SELECT pde.org_tab_in_codigo
             , pde.org_pad_in_codigo
             , pde.org_in_codigo
             , pde.org_tau_st_codigo
             , pde.cto_in_codigo
             , pde.par_in_codigo
             , cta.age_in_codigo  agencia
             , cta.cta_st_numero  conta
             , ban.ban_st_nome
             , ban.ban_in_numero

         FROM mgglo.glo_contasfin@life            cta
            , mgfin.fin_hbkcontrato@life          hco
            , mgcar.car_movimento_financeiro@life mfi
            , mgcar.car_documento_financeiro@life dfi
            , mgglo.glo_banco@life                ban
            , mgcar.car_parcela_destino@life      pde
            , mgcar.car_parcela@life              par

        WHERE par.org_tab_in_codigo = pde.org_tab_in_codigo
          AND par.org_pad_in_codigo = pde.org_pad_in_codigo
          AND par.org_in_codigo     = pde.org_in_codigo
          AND par.org_tau_st_codigo = pde.org_tau_st_codigo
          AND par.cto_in_codigo     = pde.cto_in_codigo
          AND par.par_in_codigo     = pde.par_in_codigo

          AND dfi.org_tab_in_codigo = par.org_tab_in_codigo
          AND dfi.org_pad_in_codigo = par.org_pad_in_codigo
          AND dfi.org_in_codigo     = par.org_in_codigo
          AND dfi.org_tau_st_codigo = par.org_tau_st_codigo
          AND dfi.cto_in_codigo     = par.cto_in_codigo
          AND dfi.par_in_codigo     = par.par_in_codigo

          AND mfi.dfi_in_codigo  (+)= dfi.dfi_in_codigo

          AND hco.agn_tab_in_codigo = cta.agn_tab_in_codigo
          AND hco.agn_pad_in_codigo = cta.agn_pad_in_codigo
          AND hco.agn_in_codigo     = cta.agn_in_codigo

          AND pde.hcon_in_sequencia = hco.hcon_in_sequencia

          AND ban.ban_in_numero     = cta.ban_in_numero) bol
     , mgcar.car_parcela@life                                 par

    WHERE par.org_tab_in_codigo = :v_tab
    AND par.org_pad_in_codigo = :v_pad
    AND par.org_in_codigo     = :v_cod
    AND par.org_tau_st_codigo = :v_tau
    AND par.cto_in_codigo     = :v_cto

    AND DECODE( :v_por_data_baixa, 'B', par.par_dt_baixa
                                , 'M', par.par_dt_movimento
                                , 'R', par.par_dt_realizacaobx
                                     , par.par_dt_vencimento) >= :v_dt_ini

    AND DECODE( :v_por_data_baixa, 'B', par.par_dt_baixa
                                , 'M', par.par_dt_movimento
                                , 'R', par.par_dt_realizacaobx
                                     , par.par_dt_vencimento) <= :v_dt_fim

    AND ( (   :v_cons_cessao = 'S' -- Considero as baixas separando-as por cliente (cessão de direito via botão + cliente atual)
            AND DECODE( :v_por_data_baixa, 'B', par.par_dt_baixa
                                        , 'M', par.par_dt_movimento
                                        , 'R', par.par_dt_realizacaobx
                                             , par.par_dt_vencimento) >= :v_cto_dt_ini

            AND DECODE( :v_por_data_baixa, 'B', par.par_dt_baixa
                                        , 'M', par.par_dt_movimento
                                        , 'R', par.par_dt_realizacaobx
                                             , par.par_dt_vencimento) <= :v_cto_dt_fim)
           OR :v_cons_cessao = 'N') -- Não considero baixas separando-as por cliente de cessão de direito via botão. Considero todas as baixas para o cliente atual

    AND DECODE( :v_por_data_baixa, 'V', NVL( par.par_dt_deposito, par.par_dt_baixa), :v_dt_fim) <= :v_dt_fim

    AND par.par_dt_baixa IS NOT NULL

    AND(( par.par_ch_status <> 'I') OR ( par.par_ch_status = 'I' AND par.par_dt_status > :v_dt_fim) )

    AND par.par_ch_receita      IN ( :v_tre_carteira, :v_tre_finan, :v_tre_fgts, :v_tre_bens, :v_tre_carta, :v_tre_permuta, :v_rec_subsidio, :v_rec_fin_dir)
    AND par.par_ch_receitabaixa IN ( :v_rec_cheque, :v_rec_dinheiro, :v_rec_bens, :v_rec_deposito, :v_rec_carta, :v_rec_permuta, :v_rec_boleto, :v_rec_ted, :v_rec_doc, :v_rec_boleto_av, :v_ch_rec_securit, :v_rec_repasse)

    AND ((( :v_confdivida = 'N') AND (( par.par_bo_confdivida IS NULL) OR ( par.par_bo_confdivida = 'N'))) OR ( :v_confdivida = 'S'))

    AND ((( :v_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0)) OR (( :v_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL)) OR ( :v_parc_caucao  = 'T'))

    AND ( ( :v_termo LIKE '%C%' AND ( ( par.par_bo_contratual = 'S' AND ter.tte_in_codigo IS NULL)))
       OR ( :v_termo LIKE '%F%' AND ( ( par.par_bo_contratual = 'N' AND ter.tte_in_codigo IS NULL)))
       OR ( :v_termo LIKE '%T%' AND ter.tte_in_codigo IS NOT NULL
                               AND ( :VConsT IS NULL OR :VConsT LIKE '%-' || ter.tte_in_codigo || '-%')
                               AND ( :VDescT IS NULL OR :VDescT NOT LIKE '%-' || ter.tte_in_codigo || '-%')
                               AND ( ( :v_termo LIKE '%E%' AND par.par_bo_contratual = 'S') OR ( :v_termo LIKE '%N%' AND par.par_bo_contratual = 'N') ) ) )

    AND NVL( cta.agn_in_codigo, 0) = DECODE( :v_cta_fin, 0, NVL( cta.agn_in_codigo, 0), :v_cta_fin)

    AND (( :v_ch_parcela = 'N' AND par.par_ch_parcela <> 'T') OR ( :v_ch_parcela = 'S'))

    AND (   ( :v_mostra_par_smov = 'N') -- Considera parcelas movimentadas e não movimentadas
         OR ( :v_mostra_par_smov = 'S'  -- Considera somente parcelas movimentadas
             AND EXISTS (SELECT 1
                         FROM mgcar.car_movimento_parcela@life mvv
                            , mgcar.car_movimento_rateio@life  maa
                            , mgcar.car_integra_movimento@life imm
                         WHERE (mvv.mpa_bo_ativa     = 'S' OR mvv.mpa_bo_ativa IS NULL)
                           AND mvv.org_tab_in_codigo = maa.org_tab_in_codigo
                           AND mvv.org_pad_in_codigo = maa.org_pad_in_codigo
                           AND mvv.org_in_codigo     = maa.org_in_codigo
                           AND mvv.org_tau_st_codigo = maa.org_tau_st_codigo
                           AND mvv.mov_in_codigo     = maa.mov_in_codigo
                           AND mvv.mra_in_codigo     = maa.mra_in_codigo

                           AND maa.org_tab_in_codigo = imm.org_tab_in_codigo
                           AND maa.org_pad_in_codigo = imm.org_pad_in_codigo
                           AND maa.org_in_codigo     = imm.org_in_codigo
                           AND maa.org_tau_st_codigo = imm.org_tau_st_codigo
                           AND maa.mov_in_codigo     = imm.mov_in_codigo

                           AND mvv.org_tab_in_codigo = par.org_tab_in_codigo
                           AND mvv.org_pad_in_codigo = par.org_pad_in_codigo
                           AND mvv.org_in_codigo     = par.org_in_codigo
                           AND mvv.org_tau_st_codigo = par.org_tau_st_codigo
                           AND mvv.par_in_codigo     = par.par_in_codigo

                           AND imm.imo_bo_integrado IN (:v_mov_naointeg, :v_mov_integrados)))) -- Opção pelo status do movimento financeiro

    AND obs.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND obs.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND obs.org_in_codigo     (+)= par.org_in_codigo
    AND obs.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND obs.cto_in_codigo     (+)= par.cto_in_codigo
    AND obs.par_in_codigo     (+)= par.par_in_codigo

    AND par.ban_in_numero     = ban.ban_in_numero (+)

    AND tpb.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND tpb.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND tpb.org_in_codigo     (+)= par.org_in_codigo
    AND tpb.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND tpb.cto_in_codigo     (+)= par.cto_in_codigo
    AND tpb.par_in_codigo     (+)= par.par_in_codigo

    AND cau.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND cau.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND cau.org_in_codigo     (+)= par.org_in_codigo
    AND cau.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND cau.cto_in_codigo     (+)= par.cto_in_codigo
    AND cau.par_in_codigo     (+)= par.par_in_codigo

    AND ant.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND ant.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND ant.org_in_codigo     (+)= par.org_in_codigo
    AND ant.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND ant.cto_in_codigo     (+)= par.cto_in_codigo
    AND ant.par_in_codigo     (+)= par.par_in_codigo

    AND des.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND des.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND des.org_in_codigo     (+)= par.org_in_codigo
    AND des.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND des.cto_in_codigo     (+)= par.cto_in_codigo
    AND des.par_in_codigo     (+)= par.par_in_codigo

    AND bol.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND bol.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND bol.org_in_codigo     (+)= par.org_in_codigo
    AND bol.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND bol.cto_in_codigo     (+)= par.cto_in_codigo
    AND bol.par_in_codigo     (+)= par.par_in_codigo

    AND ter.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND ter.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND ter.org_in_codigo     (+)= par.org_in_codigo
    AND ter.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND ter.cto_in_codigo     (+)= par.cto_in_codigo
    AND ter.ctt_in_codigo     (+)= par.ctt_in_codigo

    AND cpl.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND cpl.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND cpl.org_in_codigo     (+)= par.org_in_codigo
    AND cpl.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND cpl.cto_in_codigo     (+)= par.cto_in_codigo
    AND cpl.par_in_codigo     (+)= par.par_in_codigo

    AND mov.org_tab_in_codigo (+)= par.org_tab_in_codigo
    AND mov.org_pad_in_codigo (+)= par.org_pad_in_codigo
    AND mov.org_in_codigo     (+)= par.org_in_codigo
    AND mov.org_tau_st_codigo (+)= par.org_tau_st_codigo
    AND mov.cto_in_codigo     (+)= par.cto_in_codigo
    AND mov.par_in_codigo     (+)= par.par_in_codigo

    AND mra.org_tab_in_codigo (+)= mov.org_tab_in_codigo
    AND mra.org_pad_in_codigo (+)= mov.org_pad_in_codigo
    AND mra.org_in_codigo     (+)= mov.org_in_codigo
    AND mra.org_tau_st_codigo (+)= mov.org_tau_st_codigo
    AND mra.mov_in_codigo     (+)= mov.mov_in_codigo
    AND mra.mra_in_codigo     (+)= mov.mra_in_codigo

    AND itm.org_tab_in_codigo (+)= mra.org_tab_in_codigo
    AND itm.org_pad_in_codigo (+)= mra.org_pad_in_codigo
    AND itm.org_in_codigo     (+)= mra.org_in_codigo
    AND itm.org_tau_st_codigo (+)= mra.org_tau_st_codigo
    AND itm.mov_in_codigo     (+)= mra.mov_in_codigo

    AND cta.agn_tab_in_codigo (+)= itm.agn_tab_in_codigo
    AND cta.agn_pad_in_codigo (+)= itm.agn_pad_in_codigo
    AND cta.agn_in_codigo     (+)= itm.agn_in_codigo

    AND cob.hcob_in_sequencia (+)= des.hcob_in_sequencia
    AND cob.hcon_in_sequencia (+)= des.hcon_in_sequencia

    AND tpt.tte_in_codigo (+)= ter.tte_in_codigo'''
  
  dfParcelas = pd.DataFrame()
  boletim_recebimento = pd.DataFrame(columns=[  'INV_IN_CODIGO',
                                                'INV_ST_NOME',
                                                'CTO_IN_CODIGO',
                                                'FIL_IN_CODIGO',
                                                'FIL_ST_NOME',
                                                'EMP_IN_CODIGO',
                                                'EMP_ST_CODIGO',
                                                'EMP_ST_NOME',
                                                'BLO_IN_CODIGO',
                                                'BLO_ST_NOME',
                                                'UND_ST_CODIGO',
                                                'CTOENV_CH_ORIGEM',
                                                'TIPO',
                                                'AGN_ST_NOME',
                                                'PRO_IN_REDUZIDO',
                                                'PRO_ST_DESCRICAO',
                                                'AGN_IN_CODIGO',
                                                'PAR_IN_CODIGO',
                                                'PAR_DT_VENCIMENTO',
                                                'PAR_DT_MOVIMENTO',
                                                'PAR_DT_REALIZACAOBX',
                                                'PAR_RE_VALORORIGINAL',
                                                'PAR_RE_VALORPAGO',
                                                'PAR_RE_VALORTAXAS',
                                                'PAR_RE_VALORMULTA',
                                                'PAR_RE_VALORATRASO',
                                                'VL_CORRIGIDO',
                                                'VL_JURO',
                                                'PAR_RE_VALORJUROSREN',
                                                'VL_PAGO',
                                                'PAR_RE_DESCONTOSEMANTECIP',
                                                'PAR_RE_DESCONTOANTECIP',
                                                'PAR_RE_RESIDUOCOBRANCA',
                                                'PAR_DT_BAIXA',
                                                'PAR_BO_CONFDIVIDA',
                                                'PAR_ST_AGENCIA',
                                                'PAR_CH_RECEITABAIXA',
                                                'BAN_IN_NUMERO',
                                                'PAR_ST_CONTA',
                                                'PAR_RE_CREDITO',
                                                'TPR_RE_ABATIDO',
                                                'TPR_RE_JUROSTP',
                                                'ANT_RE_VMJROTPNCOB',
                                                'PAR_RE_VALORCORRECAO_ATR',
                                                'PAR_ST_OBSERVACAO',
                                                'TTE_IN_CODIGO',
                                                'TTE_ST_DESCRICAO',
                                                'IDENTIFICADOR',
                                                'PLA_RE_JROTP'])

  index=0
  for linha_contrato in contratoDf.index:
    contrato = contratoDf.at[linha_contrato, 'CTO_IN_CODIGO'].item()
    dt_entrega = contratoDf.at[linha_contrato, 'ENT_DT_ENTREGA']
    pco_bo_jrotpsaldovo = contratoDf.at[linha_contrato, 'PCO_BO_JROTPSALDOVO']
    print(contrato)

    if(dt_entrega is None):
      dt_entrega = cursor.execute('''SELECT TO_DATE( NULL, 'dd/mm/yyyy') FROM dual''').fetchall()[0][0]

    cursor_parcela_params={'v_dt_fim': v_dt_fim,
    'v_dt_ini': v_dt_ini,
    'v_tab': 53,
    'v_pad': 1,
    'v_cod': fil_in_codigo-1,
    'v_tau': 'G',
    'v_cto': contrato,
    'v_cons_cessao': 'N',
    'v_ent_dt_entrega': dt_entrega,
    'v_fil_jrotpsaldovo': pco_bo_jrotpsaldovo,
    'VConsT': '',
    'VDescT': '',
    'v_contrl_cheque': 'N', # 'S' ou 'N'
    'v_por_data_baixa': 'B', #  "B" - Indica que é por data de baixa | "M" - Indica que é por data de movimento | "R" - senão é por data de realização da baixa | "V" - Vencimento
    'v_cto_dt_ini': v_dt_ini,
    'v_cto_dt_fim': v_dt_fim,
    'v_tre_carteira': 'C', #tre_carteira - "C" ou "X" | tre_finan   - "F" ou "X" | tre_fgts       - "G" ou "X" | tre_bens      - "B" ou "X"
    'v_tre_finan': 'F',
    'v_tre_fgts': 'G',
    'v_tre_bens': 'B',
    'v_tre_carta': 'K',  # tre_carta    - "K" ou "X" | tre_permuta - "P" ou "X" | p_rec_subsidio - "S" ou "X" | p_rec_fin_dir - "I" ou "X"
    'v_tre_permuta': 'P',
    'v_rec_subsidio': 'S',
    'v_rec_fin_dir': 'I',
    'v_rec_cheque': 'C', #rec_cheque  - "C" ou "X" | rec_dinheiro - "D" ou "X" | rec_bens - "E" ou "X" | rec_deposito - "P" ou "X" | rec_carta - "K" ou "X"
    'v_rec_dinheiro': 'D',
    'v_rec_bens': 'E',
    'v_rec_deposito': 'P',
    'v_rec_carta': 'K', 
    'v_rec_permuta': 'T', #rec_permuta - "T" ou "X" | rec_boleto   - "B" ou "X" | rec_ted - "H" ou "X"  | rec_doc      - "O" ou "X" | rec_boleto_avulso - "A" ou "X" | rec_repasse - "R" ou "X"
    'v_rec_boleto': 'B',
    'v_rec_ted': 'H',
    'v_rec_doc': 'O',
    'v_rec_boleto_av': 'A',
    'v_ch_rec_securit': 'S', #p_ch_rec_securit   : Considerar Receita de Baixa Securitização:        "S" - Sim | "X" - Não
    'v_rec_repasse': 'R',
    'v_confdivida': 'N', #pconfdivida  : Considerar baixas de confissão de dívida. "S" - Sim | "N" - Não ??
    'v_parc_caucao': 'T', #pparc_caucao : Tipo Parcela: "T" - Todas | "C" - Caucionadas | "N" - Não Caucionadas
    'v_termo': 'CFTETN', #v_st_termo: "CFTETN" - todos | "C" - contratuais | "F" - não contratuais | "TE" - termos contratuais | "TN" - termos não contratuais | "CTE" - contratuais e termos contratuais | "TETN" - termos contratuais e não contratuais
    'v_cta_fin': 0, # cta_fin      : Considerar contas financeiras, 0 lista todas.
    'v_ch_parcela': 'S', #??p_ch_parcela       : Filtro a ser escolhido pelo cliente para trazer as Taxas Adicionais ou não trazer essa informação: "N" - Não traz | "S" - Traz
    'v_mostra_par_smov': 'N', #??p_mostra_par_smov  : Opção para trazer somente parcelas que nao esteja no processo de movimentação financeira. "S" - Sim | "N" - Não
    'v_mov_naointeg': 'S', #v_mov_naointegrados: Considerar Movimentos Não Integrados: "S" | "X"
    'v_mov_integrados': 'N', #p_mov_integrados   : Considerar Movimentos Integrados: "N" | "X"
    }

    dfParcelas = pd.read_sql(query_parcelas, con, params=cursor_parcela_params)

    INV_IN_CODIGO = cursor_inv.at[linha_contrato, 'INV_IN_CODIGO']
    INV_ST_NOME = cursor_inv.at[linha_contrato, 'INV_ST_NOME']
    CTO_IN_CODIGO = contratoDf.at[linha_contrato, 'CTO_IN_CODIGO']
    FIL_IN_CODIGO = contratoDf.at[linha_contrato, 'FIL_IN_CODIGO']
    FIL_ST_NOME = contratoDf.at[linha_contrato, 'FIL_ST_NOME']
    EMP_IN_CODIGO = contratoDf.at[linha_contrato, 'EMP_IN_CODIGO']
    EMP_ST_CODIGO = contratoDf.at[linha_contrato, 'EMP_ST_CODIGO']
    EMP_ST_NOME = contratoDf.at[linha_contrato, 'EMP_ST_NOME']
    BLO_IN_CODIGO = contratoDf.at[linha_contrato, 'BLO_IN_CODIGO']
    BLO_ST_NOME = contratoDf.at[linha_contrato, 'BLO_ST_NOME']
    UND_ST_CODIGO = contratoDf.at[linha_contrato, 'UND_ST_CODIGO']
    CTOENV_CH_ORIGEM = contratoDf.at[linha_contrato, 'CTOENV_CH_ORIGEM']
    TIPO = contratoDf.at[linha_contrato, 'TIPO']
    AGN_ST_NOME = contratoDf.at[linha_contrato, 'AGN_ST_NOME']  
    PRO_IN_REDUZIDO = contratoDf.at[linha_contrato, 'PRO_IN_REDUZIDO']
    PRO_ST_DESCRICAO = contratoDf.at[linha_contrato, 'PRO_ST_DESCRICAO']
    AGN_IN_CODIGO = contratoDf.at[linha_contrato, 'AGN_IN_CODIGO']
    
    for linha_parcela in dfParcelas.index:
      boletim_recebimento.at[index, 'INV_IN_CODIGO'] = INV_IN_CODIGO
      boletim_recebimento.at[index, 'INV_ST_NOME'] = INV_ST_NOME
      boletim_recebimento.at[index, 'CTO_IN_CODIGO'] = CTO_IN_CODIGO
      boletim_recebimento.at[index, 'FIL_IN_CODIGO'] = FIL_IN_CODIGO
      boletim_recebimento.at[index, 'FIL_ST_NOME'] = FIL_ST_NOME
      boletim_recebimento.at[index, 'EMP_IN_CODIGO'] = EMP_IN_CODIGO
      boletim_recebimento.at[index, 'EMP_ST_CODIGO'] = EMP_ST_CODIGO
      boletim_recebimento.at[index, 'EMP_ST_NOME'] = EMP_ST_NOME
      boletim_recebimento.at[index, 'BLO_IN_CODIGO'] = BLO_IN_CODIGO
      boletim_recebimento.at[index, 'BLO_ST_NOME'] = BLO_ST_NOME
      boletim_recebimento.at[index, 'UND_ST_CODIGO'] = UND_ST_CODIGO
      boletim_recebimento.at[index, 'CTOENV_CH_ORIGEM'] = CTOENV_CH_ORIGEM
      boletim_recebimento.at[index, 'AGN_CH_TIPOPESSOAFJ'] = TIPO
      boletim_recebimento.at[index, 'AGN_ST_NOME'] = AGN_ST_NOME
      boletim_recebimento.at[index, 'PRO_IN_REDUZIDO'] = PRO_IN_REDUZIDO
      boletim_recebimento.at[index, 'PRO_ST_DESCRICAO'] = PRO_ST_DESCRICAO
      boletim_recebimento.at[index, 'AGN_IN_CODIGO'] = AGN_IN_CODIGO
      boletim_recebimento.at[index,'PAR_IN_CODIGO'] = dfParcelas.at[linha_parcela, 'PAR_IN_CODIGO']
      boletim_recebimento.at[index,'PAR_DT_VENCIMENTO'] = dfParcelas.at[linha_parcela, 'PAR_DT_VENCIMENTO']
      boletim_recebimento.at[index,'PAR_DT_MOVIMENTO'] = dfParcelas.at[linha_parcela, 'PAR_DT_MOVIMENTO']
      boletim_recebimento.at[index,'PAR_DT_REALIZACAOBX'] = dfParcelas.at[linha_parcela, 'PAR_DT_REALIZACAOBX']
      boletim_recebimento.at[index,'PAR_RE_VALORORIGINAL'] = dfParcelas.at[linha_parcela, 'PAR_RE_VALORORIGINAL']
      boletim_recebimento.at[index,'PAR_RE_VALORPAGO'] = dfParcelas.at[linha_parcela, 'PAR_RE_VALORPAGO']
      boletim_recebimento.at[index,'PAR_RE_VALORTAXAS'] = dfParcelas.at[linha_parcela, 'PAR_RE_VALORTAXAS']
      boletim_recebimento.at[index,'PAR_RE_VALORMULTA'] = dfParcelas.at[linha_parcela, 'PAR_RE_VALORMULTA']
      boletim_recebimento.at[index,'PAR_RE_VALORATRASO'] = dfParcelas.at[linha_parcela, 'PAR_RE_VALORATRASO']
      boletim_recebimento.at[index,'VL_CORRIGIDO'] = dfParcelas.at[linha_parcela, 'VL_CORRIGIDO']
      boletim_recebimento.at[index,'VL_JURO'] = dfParcelas.at[linha_parcela, 'VL_JURO']
      boletim_recebimento.at[index,'PAR_RE_VALORJUROSREN'] = dfParcelas.at[linha_parcela, 'PAR_RE_VALORJUROSREN']
      boletim_recebimento.at[index,'VL_PAGO'] = dfParcelas.at[linha_parcela, 'VL_PAGO']
      boletim_recebimento.at[index,'PAR_RE_VALORAMORTIZADO'] = dfParcelas.at[linha_parcela, 'PAR_RE_DESCONTOSEMANTECIP']
      boletim_recebimento.at[index,'PAR_RE_VALORANTECIPACAO'] = dfParcelas.at[linha_parcela, 'PAR_RE_DESCONTOANTECIP']
      boletim_recebimento.at[index,'PAR_RE_RESIDUOCOBRANCA'] = dfParcelas.at[linha_parcela, 'PAR_RE_RESIDUOCOBRANCA']
      boletim_recebimento.at[index,'PAR_DT_BAIXA'] = dfParcelas.at[linha_parcela, 'PAR_DT_BAIXA']
      boletim_recebimento.at[index,'PAR_BO_CONFDIVIDA'] = dfParcelas.at[linha_parcela, 'PAR_BO_CONFDIVIDA']
      boletim_recebimento.at[index,'PAR_ST_AGENCIA'] = dfParcelas.at[linha_parcela, 'PAR_ST_AGENCIA']
      boletim_recebimento.at[index,'PAR_CH_RECEITABAIXA'] = dfParcelas.at[linha_parcela, 'PAR_CH_RECEITABAIXA']
      boletim_recebimento.at[index,'BAN_IN_NUMERO'] = dfParcelas.at[linha_parcela, 'BAN_IN_NUMERO']
      boletim_recebimento.at[index,'PAR_ST_CONTA'] = dfParcelas.at[linha_parcela, 'PAR_ST_CONTA']
      boletim_recebimento.at[index,'PAR_RE_CREDITO'] = dfParcelas.at[linha_parcela, 'PAR_RE_CREDITO']
      boletim_recebimento.at[index,'TPR_RE_ABATIDO'] = dfParcelas.at[linha_parcela, 'TPR_RE_ABATIDO']
      boletim_recebimento.at[index,'TPR_RE_JUROSTP'] = dfParcelas.at[linha_parcela, 'TPR_RE_JUROSTP']
      boletim_recebimento.at[index,'ANT_RE_VMJROTPNCOB'] = dfParcelas.at[linha_parcela, 'ANT_RE_VMJROTPNCOB']
      boletim_recebimento.at[index,'PAR_RE_VALORCORRECAO_ATR'] = dfParcelas.at[linha_parcela, 'PAR_RE_VALORCORRECAO_ATR']
      boletim_recebimento.at[index,'PAR_ST_OBSERVACAO'] = dfParcelas.at[linha_parcela, 'PAR_ST_OBSERVACAO']
      boletim_recebimento.at[index,'TTE_IN_CODIGO'] = dfParcelas.at[linha_parcela, 'TTE_IN_CODIGO']
      boletim_recebimento.at[index,'TTE_ST_DESCRICAO'] = dfParcelas.at[linha_parcela, 'TTE_ST_DESCRICAO']
      boletim_recebimento.at[index,'IDENTIFICADOR'] = dfParcelas.at[linha_parcela, 'IDENTIFICADOR']
      boletim_recebimento.at[index,'PLA_RE_JROTP'] = dfParcelas.at[linha_parcela, 'PLA_RE_JROTP']
      index+=1                                                
       
  boletim_recebimento = boletim_recebimento[[ 'CTO_IN_CODIGO',
                                              'FIL_IN_CODIGO',
                                              'FIL_ST_NOME',
                                              'EMP_IN_CODIGO',
                                              'EMP_ST_CODIGO',
                                              'EMP_ST_NOME',
                                              'BLO_IN_CODIGO',
                                              'BLO_ST_NOME',
                                              'UND_ST_CODIGO',
                                              'CTOENV_CH_ORIGEM',
                                              'AGN_CH_TIPOPESSOAFJ',
                                              'AGN_ST_NOME',
                                              'PAR_IN_CODIGO',
                                              'PAR_DT_VENCIMENTO',
                                              'PAR_DT_MOVIMENTO',
                                              'PAR_DT_REALIZACAOBX',
                                              'PAR_RE_VALORORIGINAL',
                                              'PAR_RE_VALORPAGO',
                                              'PAR_RE_VALORTAXAS',
                                              'PAR_RE_VALORMULTA',
                                              'PAR_RE_VALORATRASO',
                                              'VL_CORRIGIDO',
                                              'VL_JURO',
                                              'PAR_RE_VALORJUROSREN',
                                              'VL_PAGO',
                                              'PAR_RE_VALORAMORTIZADO',
                                              'PAR_RE_VALORANTECIPACAO',
                                              'PAR_RE_RESIDUOCOBRANCA',
                                              'PAR_DT_BAIXA',
                                              'PAR_BO_CONFDIVIDA',
                                              'PAR_ST_AGENCIA',
                                              'PAR_CH_RECEITABAIXA',
                                              'BAN_IN_NUMERO',
                                              'PAR_ST_CONTA',
                                              'PAR_RE_CREDITO',
                                              'TPR_RE_ABATIDO',
                                              'TPR_RE_JUROSTP',
                                              'ANT_RE_VMJROTPNCOB',
                                              'PAR_RE_VALORCORRECAO_ATR',
                                              'PRO_IN_REDUZIDO',
                                              'PRO_ST_DESCRICAO',
                                              'PAR_ST_OBSERVACAO',
                                              'TTE_IN_CODIGO',
                                              'TTE_ST_DESCRICAO',
                                              'IDENTIFICADOR',
                                              'PLA_RE_JROTP',
                                              'INV_IN_CODIGO',
                                              'INV_ST_NOME',
                                              'AGN_IN_CODIGO']]

  boletim_recebimento = boletim_recebimento.reset_index(drop=True)

  boletim_recebimento =  boletim_recebimento.sort_values(by=['UND_ST_CODIGO', 'PAR_DT_VENCIMENTO'])
  boletim_recebimento.to_excel('./boletimteste.xlsx', index=False)
  
  time_fim = dt.datetime.today()

  print("Temp decorrido: ",time_fim-time_inicio)

gera_boletim_recebimento(44, 1641, '25-01-2021', '01-01-2017')