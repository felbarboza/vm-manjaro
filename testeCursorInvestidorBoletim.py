import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt


query=''' SELECT est.org_tab_in_codigo
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
'v_tab': 53,
'v_pad': 1,
'v_cod': 0,
'v_tau': 'G',
'v_inv' :0,
'tab_in_codigo': 53,
'pad_in_codigo': 1,
'fil_in_codigo': 44,
'v_emitir_inv':'S',
'v_emitir_por': 'E',
'v_cod_emp': 1641,
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

cursor_contratos = pd.read_sql(query, con, params=cursor_params)
cursor_contratos.to_excel('./testeCursorBoletim.xlsx')

