import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt


cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
con = cx_Oracle.connect("life", "lli8339e", "megacloud")
cursor = con.cursor()

cursorQuery = '''SELECT est.emp_in_codigo
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
      , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato@life( cto.org_tab_in_codigo
                                                    , cto.org_pad_in_codigo
                                                    , cto.org_in_codigo
                                                    , cto.org_tau_st_codigo
                                                    , cto.cto_in_codigo
                                                    , 'D' 
                                                    , :v_dt_base
                                                    , ''
                                                    , ''), 0) cto_re_valorcontrato
      , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato@life( cto.org_tab_in_codigo
                                                    , cto.org_pad_in_codigo
                                                    , cto.org_in_codigo
                                                    , cto.org_tau_st_codigo
                                                    , cto.cto_in_codigo
                                                    , 'O' 
                                                    , :v_dt_base
                                                    , ''
                                                    , ''), 0) cto_re_vlroricontrato
      , cto.cto_re_totalresiduo
      , cto.cto_bo_descprorata
      , cto.cto_dt_cadastro
      , cto.cto_dt_assinatura
      , cto.cto_in_carenciaatraso
      , mgrel.pck_rel_fnc.fnc_car_status_cto_data@life ( cto.org_tab_in_codigo
                                                  , cto.org_pad_in_codigo
                                                  , cto.org_in_codigo
                                                  , cto.org_tau_st_codigo
                                                  , cto.cto_in_codigo
                                                  , :v_dt_base
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
                                        , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente@life( cto.org_tab_in_codigo
                                                                                        , cto.org_pad_in_codigo
                                                                                        , cto.org_in_codigo
                                                                                        , cto.org_tau_st_codigo
                                                                                        , cto.cto_in_codigo
                                                                                        , :v_data_base
                                                                                        , 'C'))) agn_in_codigo 
      , agn.agn_st_fantasia
      , DECODE(cto.cto_bo_cessaodir, 'N', agn.agn_st_nome
                                        , mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente@life( cto.org_tab_in_codigo
                                                                              , cto.org_pad_in_codigo
                                                                              , cto.org_in_codigo
                                                                              , cto.org_tau_st_codigo
                                                                              , cto.cto_in_codigo
                                                                              , :v_data_base
                                                                              , 'N')) agn_st_nome 
      , agn.agn_ch_tipopessoafj
      , agn.agn_st_email
      , CAST( NVL( mgrel.pck_rel_glo_fnc.fnc_glo_concatena_telagente@life( agn.agn_tab_in_codigo
                                                                    , agn.agn_pad_in_codigo
                                                                    , agn.agn_in_codigo
                                                                    , ', '
                                                                    , 'S'
                                                                    , NULL
                                                                    , 3), '-') AS VARCHAR(255)) agn_st_telefone
      
      , NVL( agn.tpl_st_sigla, '-')       tpl_st_sigla
      , NVL( agn.agn_st_logradouro, '-')  agn_st_logradouro
      , NVL( agn.agn_st_numero, 0)        agn_st_numero
      , NVL( agn.agn_st_complemento, '-') agn_st_complemento
      , NVL( agn.agn_st_bairro, '-')      agn_st_bairro
      , NVL( agn.agn_st_municipio, '-')   agn_st_municipio
      , NVL( agn.uf_st_sigla, '-')        uf_st_sigla
      , NVL( agn.agn_st_cep, '-')         agn_st_cep
      
      , pro.pro_st_extenso
      , pro.pro_st_apelido
      , pro.pro_st_descricao
      , pro.pro_tab_in_codigo
      , pro.pro_pad_in_codigo
      , pro.pro_ide_st_codigo
      , pro.pro_in_reduzido
      
      , cus.cus_st_extenso
      , cus.cus_st_apelido
      , cus.cus_st_descricao
      , cus.cus_tab_in_codigo
      , cus.cus_pad_in_codigo
      , cus.cus_ide_st_codigo
      , cus.cus_in_reduzido
      , cto.cto_ch_reajusteanual

FROM mgcon.con_centro_custo@life       cus
    , mgglo.glo_projetos@life           pro
    , mgglo.glo_agentes@life            agn
    , mgglo.glo_agentes_id@life         aid
    , mgcar.car_contrato_cliente@life   cli
    , mgdbm.dbm_condicao@life           cnd
    , mgrel.vw_car_estrutura@life       est
    , mgcar.car_contrato@life           cto

WHERE cto.org_tab_in_codigo = :v_tab
  AND cto.org_pad_in_codigo = :v_pad
  AND cto.org_in_codigo     = :v_cod
  AND cto.org_tau_st_codigo = :v_tau
  AND cto.fil_in_codigo     = :v_fil

  AND NVL( est.emp_in_codigo, 0) = DECODE( nvl(:v_emp, 0)  , 0, NVL( est.emp_in_codigo, 0), :v_emp)
  AND NVL( est.blo_in_codigo, 0) = DECODE( nvl(:v_blc, 0)  , 0, NVL( est.blo_in_codigo, 0), :v_blc)
  AND cto.cto_in_codigo          = DECODE( nvl(:v_cto, 0)  , 0, cto.cto_in_codigo, :v_cto)
  AND agn.agn_in_codigo          = DECODE( NVL( :v_agn_in_codigo, 0), 0, DECODE(cto.cto_bo_cessaodir, 'N', agn.agn_in_codigo
                                                                                                        , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente@life( cto.org_tab_in_codigo
                                                                                                                                                          , cto.org_pad_in_codigo
                                                                                                                                                          , cto.org_in_codigo
                                                                                                                                                          , cto.org_tau_st_codigo
                                                                                                                                                          , cto.cto_in_codigo
                                                                                                                                                          , :v_data_base
                                                                                                                                                          , 'C'))) -- PINC-3803
                                                                      , NVL( :v_agn_in_codigo, 0))

  -- Evita que realiza a consulta para todos empreendimentos
  AND (( nvl(:v_emp, 0) <> 0) OR ( nvl(:v_blc, 0) <> 0) OR ( nvl(:v_cto, 0) <> 0) OR ( nvl(:v_agn_in_codigo, 0) <> 0))

  AND mgrel.pck_rel_fnc.fnc_car_status_cto_data@life( cto.org_tab_in_codigo
                                                , cto.org_pad_in_codigo
                                                , cto.org_in_codigo
                                                , cto.org_tau_st_codigo
                                                , cto.cto_in_codigo
                                                , :v_dt_base
                                                , 'N') IN ( :v_ativo, :v_quit, :v_inadim, :v_distr, :v_cessao, :v_trans)

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
                                                              , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente@life( cto.org_tab_in_codigo
                                                                                                                , cto.org_pad_in_codigo
                                                                                                                , cto.org_in_codigo
                                                                                                                , cto.org_tau_st_codigo
                                                                                                                , cto.cto_in_codigo
                                                                                                                , :v_data_base
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
  AND cus.cus_in_reduzido   = cto.cus_in_reduzido '''

cursor_params = { 'v_dt_base':'24-11-2020', 
      'v_data_base':'24/11/2020',
      'v_tab':53,
      'v_pad':1,
      'v_cod':11,
      'v_tau':'G',
      'v_fil':12,
      'v_emp':708,
      'v_blc':0,
      'v_cto':0,
      'v_agn_in_codigo':0,
      'v_ativo':'A',
      'v_quit':'Q',
      'v_inadim':'I',
      'v_distr':'D',
      'v_cessao':'X',
      'v_trans':'X'}
 
cursor_table=pd.read_sql(cursorQuery,con,params=cursor_params)

declaracoes ='''

'''
cursor.execute('''
SELECT NVL(pco.pco_ch_reajprojetado, 'B')
  FROM mgdbm.dbm_parametro_contabilidade@life pco
  WHERE pco.org_tab_in_codigo = :v_tab
    AND pco.org_pad_in_codigo = :v_pad
    AND pco.org_in_codigo     = :fil_in_codigo
    AND pco.org_tau_st_codigo = :v_tau
''', v_tab=53, v_pad=1, fil_in_codigo=12, v_tau='G')

vs_IndiceProjetado=cursor.fetchall()[0][0]

v_mostra_chq = 'N'
v_desct = ''
v_const = ''

extrato_completo_query='''SELECT     
     
       CASE
          WHEN :status <> 'Q' THEN
            DECODE(NVL(res.rescob_bo_cobrado, 'N'), 'N', (DECODE(NVL(res.rescob_re_valoracobrar, 0), 0, (DECODE(mgcar.pck_car_fnc.fnc_car_valorcorrecao@life(par.org_tab_in_codigo
                                                                                                                                                      , par.org_pad_in_codigo
                                                                                                                                                      , par.org_in_codigo
                                                                                                                                                      , par.org_tau_st_codigo
                                                                                                                                                      , par.cto_in_codigo
                                                                                                                                                      , par.par_in_codigo
                                                                                                                                                      , decode(par.par_ch_receitabaixa, 'B', DECODE(:vs_IndiceProjetado, 'B', par.par_dt_baixa, par.par_dt_vencimento), par.par_dt_baixa)
                                                                                                                                                      , 'RP'
                                                                                                                                                      , 'D'
                                                                                                                                                      , -1) - res.rescob_re_correcaocobrada, 0, 0
                                                                                                                                                                                              , (DECODE(par.par_in_codigo, mgrel.pck_rel_fnc.fnc_car_saldo_rescobcontrato@life( :cto_org_tab_in_codigo
                                                                                                                                                                                                                                                                          , :cto_org_pad_in_codigo
                                                                                                                                                                                                                                                                          , :cto_org_in_codigo
                                                                                                                                                                                                                                                                          , :cto_org_tau_st_codigo
                                                                                                                                                                                                                                                                          , :codigo_contrato
                                                                                                                                                                                                                                                                          , par.par_in_codigo
                                                                                                                                                                                                                                                                          , DECODE( SIGN(  to_date(:v_data_base, 'dd/mm/yyyy') - res.rescob_dt_processo), 1, res.rescob_dt_processo - 1
                                                                                                                                                                                                                                                                                                                            , 0, res.rescob_dt_processo - 1
                                                                                                                                                                                                                                                                                                                                , :v_dt_base)
                                                                                                                                                                                                                                                                          , 'P'), mgrel.pck_rel_fnc.fnc_car_saldo_rescobcontrato@life( :cto_org_tab_in_codigo
                                                                                                                                                                                                                                                                                                                                , :cto_org_pad_in_codigo
                                                                                                                                                                                                                                                                                                                                , :cto_org_in_codigo
                                                                                                                                                                                                                                                                                                                                , :cto_org_tau_st_codigo
                                                                                                                                                                                                                                                                                                                                , :codigo_contrato
                                                                                                                                                                                                                                                                                                                                , par.par_in_codigo
                                                                                                                                                                                                                                                                                                                                , DECODE( SIGN( to_date(:v_data_base, 'dd/mm/yyyy')- res.rescob_dt_processo), 1, res.rescob_dt_processo-1
                                                                                                                                                                                                                                                                                                                                                                                  , 0, res.rescob_dt_processo-1
                                                                                                                                                                                                                                                                                                                                                                                      , :v_dt_base)
                                                                                                                                                                                                                                                                                                                                , 'V')
                                                                                                                                                                                                                                                                                , 0)) * NVL(1, 1)))
                                                                                                      , 0))
                                                        , 0)
          ELSE 0
        END rescob_re_valoracobrar                                                              
      , ROUND( NVL( MGREL.pck_rel_fnc.fnc_car_calcula_sld_cc_agente@life( :agn_tabela_in_codigo
                                                                    , :agente_pad_in_codigo
                                                                    , :agente_in_codigo
                                                                    , :v_dt_base), 0), 2) sld_ccred_corrigido 
  FROM mgcar.car_caucao_parcela@life      cau
    , mgcar.car_parcela_observacao@life  obs
    , mgcar.car_carta_credito_baixa@life ccb  
    , mgcar.car_parcela@life             prr
    , mgcar.car_residuo_cobranca@life    res
    , mgcar.car_contrato_termo@life      ctt  
    , mgdbm.dbm_condicao_item@life       cit
    , mgcar.car_parcela@life             par
    , mgcar.car_tabelaprice_baixa@life   tpb
  WHERE par.par_ch_status        <> 'I'
    AND TRUNC( par.par_dt_baixa) <= :v_dt_base

    AND (( :p_cons_confdivida  = 'N' AND NVL( par.par_bo_confdivida, 'N') = 'N')
      OR ( :p_cons_confdivida  = 'N' AND ( par.par_bo_confdivida = 'S' AND TRUNC( par.par_dt_baixa) > :v_dt_base))
      OR ( :p_cons_confdivida  = 'S'))

    
    AND (( :v_st_termo LIKE '%C%' AND (( par.par_bo_contratual = 'S' AND ctt.tte_in_codigo IS NULL)))
      OR ( :v_st_termo LIKE '%F%' AND (( par.par_bo_contratual = 'N' AND ctt.tte_in_codigo IS NULL)))
      OR ( :v_st_termo LIKE '%T%' AND ctt.tte_in_codigo IS NOT NULL
          AND ( :v_const IS NULL OR :v_const LIKE '%-' || ctt.tte_in_codigo || '-%')
          AND ( :v_desct IS NULL OR :v_desct NOT LIKE '%-' || ctt.tte_in_codigo || '-%')
          AND (( :v_st_termo LIKE '%E%' AND par.par_bo_contratual = 'S')
            OR ( :v_st_termo LIKE '%N%' AND par.par_bo_contratual = 'N'))))

    AND ((( :p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
      OR (( :p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
      OR ( :p_parc_caucao  = 'T'))

    AND ((( :p_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
      OR (( :p_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
      OR ( :p_par_secur = 'T'))

    AND par.org_tab_in_codigo = :cto_org_tab_in_codigo
    AND par.org_pad_in_codigo = :cto_org_pad_in_codigo
    AND par.org_in_codigo     = :cto_org_in_codigo
    AND par.org_tau_st_codigo = :cto_org_tau_st_codigo
    AND par.cto_in_codigo     = :codigo_contrato

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

  
  SELECT                       
                           
       0                rescob_re_valoracobrar                                                           
      
      , ROUND( NVL( mgrel.pck_rel_fnc.fnc_car_calcula_sld_cc_agente@life( :agn_tabela_in_codigo
                                                                    , :agente_pad_in_codigo
                                                                    , :agente_in_codigo
                                                                    , :v_dt_base), 0), 2) sld_ccred_corrigido                                          
    FROM mgcar.car_caucao_parcela@life     cau
      , mgcar.car_parcela_observacao@life obs
      , mgcar.car_residuo_cobranca@life   res
      , mgcar.car_contrato_termo@life     ctt 
      , mgdbm.dbm_condicao_item@life      cit
      , mgcar.car_parcela@life            par
      , mgcar.car_tabelaprice_baixa@life  tpb
  WHERE 
        ( ( par.par_ch_status <> 'I')
        OR ( par.par_ch_status  = 'I'
        AND TRUNC( par.par_dt_status) > :v_dt_base))
    AND (( par.par_dt_baixa IS NULL) OR ( TRUNC( par.par_dt_baixa) > :v_dt_base))
    AND TRUNC( par.par_dt_geracao) <= :v_dt_base

    AND (( :p_cons_confdivida  = 'N' AND NVL( par.par_bo_confdivida, 'N') = 'N')
      OR ( :p_cons_confdivida  = 'N' AND ( par.par_bo_confdivida = 'S' AND TRUNC( par.par_dt_baixa) > :v_dt_base))
      OR ( :p_cons_confdivida  = 'S'))

    
    AND (( :v_st_termo LIKE '%C%' AND (( par.par_bo_contratual = 'S' AND ctt.tte_in_codigo IS NULL)))
      OR ( :v_st_termo LIKE '%F%' AND ( ( par.par_bo_contratual = 'N' AND ctt.tte_in_codigo IS NULL)))
      OR ( :v_st_termo LIKE '%T%' AND ctt.tte_in_codigo IS NOT NULL
          AND ( :v_const IS NULL OR :v_const LIKE '%-' || ctt.tte_in_codigo || '-%')
          AND ( :v_desct IS NULL OR :v_desct NOT LIKE '%-' || ctt.tte_in_codigo || '-%')
          AND (( :v_st_termo LIKE '%E%' AND par.par_bo_contratual = 'S')
            OR ( :v_st_termo LIKE '%N%' AND par.par_bo_contratual = 'N') ) ) )

    AND ((( :p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
      OR (( :p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
        OR ( :p_parc_caucao  = 'T'))

    AND ((( :p_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
      OR (( :p_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
        OR ( :p_par_secur = 'T'))

    AND par.org_tab_in_codigo = :cto_org_tab_in_codigo
    AND par.org_pad_in_codigo = :cto_org_pad_in_codigo
    AND par.org_in_codigo     = :cto_org_in_codigo
    AND par.org_tau_st_codigo = :cto_org_tau_st_codigo
    AND par.cto_in_codigo     = :codigo_contrato

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
    AND tpb.par_in_codigo     (+)= par.par_in_codigo 
'''
i=0
extrato_completo=[]
for row in cursor_table.index:
  print(i)
  cto_in_codigo = cursor_table['CTO_IN_CODIGO'][row].item()
  cto_re_valorcontrato=cursor_table['CTO_RE_VALORCONTRATO'][row].item()
  cto_dt_cadastro=cursor_table['CTO_DT_CADASTRO'][row].strftime("%d/%m/%Y")
  cto_dt_assinatura=cursor_table['CTO_DT_ASSINATURA'][row].strftime("%d/%m/%Y")
  cto_st_observacao=cursor_table['CTO_ST_OBSERVACAO'][row]
  agn_st_nome=cursor_table['AGN_ST_NOME'][row]
  cus_st_descricao=cursor_table['CUS_ST_DESCRICAO'][row]
  cus_in_reduzido=cursor_table['CUS_IN_REDUZIDO'][row].item()
  cto_ch_status=cursor_table['CTO_CH_STATUS'][row]
  agn_tab_in_codigo=cursor_table['AGN_TAB_IN_CODIGO'][row].item()
  agn_pad_in_codigo=cursor_table['AGN_PAD_IN_CODIGO'][row].item()
  agn_in_codigo=cursor_table['AGN_IN_CODIGO'][row].item()
  cto_re_vlroricontrato=cursor_table['CTO_RE_VLRORICONTRATO'][row].item()
  emp_st_nome=cursor_table['EMP_ST_NOME'][row]
  cto_ch_reajusteanual=cursor_table['CTO_CH_REAJUSTEANUAL'][row]
  cto_re_taxaant=cursor_table['CTO_RE_TAXAANT'][row].item()
  cto_re_taxaant_sac=cursor_table['CTO_RE_TAXAANT_SAC'][row].item()
  cto_re_taxaant_tp=cursor_table['CTO_RE_TAXAANT_TP'][row].item()
  cursor.prepare(extrato_completo_query)
  cursor.execute(None, 
                  codigo_contrato=cto_in_codigo,
                  status=cto_ch_status,
                  agn_tabela_in_codigo=agn_tab_in_codigo,
                  agente_pad_in_codigo=agn_pad_in_codigo,
                  agente_in_codigo=agn_in_codigo,
                  cto_org_tab_in_codigo=53,
                  cto_org_pad_in_codigo=1,
                  cto_org_in_codigo=11,
                  cto_org_tau_st_codigo='G',
                  v_dt_base='24/11/2020',
                  v_data_base='24-11-2020',
                  v_st_termo='CFTETN',
                  v_const='0',
                  v_desct='0',
                  p_parc_caucao='T',
                  vs_IndiceProjetado=vs_IndiceProjetado,
                  p_cons_confdivida='S',
                  p_par_secur='S')
  data = cursor.fetchall()
  query_params={  'codigo_contrato':cto_in_codigo,
                  'status':cto_ch_status,
                  'agn_tabela_in_codigo':agn_tab_in_codigo,
                  'agente_pad_in_codigo':agn_pad_in_codigo,
                  'agente_in_codigo':agn_in_codigo,
                  'cto_ch_reajusteanual': cto_ch_reajusteanual,
                  'cto_org_tab_in_codigo':53,
                  'cto_org_pad_in_codigo':1,
                  'cto_org_in_codigo':11,
                  'cto_org_tau_st_codigo':'G',
                  'v_dt_base':'24/11/2020',
                  'v_st_termo':'CFTETN',
                  'v_const':'0',
                  'v_desct':'0',
                  'v_tipoindice':'RPS',
                  'v_descongela':'A',
                  'v_descap_tp':'N',
                  'v_descap_sac':'X',
                  'p_cons_taxa':'S',
                  'p_parc_caucao':'T',
                  'p_vl_corrigido':'S',
                  'p_cons_confdivida':'S',
                  'p_par_secur':'S',
                  'p_cons_jr_tp_sac':'S'}
  # for item in query_params:
  #   print(type(query_params[item]))
  # extrato_completo[i] = pd.read_sql(extrato_completo_query,con,params=query_params)

