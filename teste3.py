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
      , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato@life( cto.org_tab_in_codigo
                                                    , cto.org_pad_in_codigo
                                                    , cto.org_in_codigo
                                                    , cto.org_tau_st_codigo
                                                    , cto.cto_in_codigo
                                                    , 'D' -- Valor Original do Cto + Termos do tipo Altera o Valor do Contrato
                                                    , :v_dt_base
                                                    , ''
                                                    , ''), 0) cto_re_valorcontrato
      , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato@life( cto.org_tab_in_codigo
                                                    , cto.org_pad_in_codigo
                                                    , cto.org_in_codigo
                                                    , cto.org_tau_st_codigo
                                                    , cto.cto_in_codigo
                                                    , 'O' -- Valor Original do Cto
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
                                                                                        , 'C'))) agn_in_codigo --PINC-3803
      , agn.agn_st_fantasia
      , DECODE(cto.cto_bo_cessaodir, 'N', agn.agn_st_nome
                                        , mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente@life( cto.org_tab_in_codigo
                                                                              , cto.org_pad_in_codigo
                                                                              , cto.org_in_codigo
                                                                              , cto.org_tau_st_codigo
                                                                              , cto.cto_in_codigo
                                                                              , :v_data_base
                                                                              , 'N')) agn_st_nome --PINC-3803
      , agn.agn_ch_tipopessoafj
      , agn.agn_st_email
      , CAST( NVL( mgrel.pck_rel_glo_fnc.fnc_glo_concatena_telagente@life( agn.agn_tab_in_codigo
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

extrato_completo_query = ''' 
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

BEGIN

  v_tab          := :org_tab_in_codigo;
  v_pad          := :org_pad_in_codigo;
  v_cod          := :org_in_codigo;
  v_fil          := :fil_in_codigo;
  v_tau          := :org_tau_st_codigo;
  v_emp          := :cod_est;
  v_cto          := :cod_cto;
  v_blc          := :cod_blc;
  v_dt_base      := to_date( :v_data_base,'dd/mm/yyyy');
  v_termo        := :v_st_termo;
  v_des          := :v_tdes;
  v_des1         := :v_tdes1;
  v_con          := :v_tcon;
  v_con1         := :v_tcon1;
  v_reajuste     := :v_descongela;
  v_ctrl_cheq    := :v_ctrl_cheque;
  v_descapit_tp  := :v_descap_tp;
  v_descapit_sac := :v_descap_sac;
  v_tp_par       := :v_tipo_parcela;
  v_ativo        := :v_ati;
  v_quit         := :v_qui;
  v_inadim       := :v_ina;
  v_distr        := :v_dis;
  v_cessao       := :v_ces;
  v_trans        := :v_tra;
  v_cons_taxa    := :p_cons_taxa;
  v_cons_bonif   := :p_cons_bonif;
  v_vl_corrigido := :p_vl_corrigido;
  v_consinv      := :p_investidor;
  v_conf_divida  := :p_cons_confdivida;
  v_agn_in_codigo:= :p_agn_in_codigo;
  v_par_secur    := :p_par_secur;
  vsConsJrsTPSAC := :p_cons_jr_tp_sac;

  -- Só irei considerar o parâmetro para mostrar cheques caso o "Controla cheques" estiver selecionado. Ch 45174
  IF v_ctrl_cheq = 'S' THEN
    v_mostra_chq := :v_mostra_cheque;
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
  IF :cod_uni > 0 THEN
    BEGIN
      SELECT est.cto_in_codigo
      INTO v_cto
      FROM mgrel.vw_car_estrutura@life est
      WHERE est.und_in_codigo = :cod_uni
        AND est.estrutura IN ( 'G', 'U')

        AND est.cto_org_tab_in_codigo = v_tab
        AND est.cto_org_pad_in_codigo = v_pad
        AND est.cto_org_in_codigo     = v_cod
        AND est.cto_org_tau_st_codigo = v_tau

        AND mgrel.pck_rel_fnc.fnc_car_status_cto_data@life( est.cto_org_tab_in_codigo
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
  FROM mgdbm.dbm_parametro_contabilidade@life pco
  WHERE pco.org_tab_in_codigo = v_tab
    AND pco.org_pad_in_codigo = v_pad
    AND pco.org_in_codigo     = :fil_in_codigo
    AND pco.org_tau_st_codigo = v_tau;

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
                FROM mgdbm.dbm_investidor@life inv
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
                FROM mgdbm.dbm_investidor@life inv
                WHERE inv.agn_in_codigo <> v_fil
                GROUP BY inv.org_tab_in_codigo
                        , inv.org_pad_in_codigo
                        , inv.org_in_codigo
                        , inv.org_tau_st_codigo
                        , inv.est_in_codigo)         inv
                , mgdbm.dbm_parametro_contabilidade@life pco
                , mgrel.vw_car_estrutura@life            est

              WHERE est.org_tab_in_codigo = v_tab
                AND est.org_pad_in_codigo = v_pad
                AND est.org_in_codigo     = v_cod
                AND est.fil_in_codigo     = v_fil
                AND est.cto_in_codigo     = :cto_in_codigo

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
              + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                              , par.org_pad_in_codigo
                                                                              , par.org_in_codigo
                                                                              , par.org_tau_st_codigo
                                                                              , par.cto_in_codigo
                                                                              , par.par_in_codigo), 0), 4))),0)
        INTO v_cheque_pre
            , v_vlr_cheque_pre
        FROM mgcar.car_parcela@life par
            , mgcar.car_residuo_cobranca@life res
        WHERE (( par.par_ch_status    <> 'I' ) OR ( par.par_ch_status = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
          AND TRUNC( par.par_dt_realizacaobx) <= v_dt_base
          AND par.par_ch_receitabaixa = 'C'
          AND ( par.par_ch_status     IN ( 'P', 'D', '1', '2', 'U') OR ( par.par_ch_status = 'A' AND ( TRUNC( par.par_dt_deposito) > v_dt_base OR par.par_dt_deposito IS NULL))) -- Considera cheques pre vencidos apenas com status de aberto, devolvido, depositado ou em custódia, e NÃO CONSIDERA cheques conpensados.
          AND par.org_tab_in_codigo   = :cto_org_tab_in_codigo
          AND par.org_pad_in_codigo   = :cto_org_pad_in_codigo
          AND par.org_in_codigo       = :cto_org_in_codigo
          AND par.org_tau_st_codigo   = :cto_org_tau_st_codigo
          AND par.cto_in_codigo       = :cto_in_codigo

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
              + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                              , par.org_pad_in_codigo
                                                                              , par.org_in_codigo
                                                                              , par.org_tau_st_codigo
                                                                              , par.cto_in_codigo
                                                                              , par.par_in_codigo), 0), 4))),0)
        INTO v_cheque_pre
            , v_vlr_cheque_pre
        FROM mgcar.car_parcela@life par
            , mgcar.car_residuo_cobranca@life res
        WHERE (( par.par_ch_status    <> 'I' ) OR ( par.par_ch_status = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
          AND TRUNC( par.par_dt_baixa)         > v_dt_base -- Apenas considerará cheques pré futuros, os pré vencidos considera como pagos e não contabiliza no contador
          AND TRUNC( par.par_dt_realizacaobx) <= v_dt_base
          AND par.par_ch_receitabaixa = 'C'
          AND par.org_tab_in_codigo   = :cto_org_tab_in_codigo
          AND par.org_pad_in_codigo   = :cto_org_pad_in_codigo
          AND par.org_in_codigo       = :cto_org_in_codigo
          AND par.org_tau_st_codigo   = :cto_org_tau_st_codigo
          AND par.cto_in_codigo       = :cto_in_codigo

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
      FROM mgcar.car_parcela@life par
      WHERE par.org_tab_in_codigo = :cto_org_tab_in_codigo
        AND par.org_pad_in_codigo = :cto_org_pad_in_codigo
        AND par.org_in_codigo     = :cto_org_in_codigo
        AND par.org_tau_st_codigo = :cto_org_tau_st_codigo
        AND par.cto_in_codigo     = :cto_in_codigo
        AND  (( par.par_ch_status <> 'I') OR ( par.par_ch_status  = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
        AND TRUNC( par.par_dt_geracao) <= v_dt_base;
    EXCEPTION WHEN OTHERS THEN
      v_dt_ini := TO_DATE( NULL, 'dd/mm/yyy');
      v_dt_fim := TO_DATE( NULL, 'dd/mm/yyy');
    END;

    BEGIN
    -- Total a Pagar
      SELECT SUM ( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                                          , par.org_pad_in_codigo
                                                          , par.org_in_codigo
                                                          , par.org_tau_st_codigo
                                                          , par.cto_in_codigo
                                                          , par.par_in_codigo
                                                          , v_dt_base
                                                          , :v_tipoindice  --'RP'
                                                          , 'A'
                                                          , -1
                                                          , 'S'), 0) )
                  + SUM ( NVL( DECODE( SIGN( par.par_dt_vencimento - v_dt_base), -1, ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos@life( par.org_tab_in_codigo
                                                                                                                                            , par.org_pad_in_codigo
                                                                                                                                            , par.org_in_codigo
                                                                                                                                            , par.org_tau_st_codigo
                                                                                                                                            , par.cto_in_codigo
                                                                                                                                            , par.par_in_codigo
                                                                                                                                            , v_dt_base
                                                                                                                                            , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                                                                                                                                                                            , par.org_pad_in_codigo
                                                                                                                                                                                            , par.org_in_codigo
                                                                                                                                                                                            , par.org_tau_st_codigo
                                                                                                                                                                                            , par.cto_in_codigo
                                                                                                                                                                                            , par.par_in_codigo
                                                                                                                                                                                            , v_dt_base
                                                                                                                                                                                            , :v_tipoindice
                                                                                                                                                                                            , v_reajuste
                                                                                                                                                                                            , -1
                                                                                                                                                                                            , 'S'), 0),2)
                                                                                                                                            , 'AM'), 0),2) -- Calcula mora e multa, passando  no parametro o valor 'AM'
                                                                                      + NVL( DECODE( SIGN( res.rescob_dt_processo - v_dt_base), 1, 0, par.par_re_residuocobranca),0)), 0), 0))
                    -- Alteração para adicionar o valor das taxas das parcelas
                  + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                                  , par.org_pad_in_codigo
                                                                                  , par.org_in_codigo
                                                                                  , par.org_tau_st_codigo
                                                                                  , par.cto_in_codigo
                                                                                  , par.par_in_codigo), 0), 2) )
                    -- Considero também o valor de resíduo corrigido. PINC-4292
                  + SUM ( ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduoCorrigido@life( par.org_tab_in_codigo
                                                                                  , par.org_pad_in_codigo
                                                                                  , par.org_in_codigo
                                                                                  , par.org_tau_st_codigo
                                                                                  , par.cto_in_codigo
                                                                                  , par.par_in_codigo
                                                                                  , v_dt_base), 0), 2) )

        INTO v_vlr_apagar
        FROM mgcar.car_caucao_parcela@life   cau
          , mgcar.car_residuo_cobranca@life res
          , mgcar.car_contrato_termo@life   ctt
          , mgcar.car_parcela@life          par
        WHERE --PAR
              (( par.par_ch_status <> 'I') OR ( par.par_ch_status  = 'I' AND TRUNC( par.par_dt_status) > v_dt_base))
          AND (( par.par_dt_baixa IS NULL) OR ( TRUNC( par.par_dt_baixa) > v_dt_base))
          AND TRUNC( par.par_dt_geracao) <= v_dt_base

          AND (( v_conf_divida  = 'N' AND NVL( par.par_bo_confdivida, 'N') = 'N')
            OR ( v_conf_divida  = 'N' AND ( par.par_bo_confdivida = 'S' AND TRUNC( par.par_dt_baixa) > v_dt_base))
            OR ( v_conf_divida  = 'S'))

          AND ((( :p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
            OR (( :p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
            OR ( :p_parc_caucao  = 'T'))

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

          AND par.org_tab_in_codigo = :cto_org_tab_in_codigo
          AND par.org_pad_in_codigo = :cto_org_pad_in_codigo
          AND par.org_in_codigo     = :cto_org_in_codigo
          AND par.org_tau_st_codigo = :cto_org_tau_st_codigo
          AND par.cto_in_codigo     = :cto_in_codigo

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
              + SUM ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                              , par.org_pad_in_codigo
                                                                              , par.org_in_codigo
                                                                              , par.org_tau_st_codigo
                                                                              , par.cto_in_codigo
                                                                              , par.par_in_codigo), 0), 4)) )
        INTO v_vlr_pago
        FROM mgcar.car_caucao_parcela@life      cau
          , mgcar.car_carta_credito_baixa@life ccb -- tabela para retorno das parcelas que foram pagas com carta de crédito resultante de pagamento a maior
          , mgcar.car_parcela@life             prr
          , mgcar.car_residuo_cobranca@life    res
          , mgcar.car_contrato_termo@life      ctt -- Tabela para relacionamento do filtro de termos
          , mgcar.car_parcela@life             par
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

        AND ((( :p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
          OR (( :p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
            OR ( :p_parc_caucao  = 'T'))

        AND ((( v_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
          OR (( v_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
            OR ( v_par_secur = 'T'))

        AND par.org_tab_in_codigo = :cto_org_tab_in_codigo
        AND par.org_pad_in_codigo = :cto_org_pad_in_codigo
        AND par.org_in_codigo     = :cto_org_in_codigo
        AND par.org_tau_st_codigo = :cto_org_tau_st_codigo
        AND par.cto_in_codigo     = :cto_in_codigo

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
    
    mgcar.pck_car_contabil.prc_car_calcula_abatidotpcto@life(:cto_org_tab_in_codigo,
                                                        :cto_org_pad_in_codigo,
                                                        :cto_org_in_codigo,
                                                        :cto_org_tau_st_codigo,
                                                        :cto_in_codigo);

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
      , :cto_org_tab_in_codigo             --14
      , :cto_org_pad_in_codigo             --15
      , :cto_org_in_codigo                 --16
      , :cto_org_tau_st_codigo             --17
      , :cto_in_codigo                     --18
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
      , mgcar.pck_car_fnc.fnc_car_origemparcela@life( par.org_tab_in_codigo
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
      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
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
        + ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                  , par.org_pad_in_codigo
                                                                  , par.org_in_codigo
                                                                  , par.org_tau_st_codigo
                                                                  , par.cto_in_codigo
                                                                  , par.par_in_codigo), 0), 4)) * NVL( v_perc_org, 1) par_re_valorpago --82
      , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , par.par_dt_baixa
                                                    , 'R') par_ch_reajuste                       --83
      , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , par.par_dt_baixa
                                                    , 'S') par_st_siglaindice                    --84
      , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , par.par_dt_baixa
                                                    , 'F') par_st_defasagem                      --85
      , TO_DATE( mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                            , par.org_pad_in_codigo
                                                            , par.org_in_codigo
                                                            , par.org_tau_st_codigo
                                                            , par.cto_in_codigo
                                                            , par.par_in_codigo
                                                            , par.par_dt_baixa
                                                            , 'D'), 'DD/MM/YYYY') par_dt_vigenciaindice        --86
      -- Corrige valor parcela até data de baixa
      , NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                              , par.org_pad_in_codigo
                                              , par.org_in_codigo
                                              , par.org_tau_st_codigo
                                              , par.cto_in_codigo
                                              , par.par_in_codigo
                                              , par.par_dt_baixa
                                              , :v_tipoindice  --'RP'
                                              , 'A'
                                              , -1
                                              , 'S'), 0) * NVL( v_perc_org, 1)    par_re_valoratualizado               --87
      -- Calculado o valor do resíduo a gerar sem correcao
      , ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduo@life( par.org_tab_in_codigo
                                                                  , par.org_pad_in_codigo
                                                                  , par.org_in_codigo
                                                                  , par.org_tau_st_codigo
                                                                  , par.cto_in_codigo
                                                                  , par.par_in_codigo
                                                                  , v_dt_base), 0), 2) * NVL( v_perc_org, 1) par_re_valorresiduo    --88
      -- Calculado o valor do resíduo a gerar corrigido
      , ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduoCorrigido@life( par.org_tab_in_codigo
                                                                          , par.org_pad_in_codigo
                                                                          , par.org_in_codigo
                                                                          , par.org_tau_st_codigo
                                                                          , par.cto_in_codigo
                                                                          , par.par_in_codigo
                                                                          , v_dt_base), 0), 2) * NVL( v_perc_org, 1) par_re_valorresiduocorrigido   --89
      , TRIM( par.par_in_residuo) par_in_residuo                                                                 --90
      , DECODE( SIGN( mgrel.pck_rel_fnc.fnc_car_parcela_de_residuo@life( par.org_tab_in_codigo
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
            DECODE(NVL(res.rescob_bo_cobrado, 'N'), 'N', (DECODE(NVL(res.rescob_re_valoracobrar, 0), 0, (DECODE(mgcar.pck_car_fnc.fnc_car_valorcorrecao@life(par.org_tab_in_codigo
                                                                                                                                                      , par.org_pad_in_codigo
                                                                                                                                                      , par.org_in_codigo
                                                                                                                                                      , par.org_tau_st_codigo
                                                                                                                                                      , par.cto_in_codigo
                                                                                                                                                      , par.par_in_codigo
                                                                                                                                                      , decode(par.par_ch_receitabaixa, 'B', DECODE(vs_IndiceProjetado, 'B', par.par_dt_baixa, par.par_dt_vencimento), par.par_dt_baixa)
                                                                                                                                                      , 'RP'
                                                                                                                                                      , 'D'
                                                                                                                                                      , -1) - res.rescob_re_correcaocobrada, 0, 0
                                                                                                                                                                                              , (DECODE(par.par_in_codigo, mgrel.pck_rel_fnc.fnc_car_saldo_rescobcontrato@life( :cto_org_tab_in_codigo
                                                                                                                                                                                                                                                                          , :cto_org_pad_in_codigo
                                                                                                                                                                                                                                                                          , :cto_org_in_codigo
                                                                                                                                                                                                                                                                          , :cto_org_tau_st_codigo
                                                                                                                                                                                                                                                                          , :cto_in_codigo
                                                                                                                                                                                                                                                                          , par.par_in_codigo
                                                                                                                                                                                                                                                                          , DECODE( SIGN( v_dt_base - res.rescob_dt_processo), 1, res.rescob_dt_processo - 1
                                                                                                                                                                                                                                                                                                                            , 0, res.rescob_dt_processo - 1
                                                                                                                                                                                                                                                                                                                                , v_dt_base)
                                                                                                                                                                                                                                                                          , 'P'), mgrel.pck_rel_fnc.fnc_car_saldo_rescobcontrato@life( :cto_org_tab_in_codigo
                                                                                                                                                                                                                                                                                                                                , :cto_org_pad_in_codigo
                                                                                                                                                                                                                                                                                                                                , :cto_org_in_codigo
                                                                                                                                                                                                                                                                                                                                , :cto_org_tau_st_codigo
                                                                                                                                                                                                                                                                                                                                , :cto_in_codigo
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

      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
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
      , mgrel.pck_rel_fnc.fnc_car_valor_parcelaquitacao@life( par.org_tab_in_codigo
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
                                                        , :v_tipoindice
                                                        , -1
                                                        ,'S'
                                                        , v_reajuste) vl_quit_tot --108
        -- Data do último reajuste anual, se não houver reajueste, retorna a data do contrato
      , mgrel.pck_rel_fnc.fnc_car_data_ult_reajuste@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo) data_ult_reaj_anual--109
      , v_dt_ini data_primeira_parc                                                        --110
      , v_dt_fim data_ultima_parc                                                          --111
      , ROUND( NVL( DECODE( ccb.ccr_in_codigo, NULL, 0, ccb.ccrb_re_valorbaixa), 0), 2) valor_baixa_ccred --112
      -- Saldo da(s) carta(s) de Crédito do Cliente
      , ROUND( NVL( MGREL.pck_rel_fnc.fnc_car_calcula_sld_cc_agente@life( cto.agn_tab_in_codigo
                                                                    , cto.agn_pad_in_codigo
                                                                    , cto.agn_in_codigo
                                                                    , v_dt_base), 0), 2) * NVL( v_perc_org, 1) sld_ccred_corrigido --113
      , v_vlr_apagar  --114
      , v_vlr_pago    --115
      , v_perc_org    --116
      , cto.cto_re_vlroricontrato * NVL( v_perc_org, 1) cto_re_vlroricontrato  --117

  FROM mgcar.car_caucao_parcela@life      cau
    , mgcar.car_parcela_observacao@life  obs
    , mgcar.car_carta_credito_baixa@life ccb  -- tabela para retorno das parcelas que foram pagas com carta de crédito resultante de pagamento a maior
    , mgcar.car_parcela@life             prr
    , mgcar.car_residuo_cobranca@life    res
    , mgcar.car_contrato_termo@life      ctt  -- Tabela para relacionamento do filtro de termos
    , mgdbm.dbm_condicao_item@life       cit
    , mgcar.car_parcela@life             par
    , mgcar.car_tabelaprice_baixa@life   tpb
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

    AND ((( :p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
      OR (( :p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
      OR ( :p_parc_caucao  = 'T'))

    AND ((( v_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
      OR (( v_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
      OR ( v_par_secur = 'T'))

    AND par.org_tab_in_codigo = :cto_org_tab_in_codigo
    AND par.org_pad_in_codigo = :cto_org_pad_in_codigo
    AND par.org_in_codigo     = :cto_org_in_codigo
    AND par.org_tau_st_codigo = :cto_org_tau_st_codigo
    AND par.cto_in_codigo     = :cto_in_codigo

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
      , :cto_org_tab_in_codigo       --14
      , :cto_org_pad_in_codigo       --15
      , :cto_org_in_codigo           --16
      , :cto_org_tau_st_codigo       --17
      , :cto_in_codigo               --18
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
      , NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                              , par.org_pad_in_codigo
                                              , par.org_in_codigo
                                              , par.org_tau_st_codigo
                                              , par.cto_in_codigo
                                              , par.par_in_codigo
                                              , v_dt_base
                                              , :v_tipoindice
                                              , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                            , par.org_pad_in_codigo
                                                                                            , par.org_in_codigo
                                                                                            , par.org_tau_st_codigo
                                                                                            , par.cto_in_codigo
                                                                                            , par.par_in_codigo), 'T', DECODE( v_vl_corrigido, 'S', 'M', DECODE( v_descapit_tp, 'S', 'TA' ,'M'))  -- TP
                                                                                                                , 'S', DECODE( v_vl_corrigido, 'S', 'M', DECODE( v_descapit_sac, 'S', 'SA', 'M')) -- SAC
                                                                                                                      , v_reajuste)
                                              , -1
                                              , 'N'), 0) * NVL( v_perc_org, 1) par_re_valorcorrigido --71

      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                                      , par.org_pad_in_codigo
                                                      , par.org_in_codigo
                                                      , par.org_tau_st_codigo
                                                      , par.cto_in_codigo
                                                      , par.par_in_codigo
                                                      , v_dt_base
                                                      , :v_tipoindice
                                                      , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                                    , par.org_pad_in_codigo
                                                                                                    , par.org_in_codigo
                                                                                                    , par.org_tau_st_codigo
                                                                                                    , par.cto_in_codigo
                                                                                                    , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', 'SA', 'M')
                                                                                                                        , 'T', DECODE( v_descapit_tp , 'S', 'TA', 'M')
                                                                                                                        , v_reajuste)
                                                      , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                                                              , par.org_pad_in_codigo
                                                                                                                              , par.org_in_codigo
                                                                                                                              , par.org_tau_st_codigo
                                                                                                                              , par.cto_in_codigo
                                                                                                                              , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', nvl(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                  , 'T', DECODE( v_descapit_tp , 'S', nvl(cto.cto_re_taxaant_tp, 0) , 0)
                                                                                                                                                      , nvl(cto.cto_re_taxaant, 0))
                                                                                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
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
      , mgcar.pck_car_fnc.fnc_car_origemparcela@life( par.org_tab_in_codigo
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
      , NVL( DECODE( SIGN( par.par_dt_vencimento - v_dt_base), -1, ( ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos@life( par.org_tab_in_codigo
                                                                                                                          , par.org_pad_in_codigo
                                                                                                                          , par.org_in_codigo
                                                                                                                          , par.org_tau_st_codigo
                                                                                                                          , par.cto_in_codigo
                                                                                                                          , par.par_in_codigo
                                                                                                                          , v_dt_base
                                                                                                                          , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                                                                                                                                                          , par.org_pad_in_codigo
                                                                                                                                                                          , par.org_in_codigo
                                                                                                                                                                          , par.org_tau_st_codigo
                                                                                                                                                                          , par.cto_in_codigo
                                                                                                                                                                          , par.par_in_codigo
                                                                                                                                                                          , v_dt_base
                                                                                                                                                                          , :v_tipoindice
                                                                                                                                                                          , DECODE( par.par_ch_origem, 'T', DECODE( v_descapit_tp, 'S', DECODE(v_reajuste, 'A', 'TA', 'TM'), v_reajuste)  -- TP
                                                                                                                                                                                                    , 'S', DECODE( v_descapit_sac, 'S', DECODE(v_reajuste, 'A', 'SA', 'SM'), v_reajuste) -- SAC
                                                                                                                                                                                                    , v_reajuste)
                                                                                                                                                                          , -1
                                                                                                                                                                          , 'S'), 0), 2)
                                                                                                                          , 'AM'), 0), 2) -- Calcula mora e multa, passando  no parametro o valor 'AM'
                                                                    ), 0), 0) * NVL( v_perc_org, 1) par_re_valorencargos          --78
      , 0   par_re_valordesconto           --79
      , ROUND( NVL( DECODE( SIGN( mgrel.pck_rel_fnc.fnc_car_parcela_de_residuo@life( par.org_tab_in_codigo
                                                                              , par.org_pad_in_codigo
                                                                              , par.org_in_codigo
                                                                              , par.org_tau_st_codigo
                                                                              , par.cto_in_codigo
                                                                              , par.par_in_codigo
                                                                              , 'D') - v_dt_base), 1, 0, par.par_re_residuocobranca),0),4) * NVL( v_perc_org, 1) par_re_residuocobranca     --80
      -- Taxas Adicionais
      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_total_taxasparcela@life( par.org_tab_in_codigo
                                                                , par.org_pad_in_codigo
                                                                , par.org_in_codigo
                                                                , par.org_tau_st_codigo
                                                                , par.cto_in_codigo
                                                                , par.par_in_codigo), 0), 4) par_re_valortaxas    --81
      -- Calcula o total pago na data de baixa
      , 0   par_re_valorpago     --82
      , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , v_dt_base
                                                    , 'R') par_ch_reajuste    --83
      , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , v_dt_base
                                                    , 'S') par_st_siglaindice  --84
      , mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , v_dt_base
                                                    , 'F') par_st_defasagem    --85
      , TO_DATE(mgrel.pck_rel_fnc.fnc_car_busca_indicebaixa@life( par.org_tab_in_codigo
                                                            , par.org_pad_in_codigo
                                                            , par.org_in_codigo
                                                            , par.org_tau_st_codigo
                                                            , par.cto_in_codigo
                                                            , par.par_in_codigo
                                                            , v_dt_base
                                                            , 'D'), 'DD/MM/YYYY') par_dt_vigenciaindice   --86
      -- Corrige valor parcela até data base
      , NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                              , par.org_pad_in_codigo
                                              , par.org_in_codigo
                                              , par.org_tau_st_codigo
                                              , par.cto_in_codigo
                                              , par.par_in_codigo
                                              , v_dt_base
                                              , :v_tipoindice  --'RP'
                                              , 'A'
                                              , -1
                                              , 'S'), 0) * NVL( v_perc_org, 1)       par_re_valoratualizado   --87
      -- Calculado o valor do resíduo a gerar sem correcao
      , CASE
          WHEN (cto.cto_ch_reajusteanual = 'V' AND par.par_dt_vencimento < v_dt_base) THEN
            ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduo@life( par.org_tab_in_codigo
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
            ROUND( NVL( mgcar.pck_car_residuoanual.fncGetValorResiduoCorrigido@life( par.org_tab_in_codigo
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
        + NVL( mgcar.pck_car_fnc.fnc_car_valorjuros@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , v_dt_base
                                                    , :v_tipoindice  --'RP'
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
        + NVL( mgcar.pck_car_fnc.fnc_car_valorcorrecao@life( par.org_tab_in_codigo
                                                      , par.org_pad_in_codigo
                                                      , par.org_in_codigo
                                                      , par.org_tau_st_codigo
                                                      , par.cto_in_codigo
                                                      , par.par_in_codigo
                                                      , v_dt_base
                                                      , :v_tipoindice  --'RP'
                                                      , v_reajuste  -- sempre considerar o valor original da parcela. PINC-4318
                                                      , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                                                              , par.org_pad_in_codigo
                                                                                                                              , par.org_in_codigo
                                                                                                                              , par.org_tau_st_codigo
                                                                                                                              , par.cto_in_codigo
                                                                                                                              , par.par_in_codigo), 'S', DECODE( v_descapit_sac  , 'S', nvl(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                  , 'T', DECODE( v_descapit_tp , 'S', nvl(cto.cto_re_taxaant_tp, 0), 0)
                                                                                                                                                        , nvl(cto.cto_re_taxaant, 0))
                                                                                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
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
      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos@life( par.org_tab_in_codigo
                                                              , par.org_pad_in_codigo
                                                              , par.org_in_codigo
                                                              , par.org_tau_st_codigo
                                                              , par.cto_in_codigo
                                                              , par.par_in_codigo
                                                              , v_dt_base
                                                              , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                                                                                            , par.org_pad_in_codigo
                                                                                                            , par.org_in_codigo
                                                                                                            , par.org_tau_st_codigo
                                                                                                            , par.cto_in_codigo
                                                                                                            , par.par_in_codigo
                                                                                                            , v_dt_base
                                                                                                            , :v_tipoindice
                                                                                                            , DECODE( par.par_ch_origem, 'T', DECODE( v_descapit_tp, 'S', 'TA', v_reajuste)   -- TP
                                                                                                                                        , 'S', DECODE( v_descapit_sac, 'S', 'SA', v_reajuste) -- SAC
                                                                                                                                            , v_reajuste)
                                                                                                            , -1
                                                                                                            , 'S'), 0),2)
                                                              , 'M'), 0),2) par_re_valormulta -- Calcular  multa, passando  no parametro o valor 'M'    --100

        -- Calcula valor do atraso
      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_calculaencargos@life( par.org_tab_in_codigo
                                                              , par.org_pad_in_codigo
                                                              , par.org_in_codigo
                                                              , par.org_tau_st_codigo
                                                              , par.cto_in_codigo
                                                              , par.par_in_codigo
                                                              , v_dt_base
                                                              , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                                                                                            , par.org_pad_in_codigo
                                                                                                            , par.org_in_codigo
                                                                                                            , par.org_tau_st_codigo
                                                                                                            , par.cto_in_codigo
                                                                                                            , par.par_in_codigo
                                                                                                            , v_dt_base
                                                                                                            , :v_tipoindice
                                                                                                            , DECODE( par.par_ch_origem, 'T', DECODE( v_descapit_tp, 'S', 'TA', v_reajuste)   -- TP
                                                                                                                                        , 'S', DECODE( v_descapit_sac, 'S', 'SA', v_reajuste) -- SAC
                                                                                                                                            , v_reajuste)
                                                                                                            , -1
                                                                                                            , 'S'), 0), 2)
                                                              , 'A'), 0),2) par_re_valoratraso -- Calcular  valor atraso, passando  no parametro o valor 'A' ---101
      , ROUND( mgcar.pck_car_fnc.fnc_car_calculaencargos@life( par.org_tab_in_codigo
                                                        , par.org_pad_in_codigo
                                                        , par.org_in_codigo
                                                        , par.org_tau_st_codigo
                                                        , par.cto_in_codigo
                                                        , par.par_in_codigo
                                                        , v_dt_base
                                                        , NVL( par.par_re_valororiginal, 0)
                                                        + NVL( par.par_re_valorjuros, 0)
                                                        + NVL( par.par_re_valorcorrecao, 0)
                                                        + ROUND( mgcar.pck_car_fnc.fnc_car_valorcorrecao@life( par.org_tab_in_codigo
                                                                                                        , par.org_pad_in_codigo
                                                                                                        , par.org_in_codigo
                                                                                                        , par.org_tau_st_codigo
                                                                                                        , par.cto_in_codigo
                                                                                                        , par.par_in_codigo
                                                                                                        , v_dt_base
                                                                                                        , 'RP'
                                                                                                        , 'M'
                                                                                                        , -1), 2)
                                                        + ROUND( mgcar.pck_car_fnc.fnc_car_valorjuros@life( par.org_tab_in_codigo
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

      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
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
      , NVL( mgrel.pck_rel_fnc.fnc_car_valor_parcelaquitacao@life( par.org_tab_in_codigo
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
                                                            , :v_tipoindice
                                                            , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                                                                    , par.org_pad_in_codigo
                                                                                                                                    , par.org_in_codigo
                                                                                                                                    , par.org_tau_st_codigo
                                                                                                                                    , par.cto_in_codigo
                                                                                                                                    , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', NVL(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                        , 'T', DECODE( v_descapit_tp , 'S', NVL(cto.cto_re_taxaant_tp, 0), 0)
                                                                                                                                                              , NVL( cto.cto_re_taxaant, 0))
                                                                                      , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
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
      , ROUND( NVL( mgcar.pck_car_fnc.fnc_car_renda_postecipada@life( mgcar.pck_car_fnc.fnc_car_corrige@life( par.org_tab_in_codigo
                                                                                                  , par.org_pad_in_codigo
                                                                                                  , par.org_in_codigo
                                                                                                  , par.org_tau_st_codigo
                                                                                                  , par.cto_in_codigo
                                                                                                  , par.par_in_codigo
                                                                                                  , v_dt_base
                                                                                                  , 'RP'
                                                                                                  , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                                                                                , par.org_pad_in_codigo
                                                                                                                                                , par.org_in_codigo
                                                                                                                                                , par.org_tau_st_codigo
                                                                                                                                                , par.cto_in_codigo
                                                                                                                                                , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', 'SA', 'M')
                                                                                                                                                                    , 'T', DECODE( v_descapit_tp,  'S', 'TA', 'M')
                                                                                                                                                                          , v_reajuste)
                                                                                                  , DECODE( v_cons_taxa, 'S', DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                                                                                                          , par.org_pad_in_codigo
                                                                                                                                                                          , par.org_in_codigo
                                                                                                                                                                          , par.org_tau_st_codigo
                                                                                                                                                                          , par.cto_in_codigo
                                                                                                                                                                          , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', nvl(cto.cto_re_taxaant_sac, 0), 0)
                                                                                                                                                                                              , 'T', DECODE( v_descapit_tp, 'S', nvl(cto.cto_re_taxaant_tp, 0), 0)
                                                                                                                                                                                                    , nvl(cto.cto_re_taxaant, 0))
                                                                                                                            , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
                                                                                                                                                                          , par.org_pad_in_codigo
                                                                                                                                                                          , par.org_in_codigo
                                                                                                                                                                          , par.org_tau_st_codigo
                                                                                                                                                                          , par.cto_in_codigo
                                                                                                                                                                          , par.par_in_codigo), 'S', DECODE( v_descapit_sac, 'S', -1, 0)
                                                                                                                                                                                              , 'T', DECODE( v_descapit_tp, 'S', -1, 0)
                                                                                                                                                                                                    , 0)))
                                                                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life ( par.org_tab_in_codigo
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
                                                                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem@life( par.org_tab_in_codigo
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
      , mgrel.pck_rel_fnc.fnc_car_valor_parcelaquitacao@life( par.org_tab_in_codigo
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
                                                        , :v_tipoindice
                                                        , -1
                                                        , 'S'
                                                        , v_reajuste) vl_quit_tot  --108
        -- Data do último reajuste anual, se não houver reajueste, retorna a data do contrato
      , mgrel.pck_rel_fnc.fnc_car_data_ult_reajuste@life( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo) data_ult_reaj_anual --109
      , v_dt_ini data_primeira_parc                                                         --110
      , v_dt_fim data_ultima_parc                                                           --111
      , 0 valor_baixa_ccred                                                                 --112
      -- Saldo da(s) carta(s) de Crédito do Cliente
      , ROUND( NVL( mgrel.pck_rel_fnc.fnc_car_calcula_sld_cc_agente@life( cto.agn_tab_in_codigo
                                                                    , cto.agn_pad_in_codigo
                                                                    , cto.agn_in_codigo
                                                                    , v_dt_base), 0), 2) * NVL( v_perc_org, 1) sld_ccred_corrigido --113
      , v_vlr_apagar                                                                                                              --114
      , v_vlr_pago                                                                                                                --115
      , v_perc_org                                                                                                                --116
      , cto.cto_re_vlroricontrato * NVL( v_perc_org, 1) cto_re_vlroricontrato                                                     --117
    FROM mgcar.car_caucao_parcela@life     cau
      , mgcar.car_parcela_observacao@life obs
      , mgcar.car_residuo_cobranca@life   res
      , mgcar.car_contrato_termo@life     ctt -- Tabela para relacionamento do filtro de termos
      , mgdbm.dbm_condicao_item@life      cit
      , mgcar.car_parcela@life            par
      , mgcar.car_tabelaprice_baixa@life  tpb
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

    AND ((( :p_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0))
      OR (( :p_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL))
        OR ( :p_parc_caucao  = 'T'))

    AND ((( v_par_secur = 'S') AND ( par.par_bo_securitizada = 'S'))
      OR (( v_par_secur = 'N') AND ( par.par_bo_securitizada = 'N'))
        OR ( v_par_secur = 'T'))

    AND par.org_tab_in_codigo = :cto_org_tab_in_codigo
    AND par.org_pad_in_codigo = :cto_org_pad_in_codigo
    AND par.org_in_codigo     = :cto_org_in_codigo
    AND par.org_tau_st_codigo = :cto_org_tau_st_codigo
    AND par.cto_in_codigo     = :cto_in_codigo

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

  BEGIN
    SELECT SUM( NVL(rel.par_re_valorresiduo_cc, 0))
    INTO vnTotalResiduo
    FROM mgcustom.rel_dados_contrato_api@life rel
       , ( SELECT par.org_tab_in_codigo
                , par.org_pad_in_codigo
                , par.org_in_codigo
                , par.org_tau_st_codigo
                , par.cto_in_codigo
                , par.par_in_codigo

           FROM mgcar.car_parcela@life par
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
END;
'''

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
 
cursor=pd.read_sql(cursorQuery,con,params=cursor_params)

i=0
for row in cursor.iterrows():
  query_params={  'org_tab_in_codigo':53,
                  'org_pad_in_codigo':1,
                  'org_in_codigo':11,
                  'fil_in_codigo':12,
                  'org_tau_st_codigo':'G',
                  'cod_est':708,
                  'cod_cto':0,
                  'cod_blc':0,
                  'cod_uni':0,
                  'v_data_base':'24-11-2020',
                  'v_st_termo':'CFTETN',
                  'v_tdes':'0',
                  'v_tdes1':'0',
                  'v_tcon':'0',
                  'v_tcon1':'0',
                  'v_tipoindice':'RPS',
                  'v_descongela':'A',
                  'v_ctrl_cheque':'N',
                  'v_descap_tp':'N',
                  'v_descap_sac':'X',
                  'v_mostra_cheque':'S',
                  'v_tipo_parcela':'S',
                  'v_ati':'A',
                  'v_qui':'Q',
                  'v_ina':'U',
                  'v_dis':'D',
                  'v_ces':'X',
                  'v_tra':'X',
                  'p_cons_taxa':'S',
                  'p_cons_bonif':'S',
                  'p_parc_caucao':'T',
                  'p_vl_corrigido':'S',
                  'p_investidor':'N',
                  'p_cons_confdivida':'S',
                  'p_agn_in_codigo':0,
                  'p_par_secur':'S',
                  'p_cons_jr_tp_sac':'S'}
  if(i==0):
    extrato_completo = pd.read_sql(extrato_completo_query,con,params=query_params)