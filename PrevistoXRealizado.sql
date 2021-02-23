CREATE OR REPLACE PROCEDURE MGCUSTOM.PRC_CAR_Previsto_Realizado_Ana( retorno IN OUT mgrel.pck_resultado.result
                                                                , org_tab_in_codigo NUMBER
                                                                , org_pad_in_codigo NUMBER
                                                                , org_in_codigo     NUMBER
                                                                , org_tau_st_codigo VARCHAR2
                                                                , fil_in_codigo     NUMBER
                                                                , usu_in_codigo     NUMBER
                                                                , comp_st_nome      VARCHAR2
                                                                , p_cod_emp         NUMBER
                                                                , p_data_ini        VARCHAR2
                                                                , p_data_fim        VARCHAR2
                                                                , p_sincroniza      VARCHAR2
                                                                , p_exclui_ina      VARCHAR2) AS

------------------------------------------------------------------------------------------------------------------------------------------------------------
-- RPT        : R_CAR_Previsto_Realizado.rpt
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Criada     : 14/05/2010
-- Responsavel: Janaina
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Parâmetros:
-- p_cod_emp   : Código do empreendimento, sendo '0' mostra todos.
-- p_data_ini  : Data início utilizada para filtrar data de vencimento e de baixa das parcelas.
-- p_data_fim  : Data fim utilizada para filtrar data de vencimento e de baixa das parcelas.
-- p_sincroniza: Considera Empreendimentos Sincronizados: 'N' - Não
--                                                        'S' - Sim
--                                                        'T' - Todos
-- p_exclui_ina: Opção para desconsiderar contratos inadimplentes: 'U' - Excluir inadimplentes
--                                                                 'N' - Não excluir
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Alterações:
-- 14/05/2010 - Janaina: Criada a procedure. Ch. 37907.
-- 13/09/2012 - Dayan: Inseridos campos 'fil_st_nome' e 'fil_in_codigo'. Ch. 357076
-- 05/12/2013 - Daniel Nascimento: Retirado o cursor de filial ativa e posto as tabelas no select do cursor "cur_cto_emp"
--              conforme padrão atual de desenvolvimento, e retirado as tabelas de agentes que não são usadas no relatório.
--              Retirado a view "mgglo.glo_vw_organizacao" dos selects de parcelas para ser usado no select de retorno.
--              No select de "Parcelas em aberto" foi utilizado a função "fnc_car_calculaencargos" no campo "par_re_saldodevedor"
--              passando "AM" no parâmetro "ASENCARGO" ao invés de usar as funções separadas. Ch. 68388
-- 24/03/2014 - Eliani: Alterada estrutura da procedure para melhora de performance. Ch.83094
-- 24/07/2014 - Jaqueline Silva: Alterada a cláusula where do cursor "parc_abertas" para que filtre os vctos das parcelas entre as
--              datas passadas como parâmetro do relatório;
--              Incluídos campos no Retorno da PRC, para serem utilizados nos QR's "Previsto", "Realizado" e "Não Realizado";
--              Excluídos campos "Qtde_baixada" e "Qtde_aberta", visto que não serão mais utilizados no relatório. Ch.: 119699
-- 12/03/2015 - Jaqueline Silva: Alterada a data passada para a função "fnc_car_status_cto_data" de v_mes_fim para v_data_status, visto que com
--              a data anterior, a função era chamada mais vezes desnecessariamente, sendo que não há status superior ao encontrado com SYSDATE. Issue 1100
-- 15/04/2015 - Caio Ernani: houve a alteração do cursor de parcelas em aberto,  para trazer o trunc do parametro data_ini já que a data de vencimento
--              também possuía TRUNC. PINC-1319
-- 21/09/2015 - Mauricio Neves: Alteração relatório de Acompanhamento da Carteira - Previsto X Realizado. PINC-2211
-- 07/10/2015 - Mauricio Neves: Retirado do campo 'tipo_parcela' a verificação (Dt.Contrato = Dt. Vencimento = Dt. Baixa) retornava 'N' Sinal.
--                              Valor de Antecipação, passa ser verificado pela 'Data de Vencimento' e não mas pela 'Data da Baixa'. PINC-2211
-- 26/11/2015 - Mauricio Neves: Inserido filtro para desconsiderar contratos D' - Distratados, 'C' Cessão de Direito e 'T' Transferido
--                              para parcelas pagas. PINC-2803
-- 16/12/2015 - Alex Xavier: Alterado o campo 'fil_st_nome' de 'agn.agn_st_nome' para 'agn.agn_st_fantasia'. PINC-2923
-- 22/07/2016 - André Luiz Carraro: Adicionado TRUNC(MONTH) na data inicial para sempre pegar o primeiro dia do mês. PINC-3684
-- 28/07/2016 - André Luiz Carraro: Corrigido condição do cursor de parcelas em aberto. PINC-3720
-- 04/10/2016 - André Luiz Carraro: Retornado alguns campos específicos por parcela, para criar a visão analítica. PINC-3900
-- 15/12/2016 - André Luiz Carraro: Reestruturado conforme REQINC466. PINC-4099
-- 16/11/2017 - Valmir Silva / AWR: Corrigido coluna "Inadimplência Acumulada" entre outros problemas. PINC-4871
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Fatos Relevantes:
-- V.10 - Revisor: 05/12/2013 - Daniel
-- V.10 - Auditor: 22/07/2014 - Jaqueline Silva
-- V.11 - Revisor: 03/12/2014 - Anacris Kosinsk
-- V.11 - Auditor: 15/04/2015 - Caio Ernani Nogarotto
------------------------------------------------------------------------------------------------------------------------------------------------------------

v_tab              NUMBER(3);
v_pad              NUMBER(3);
v_tau              VARCHAR2(3);
v_usuario          NUMBER(5);
v_comp             VARCHAR2(255);
v_cod_emp          NUMBER(22);
v_dt_inicial       DATE;
v_dt_final         DATE;
v_sincroniza       CHAR(1);
v_exclui_ina       CHAR(1);
v_mes_inicial      DATE;
v_mes_final        DATE;
v_total_meses      NUMBER(3);

CURSOR cur_cto_emp IS
  SELECT cto.org_tab_in_codigo
       , cto.org_pad_in_codigo
       , cto.org_in_codigo
       , cto.org_tau_st_codigo
       , cto.fil_in_codigo
       , atv.fil_st_nome
       , cto.cto_in_codigo
       , cto.cto_re_mora
       , cto.cto_re_juros
       , cto.cto_ch_tipomulta
       , NVL( cto.cto_re_totalresiduo, 0) cto_re_totalresiduo
       , cto.cto_bo_taxaempr
       , cto.cto_in_carenciaatraso
       , cto.cto_dt_cadastro
       , est.emp_in_codigo
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

  FROM mgrel.vw_car_estrutura                         est
     , mgcar.car_contrato                             cto
     , ( SELECT fil.fil_tab_in_codigo                                        org_tab_in_codigo
              , fil.fil_pad_in_codigo                                        org_pad_in_codigo
              , DECODE( agn.agn_bo_consolidador, 'E', agn.agn_in_codigo
                                                    , agn.pai_agn_in_codigo) org_in_codigo
              , agn.agn_in_codigo                                            fil_in_codigo
              , agn.agn_st_fantasia                                          fil_st_nome

          FROM mgglo.glo_filial_ativa fil
             , mgglo.glo_agentes      agn

          WHERE fil.usu_in_codigo     = v_usuario
            AND fil.comp_st_nome      = v_comp

            AND agn.agn_tab_in_codigo = fil.fil_tab_in_codigo
            AND agn.agn_pad_in_codigo = fil.fil_pad_in_codigo
            AND agn.agn_in_codigo     = fil.fil_in_codigo
            AND agn.agn_bo_consolidador IN('E', 'F')) atv

  WHERE cto.org_tab_in_codigo = v_tab
    AND cto.org_pad_in_codigo = v_pad
    AND cto.org_tau_st_codigo = v_tau
    AND est.emp_in_codigo     = DECODE( v_cod_emp, 0, est.emp_in_codigo, v_cod_emp)

    AND cto.cto_ch_status          NOT IN('D', 'T', 'C', v_exclui_ina)
    AND NVL( est.emp_in_codigo, 0) = NVL( mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                                                           , est.org_pad_in_codigo
                                                                                           , est.org_in_codigo
                                                                                           , est.org_tau_st_codigo
                                                                                           , est.emp_in_codigo
                                                                                           , v_sincroniza
                                                                                           ), 0)

    AND est.cto_org_tab_in_codigo = cto.org_tab_in_codigo
    AND est.cto_org_pad_in_codigo = cto.org_pad_in_codigo
    AND est.cto_org_in_codigo     = cto.org_in_codigo
    AND est.cto_org_tau_st_codigo = cto.org_tau_st_codigo
    AND est.cto_in_codigo         = cto.cto_in_codigo

    AND cto.org_tab_in_codigo     = atv.org_tab_in_codigo
    AND cto.org_pad_in_codigo     = atv.org_pad_in_codigo
    AND cto.org_in_codigo         = atv.org_in_codigo
    AND cto.fil_in_codigo         = atv.fil_in_codigo

  ORDER BY est.emp_in_codigo
         , cto.cto_in_codigo;

CURSOR cur_inadimplencia_anterior( p_cto_tab      NUMBER
                                 , p_cto_pad      NUMBER
                                 , p_cto_org      NUMBER
                                 , p_cto_tau      VARCHAR2
                                 , p_contrato     NUMBER
                                 , p_data_ini_mes DATE
                                 ) IS
  SELECT par.org_tab_in_codigo
       , par.org_pad_in_codigo
       , par.org_in_codigo
       , par.org_tau_st_codigo
       , par.cto_in_codigo
       , par.par_in_codigo
       , par.par_dt_vencimento
       , par.par_dt_baixa
       , DECODE(par.par_ch_receita, 'F', 'F', par.par_ch_parcela) par_ch_parcela
       , par.par_re_valororiginal
       , ROUND(CASE
                 WHEN par.par_dt_baixa IS NULL THEN
                   NVL(mgcar.pck_car_fnc.FNC_CAR_CORRIGE( par.org_tab_in_codigo
                                                        , par.org_pad_in_codigo
                                                        , par.org_in_codigo
                                                        , par.org_tau_st_codigo
                                                        , par.cto_in_codigo
                                                        , par.par_in_codigo
                                                        , par.par_dt_vencimento
                                                        , 'RP'
                                                        , 'M'
                                                        , -1), 0)
                 ELSE
                   NVL(par.par_re_valororiginal, 0)
                 + NVL(par.par_re_valorjuros, 0)
                 + NVL(par.par_re_valorcorrecao, 0)
                 + NVL(par.par_re_valorjurosren, 0)
                 + NVL(par.par_re_valorcorrecaobx, 0)
                 + NVL(par.par_re_valorjurosbx, 0)
               END
             + NVL(mgcar.pck_car_fnc.FNC_CAR_TOTAL_TAXASPARCELA( par.org_tab_in_codigo
                                                               , par.org_pad_in_codigo
                                                               , par.org_in_codigo
                                                               , par.org_tau_st_codigo
                                                               , par.cto_in_codigo
                                                               , par.par_in_codigo), 2)
             + NVL(par.par_re_residuocobranca, 0), 2) par_re_vlrtotalparcela
       , CASE
           WHEN (par.par_dt_baixa IS NULL ) or (par.par_dt_baixa >= p_data_ini_mes) THEN 'I'
           ELSE 'T'
         END  par_ch_baixada

  FROM mgcar.car_parcela par
  LEFT JOIN mgcar.car_contrato_termo ctt ON
            par.org_tab_in_codigo = ctt.org_tab_in_codigo
        AND par.org_pad_in_codigo = ctt.org_pad_in_codigo
        AND par.org_in_codigo     = ctt.org_in_codigo
        AND par.org_tau_st_codigo = ctt.org_tau_st_codigo
        AND par.cto_in_codigo     = ctt.cto_in_codigo
        AND par.ctt_in_codigo     = ctt.ctt_in_codigo

  WHERE par.org_tab_in_codigo = p_cto_tab
    AND par.org_pad_in_codigo = p_cto_pad
    AND par.org_in_codigo     = p_cto_org
    AND par.org_tau_st_codigo = p_cto_tau
    AND par.cto_in_codigo     = p_contrato
    AND par.par_ch_status     <> 'I'
    AND par.par_bo_confdivida = 'N'

    AND par.par_dt_vencimento < p_data_ini_mes

    AND (ctt.ctt_ch_status = 'A'
      OR ctt.ctt_ch_status IS NULL);

CURSOR cur_parcelas( p_cto_tab      NUMBER
                   , p_cto_pad      NUMBER
                   , p_cto_org      NUMBER
                   , p_cto_tau      VARCHAR2
                   , p_contrato     NUMBER
                   , p_data_ini_mes DATE
                   , p_data_fim_mes DATE
                   , p_filial       NUMBER
                   ) IS
  SELECT par.org_tab_in_codigo
       , par.org_pad_in_codigo
       , par.org_in_codigo
       , par.org_tau_st_codigo
       , par.cto_in_codigo
       , par.par_in_codigo
       , par.par_dt_vencimento
       , par.par_dt_baixa
       , DECODE(par.par_ch_receita, 'F', 'F', par.par_ch_parcela) par_ch_parcela
       , par.par_re_valororiginal
       , ROUND(NVL(par.par_re_valororiginal, 0)
             + NVL(par.par_re_valorjuros, 0)
             + NVL(par.par_re_valorcorrecao, 0)
             + NVL(par.par_re_valorjurosren, 0)
             + NVL(par.par_re_valorcorrecaobx, 0)
             + NVL(par.par_re_valorjurosbx, 0)
       + NVL(mgcar.pck_car_fnc.FNC_CAR_TOTAL_TAXASPARCELA( par.org_tab_in_codigo
                                                         , par.org_pad_in_codigo
                                                         , par.org_in_codigo
                                                         , par.org_tau_st_codigo
                                                         , par.cto_in_codigo
                                                         , par.par_in_codigo), 0)
       + NVL(par.par_re_residuocobranca, 0), 2) par_re_vlrtotalparcela
       , CASE
           WHEN (TO_CHAR(par.par_dt_baixa, 'MMYYYY') = TO_CHAR(par.par_dt_vencimento, 'MMYYYY')
                 OR par.par_dt_baixa = mgdbm.fnc_dbm_venctoreal( par.par_dt_vencimento
                                                               , par.org_tab_in_codigo
                                                               , par.org_pad_in_codigo
                                                               , p_filial))
                AND par.par_dt_vencimento BETWEEN p_data_ini_mes AND p_data_fim_mes THEN 'M'

           WHEN par.par_dt_baixa < p_data_ini_mes THEN 'A'

           WHEN TO_CHAR(par.par_dt_baixa, 'MMYYYY') = TO_CHAR(p_data_ini_mes, 'MMYYYY')
            AND par.par_dt_vencimento < p_data_ini_mes THEN 'I'

           WHEN par.par_dt_baixa > par.par_dt_vencimento AND par.par_dt_baixa > p_data_fim_mes THEN 'H'

           ELSE 'N'
         END par_ch_baixada

  FROM mgcar.car_parcela par
  LEFT JOIN mgcar.car_contrato_termo ctt ON
            par.org_tab_in_codigo = ctt.org_tab_in_codigo
        AND par.org_pad_in_codigo = ctt.org_pad_in_codigo
        AND par.org_in_codigo     = ctt.org_in_codigo
        AND par.org_tau_st_codigo = ctt.org_tau_st_codigo
        AND par.cto_in_codigo     = ctt.cto_in_codigo
        AND par.ctt_in_codigo     = ctt.ctt_in_codigo

  WHERE par.org_tab_in_codigo = p_cto_tab
    AND par.org_pad_in_codigo = p_cto_pad
    AND par.org_in_codigo     = p_cto_org
    AND par.org_tau_st_codigo = p_cto_tau
    AND par.cto_in_codigo     = p_contrato
    AND par.par_ch_status     <> 'I'
    AND par.par_bo_confdivida = 'N'

    AND (( par.par_dt_vencimento BETWEEN p_data_ini_mes AND p_data_fim_mes
    AND ( ( par.par_dt_baixa BETWEEN p_data_ini_mes AND p_data_fim_mes
         OR par.par_dt_baixa = mgdbm.fnc_dbm_venctoreal( par.par_dt_vencimento
                                                       , par.org_tab_in_codigo
                                                       , par.org_pad_in_codigo
                                                       , p_filial))
         OR par.par_dt_baixa < p_data_ini_mes))

      OR ( par.par_dt_baixa BETWEEN p_data_ini_mes AND p_data_fim_mes
       AND par.par_dt_vencimento < p_data_ini_mes
       AND par.par_dt_baixa > mgdbm.fnc_dbm_venctoreal( par.par_dt_vencimento
                                                       , par.org_tab_in_codigo
                                                       , par.org_pad_in_codigo
                                                       , p_filial))

      OR ( par.par_dt_vencimento BETWEEN p_data_ini_mes AND p_data_fim_mes
       AND par.par_dt_baixa > p_data_fim_mes))

    AND (ctt.ctt_ch_status = 'A'
      OR ctt.ctt_ch_status IS NULL)

  UNION ALL

  SELECT par.org_tab_in_codigo
       , par.org_pad_in_codigo
       , par.org_in_codigo
       , par.org_tau_st_codigo
       , par.cto_in_codigo
       , par.par_in_codigo
       , par.par_dt_vencimento
       , par.par_dt_baixa
       , DECODE(par.par_ch_receita, 'F', 'F', par.par_ch_parcela) par_ch_parcela
       , par.par_re_valororiginal
       , ROUND(NVL(mgcar.pck_car_fnc.FNC_CAR_CORRIGE( par.org_tab_in_codigo
                                                    , par.org_pad_in_codigo
                                                    , par.org_in_codigo
                                                    , par.org_tau_st_codigo
                                                    , par.cto_in_codigo
                                                    , par.par_in_codigo
                                                    , par.par_dt_vencimento
                                                    , 'RP'
                                                    , 'M'
                                                    , -1), 0)
       + NVL(mgcar.pck_car_fnc.FNC_CAR_TOTAL_TAXASPARCELA( par.org_tab_in_codigo
                                                         , par.org_pad_in_codigo
                                                         , par.org_in_codigo
                                                         , par.org_tau_st_codigo
                                                         , par.cto_in_codigo
                                                         , par.par_in_codigo), 0)
       + NVL(par.par_re_residuocobranca, 0), 2) par_re_vlrtotalparcela
       , 'P' par_ch_baixada

  FROM mgcar.car_parcela par
  LEFT JOIN mgcar.car_contrato_termo ctt ON
            par.org_tab_in_codigo = ctt.org_tab_in_codigo
        AND par.org_pad_in_codigo = ctt.org_pad_in_codigo
        AND par.org_in_codigo     = ctt.org_in_codigo
        AND par.org_tau_st_codigo = ctt.org_tau_st_codigo
        AND par.cto_in_codigo     = ctt.cto_in_codigo
        AND par.ctt_in_codigo     = ctt.ctt_in_codigo

  WHERE par.org_tab_in_codigo = p_cto_tab
    AND par.org_pad_in_codigo = p_cto_pad
    AND par.org_in_codigo     = p_cto_org
    AND par.org_tau_st_codigo = p_cto_tau
    AND par.cto_in_codigo     = p_contrato
    AND par.par_ch_status     <> 'I'
    AND par.par_bo_confdivida = 'N'
    AND par.par_dt_baixa      IS NULL

    AND par.par_dt_vencimento BETWEEN p_data_ini_mes AND p_data_fim_mes

    AND (ctt.ctt_ch_status = 'A'
      OR ctt.ctt_ch_status IS NULL);

BEGIN
  v_tab               := org_tab_in_codigo;
  v_pad               := org_pad_in_codigo;
  v_tau               := org_tau_st_codigo;
  v_usuario           := usu_in_codigo;
  v_comp              := comp_st_nome;
  v_cod_emp           := p_cod_emp;
  v_dt_inicial        := TRUNC(TO_DATE(p_data_ini, 'DD/MM/RRRR'), 'MONTH');
  v_dt_final          := TO_DATE(p_data_fim, 'DD/MM/RRRR');
  v_sincroniza        := p_sincroniza;
  v_exclui_ina        := p_exclui_ina;
  v_total_meses       := MONTHS_BETWEEN(TRUNC(v_dt_final, 'MONTH'), TRUNC(v_dt_inicial, 'MONTH')) + 1;

  DELETE FROM mgrel.rel_dados_contrato;
  COMMIT;

  v_mes_inicial := ADD_MONTHS(v_dt_inicial, -1);
  FOR i IN 1..v_total_meses LOOP
    v_mes_inicial := ADD_MONTHS(v_mes_inicial, 1);
    v_mes_final := LAST_DAY(v_mes_inicial);

    FOR cto IN cur_cto_emp LOOP

      IF i = 1 THEN
        FOR par_inadimp_anterior IN cur_inadimplencia_anterior( cto.org_tab_in_codigo
                                                              , cto.org_pad_in_codigo
                                                              , cto.org_in_codigo
                                                              , cto.org_tau_st_codigo
                                                              , cto.cto_in_codigo
                                                              , v_mes_inicial
                                                              ) LOOP
          INSERT INTO mgcustom.rel_dados_contrato_api( org_tab_in_codigo
                                              , org_pad_in_codigo
                                              , org_in_codigo
                                              , org_tau_st_codigo
                                              , fil_in_codigo
                                              , agn_st_nome
                                              , emp_in_codigo
                                              , emp_st_codigo
                                              , emp_st_nome
                                              , etp_in_codigo
                                              , etp_st_codigo
                                              , etp_st_nome
                                              , blo_in_codigo
                                              , blo_st_codigo
                                              , blo_st_nome
                                              , und_in_codigo
                                              , und_st_codigo
                                              , und_st_nome
                                              , cto_in_codigo
                                              , cto_bo_taxaempr
                                              , par_in_codigo
                                              , par_dt_baixa
                                              , par_dt_vencimento
                                              , par_dt_geracao
                                              , par_ch_parcela
                                              , par_re_valororiginal
                                              , par_re_valor_totalpago
                                              , par_re_credito
                                              , par_re_valoratraso
                                              , par_re_valorvencer
                                              , par_re_saldodevedor
                                              , par_re_saldoquitacao
                                              , par_re_valor_pagocorrigido
                                              , par_ch_origem)

                                       VALUES ( cto.org_tab_in_codigo
                                              , cto.org_pad_in_codigo
                                              , cto.org_in_codigo
                                              , cto.org_tau_st_codigo
                                              , cto.fil_in_codigo
                                              , cto.fil_st_nome
                                              , cto.emp_in_codigo
                                              , cto.emp_st_codigo
                                              , cto.emp_st_nome
                                              , cto.etp_in_codigo
                                              , cto.etp_st_codigo
                                              , cto.etp_st_nome
                                              , cto.blo_in_codigo
                                              , cto.blo_st_codigo
                                              , cto.blo_st_nome
                                              , cto.und_in_codigo
                                              , cto.und_st_codigo
                                              , cto.und_st_nome
                                              , cto.cto_in_codigo
                                              , cto.cto_bo_taxaempr
                                              , par_inadimp_anterior.par_in_codigo
                                              , par_inadimp_anterior.par_dt_baixa
                                              , par_inadimp_anterior.par_dt_vencimento
                                              , TRUNC(v_mes_inicial - 1, 'MONTH')
                                              , par_inadimp_anterior.par_ch_parcela
                                              , par_inadimp_anterior.par_re_valororiginal
                                              , 0
                                              , 0
                                              , 0
                                              , 0
                                              , 0
                                              , DECODE( par_inadimp_anterior.par_ch_baixada, 'I', par_inadimp_anterior.par_re_vlrtotalparcela, 0)
                                              , par_inadimp_anterior.par_re_vlrtotalparcela
                                              , par_inadimp_anterior.par_ch_baixada);
        END LOOP;
      END IF;

      FOR par_parcela IN cur_parcelas( cto.org_tab_in_codigo
                                     , cto.org_pad_in_codigo
                                     , cto.org_in_codigo
                                     , cto.org_tau_st_codigo
                                     , cto.cto_in_codigo
                                     , v_mes_inicial
                                     , v_mes_final
                                     , cto.fil_in_codigo
                                     ) LOOP
        INSERT INTO mgcustom.rel_dados_contrato_api( org_tab_in_codigo
                                            , org_pad_in_codigo
                                            , org_in_codigo
                                            , org_tau_st_codigo
                                            , fil_in_codigo
                                            , agn_st_nome
                                            , emp_in_codigo
                                            , emp_st_codigo
                                            , emp_st_nome
                                            , etp_in_codigo
                                            , etp_st_codigo
                                            , etp_st_nome
                                            , blo_in_codigo
                                            , blo_st_codigo
                                            , blo_st_nome
                                            , und_in_codigo
                                            , und_st_codigo
                                            , und_st_nome
                                            , cto_in_codigo
                                            , cto_bo_taxaempr
                                            , par_in_codigo
                                            , par_dt_baixa
                                            , par_dt_vencimento
                                            , par_dt_geracao
                                            , par_ch_parcela
                                            , par_re_valororiginal
                                            , par_re_valor_totalpago
                                            , par_re_credito
                                            , par_re_valoratraso
                                            , par_re_valorvencer
                                            , par_re_saldodevedor
                                            , par_re_saldoquitacao
                                            , par_re_valor_pagocorrigido
                                            , par_ch_origem)

                                     VALUES ( cto.org_tab_in_codigo
                                            , cto.org_pad_in_codigo
                                            , cto.org_in_codigo
                                            , cto.org_tau_st_codigo
                                            , cto.fil_in_codigo
                                            , cto.fil_st_nome
                                            , cto.emp_in_codigo
                                            , cto.emp_st_codigo
                                            , cto.emp_st_nome
                                            , cto.etp_in_codigo
                                            , cto.etp_st_codigo
                                            , cto.etp_st_nome
                                            , cto.blo_in_codigo
                                            , cto.blo_st_codigo
                                            , cto.blo_st_nome
                                            , cto.und_in_codigo
                                            , cto.und_st_codigo
                                            , cto.und_st_nome
                                            , cto.cto_in_codigo
                                            , cto.cto_bo_taxaempr
                                            , par_parcela.par_in_codigo
                                            , par_parcela.par_dt_baixa
                                            , par_parcela.par_dt_vencimento
                                            , v_mes_inicial
                                            , par_parcela.par_ch_parcela
                                            , par_parcela.par_re_valororiginal
                                            , DECODE( par_parcela.par_ch_baixada, 'M', par_parcela.par_re_vlrtotalparcela, 0)
                                            , DECODE( par_parcela.par_ch_baixada, 'A', par_parcela.par_re_vlrtotalparcela, 0)
                                            , DECODE( par_parcela.par_ch_baixada, 'I', par_parcela.par_re_vlrtotalparcela, 0)
                                            , DECODE( par_parcela.par_ch_baixada, 'P', par_parcela.par_re_vlrtotalparcela, 'H', par_parcela.par_re_vlrtotalparcela, 0)
                                            , 0
                                            , 0
                                            , 0
                                            , par_parcela.par_ch_baixada);
      END LOOP;
    END LOOP;
  END LOOP;
  COMMIT;

  OPEN retorno FOR
    SELECT rel.org_tab_in_codigo                            org_tab_in_codigo
         , rel.org_pad_in_codigo                            org_pad_in_codigo
         , rel.org_in_codigo                                org_in_codigo
         , rel.org_tau_st_codigo                            org_tau_st_codigo
         , rel.fil_in_codigo                                fil_in_codigo
         , rel.agn_st_nome                                  fil_st_nome
         , rel.emp_in_codigo                                emp_in_codigo
         , rel.emp_st_codigo                                emp_st_codigo
         , rel.emp_st_nome                                  emp_st_nome
         , rel.etp_in_codigo                                etp_in_codigo
         , rel.etp_st_codigo                                etp_st_codigo
         , rel.etp_st_nome                                  etp_st_nome
         , rel.blo_in_codigo                                blo_in_codigo
         , rel.blo_st_codigo                                blo_st_codigo
         , rel.blo_st_nome                                  blo_st_nome
         , rel.und_in_codigo                                und_in_codigo
         , rel.und_st_codigo                                und_st_codigo
         , rel.und_st_nome                                  und_st_nome
         , rel.cto_in_codigo                                cto_in_codigo
         , rel.cto_bo_taxaempr                              cto_bo_taxaempr
         , rel.par_in_codigo                                par_in_codigo
         , rel.par_dt_baixa                                 par_dt_baixa
         , TRUNC(rel.par_dt_vencimento, 'MONTH')            par_dt_vencimento
         , rel.par_dt_geracao                               par_dt_geracao
         , rel.par_ch_parcela                               par_ch_parcela
         , DECODE(rel.par_ch_parcela, 'N', 'Sinal'
                                    , 'S', 'Sinal'
                                    , 'M', 'Mensal'
                                    , 'I', 'Intermediária'
                                    , 'C', 'Conclusão'
                                    , 'F', 'Financiamento'
                                    , 'T', 'Taxa'
                                    , 'R', 'Resíduo'
                                    , 'B', 'Res.Cobrança')   par_ds_parcela
         , DECODE(rel.par_ch_parcela, 'N', 'A'
                                    , 'S', 'A'
                                    , 'M', 'B'
                                    , 'I', 'C'
                                    , 'C', 'D'
                                    , 'F', 'E'
                                    , 'T', 'F'
                                    , 'R', 'G'
                                    , 'B', 'H')              par_ch_parc_ordem
         , rel.par_re_valororiginal                          par_re_valororiginal
         , DECODE(rel.par_ch_parcela, 'N', 0, rel.par_re_valor_totalpago)                                                                           AS par_re_totalpago
         , SUM( DECODE(rel.par_ch_origem, 'H', 0, NVL( rel.par_re_valor_totalpago, 0))) over (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao)   AS vl_pago_acumulado --(Receita Realizada)
         , DECODE( rel.par_ch_parcela         , 'N', 0, rel.par_re_valorvencer)                                                                     AS par_re_valorvencer
         , SUM( NVL( DECODE(rel.par_ch_parcela, 'N', 0, rel.par_re_valorvencer), 0)) over (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao)      AS vl_avencer_acumulado --(Receita Não Realizada)
         , SUM( NVL( rel.par_re_saldodevedor, 0)) over (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao)                                         AS vl_saldodevedor_acumulado
         , SUM( NVL( DECODE( rel.par_ch_parcela, 'N', rel.par_re_valor_totalpago, 0), 0)) over (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao) AS vl_rea_sinal
         , SUM( NVL( DECODE( rel.par_ch_parcela, 'N', 0, rel.par_re_valor_totalpago), 0)) over (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao) AS vl_rea_mensal
         , SUM( NVL( rel.par_re_valoratraso, 0)) over (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao)                                          AS vl_rea_inadimp
         , SUM( NVL( rel.par_re_credito, 0)) over (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao)                                              AS vl_rea_antecip --Antecipação Efetuada
         , NVL((select SUM( NVL( DECODE(rel2.par_ch_parcela, 'N', 0, DECODE(rel2.par_ch_origem, 'I', NVL(rel2.par_re_saldoquitacao,0), rel2.par_re_valorvencer)), 0))-
                       SUM( DECODE(rel2.par_ch_parcela, 'N', 0, NVL( rel2.par_re_valoratraso, 0)))
                FROM mgcustom.rel_dados_contrato_api rel2
                where rel2.emp_in_codigo = rel.emp_in_codigo
                  and TRUNC( rel2.par_dt_geracao, 'MONTH') <= TRUNC( rel.par_dt_geracao, 'MONTH')
                group by rel2.emp_in_codigo
           ),0) AS vl_vencer_acumulado
         , NVL( ( SELECT SUM(rel2.par_re_saldoquitacao) + SUM(rel2.par_re_valorvencer) +
                         NVL(( SELECT SUM(rel3.par_re_valorvencer)
                               FROM mgcustom.rel_dados_contrato_api rel3
                               WHERE rel3.par_ch_origem = 'H'
                                 AND rel3.emp_in_codigo = rel.emp_in_codigo
                                 AND TRUNC(rel3.par_dt_geracao, 'MONTH') <= TRUNC(rel.par_dt_geracao, 'MONTH')
                               GROUP BY rel3.emp_in_codigo), 0) -
                         NVL( ( SELECT SUM(NVL( rel4.par_re_valoratraso, 0))
                                FROM mgcustom.rel_dados_contrato_api rel4
                                WHERE rel4.emp_in_codigo = rel.emp_in_codigo
                                  AND TRUNC(rel4.par_dt_geracao, 'MONTH') < TRUNC(rel.par_dt_geracao, 'MONTH')
                                GROUP BY rel4.emp_in_codigo), 0) vl_recup_inad_acumulado
                  FROM mgcustom.rel_dados_contrato_api rel2
                  WHERE rel2.emp_in_codigo = rel.emp_in_codigo
                    AND TRUNC(rel2.par_dt_geracao, 'MONTH') <= TRUNC(rel.par_dt_geracao, 'MONTH')
                  GROUP BY rel2.emp_in_codigo), 0) vl_recup_inad_acumulado
         -- Quadro resumo "Previsto"
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'S', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_sinal
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'M', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_mensal
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'I', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_interm
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'C', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_conclu
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'F', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_financ
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'T', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_taxa
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'R', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_residuo
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'B', 1, 0)))) over (PARTITION BY rel.par_ch_parcela) AS qtde_prev_rescob

         -- Quadro resumo "Previsto" e "Realizado"
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'S', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_sinal
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'M', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_mensal
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'I', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_interm
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'C', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_concl
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'F', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_finan
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'T', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_taxa
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'R', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_residuo
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'B', rel.par_re_valor_totalpago+rel.par_re_credito, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_pago_rescob

         -- Quadro resumo "Realizado"
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'S', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_sinal
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'M', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_mensal
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'I', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_interm
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'C', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_concl
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'F', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_finan
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'T', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_taxa
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'R', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_residuo
         , SUM(DECODE(rel.par_ch_origem, 'H', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'B', DECODE(NVL(rel.par_re_valor_totalpago,0)+NVL(rel.par_re_credito,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_pago_rescob

         -- Quadro resumo "Previsto" e "Não Realizado"
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'S', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_sinal
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'M', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_mensal
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'I', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_interm
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'C', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_concl
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'F', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_finan
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'T', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_taxa
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'R', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_residuo
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'B', rel.par_re_valorvencer, 0)))) over (PARTITION BY rel.par_ch_parcela) vl_aber_rescob

         -- Quadro resumo "Não Realizado"
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'S', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_sinal
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'M', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_mensal
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'I', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_interm
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'C', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_concl
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'F', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_finan
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'T', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_taxa
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'R', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_residuo
         , SUM(DECODE(rel.par_ch_origem, 'I', 0, DECODE(SIGN(rel.par_dt_vencimento - v_dt_inicial), -1, 0, DECODE(rel.par_ch_parcela, 'B', DECODE(NVL(rel.par_re_valorvencer,0), 0, 0, 1), 0)))) over (PARTITION BY rel.par_ch_parcela) qtde_aber_rescob

         -- Visão Analítica.
         , NVL( rel.par_re_valor_totalpago, 0) par_re_valor_totalpago
         , NVL( rel.par_re_valoratraso, 0)     par_re_valoratraso
         , NVL( rel.par_re_credito, 0)         par_re_credito
         , rel.par_dt_vencimento               par_dia_vencimento

         , SUM(NVL(rel.par_re_saldoquitacao, 0)) OVER (PARTITION BY rel.emp_in_codigo, rel.par_dt_geracao) par_re_saldoquitacao

         , (SELECT SUM(rel2.par_re_valor_pagocorrigido) + SUM(rel2.par_re_valor_totalpago) + SUM(rel2.par_re_valorvencer) + SUM(rel2.par_re_credito)
            FROM mgcustom.rel_dados_contrato_api rel2
            WHERE rel2.emp_in_codigo = rel.emp_in_codigo
              AND TRUNC(rel2.par_dt_geracao, 'MONTH') <= TRUNC(rel.par_dt_geracao, 'MONTH')
            GROUP BY rel2.emp_in_codigo) par_re_vlracumprevisto

         , rel.par_ch_origem

    FROM mgcustom.rel_dados_contrato_api rel
    ORDER BY rel.par_dt_vencimento;

END PRC_CAR_Previsto_Realizado_Ana;
