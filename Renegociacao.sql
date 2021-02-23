CREATE OR REPLACE PROCEDURE MGREL."PRC_CAR_REL_RENPARCELA" ( retorno IN OUT mgrel.pck_resultado.RESULT
                                                        , org_tab_in_codigo NUMBER
                                                        , org_pad_in_codigo NUMBER
                                                        , org_in_codigo     NUMBER
                                                        , org_tau_st_codigo STRING
                                                        , v_blo_codigo      NUMBER     -- Alterado para o codigo do empreendimento
                                                        , v_tre_bo_status   CHAR
                                                        , v_dt_ren_ini      VARCHAR2
                                                        , v_dt_ren_fim      VARCHAR2
                                                        , v_cto_ch_status   CHAR
                                                        , tipo_relat        VARCHAR2
                                                        , cd_proj           VARCHAR2
                                                        , usu_in_codigo     NUMBER
                                                        , comp_st_nome      VARCHAR2
                                                        , v_sincroniza      CHAR
                                                        , v_bens            CHAR
                                                        , fil_in_codigo     NUMBER
                                                        , p_parc_caucao     CHAR) AS

--------------------------------------------------------------------------------------------------------------------
-- Criada     : 2002
-- Responsavel: Edson
-- Relatório  : R_CAR_Renegociacao.rpt
-- Alterada   : 11/06/2003
-- Responsavel: Edson
--------------------------------------------------------------------------------------------------------------------
-- Parâmetros:
-- v_blo_codigo    = Codigo do empreendimento (0 - todos)
-- v_tre_bo_status = Status da renegociacao - T - Todas
--                                            A - Aprovada
--                                            E - Em estudo
--                                            R - Reprovada
-- v_dt_ren_ini    = Data inicial da renegociacao
-- v_dt_ren_fim    = Data final da renegociacao
-- v_cto_ch_status = Status atual do contrato - TUDO - Todas
--                                              A - Ativo
--                                              C - Cessao de direitos
--                                              D - Distratado
--                                              U - Inadimplente
--                                              Q - Quitado
--                                              T - Transferido
-- tipo_relat      = Tipo do relatório E - Empreendimento, P - Projeto
-- cd_proj         = Codigo do projeto
-- v_sincroniza: Considera Empreendimentos Sincronizados: 'N' - Não
--                                                        'S' - Sim
--                                                        'T' - Todos
-- v_bens: Considerar Bens de Terceiros: 'B' - Sim
--                                       'X' - Não
-- p_parc_caucao    : Tipo Parcela:
--                    Todas           "T"
--                    Caucionadas     "C"
--                    Não Caucionadas "N"
--------------------------------------------------------------------------------------------------------------------
-- Alterações :
-- 11/06/2003 - Edson: Foi alterada o retorno dos dados para a package padrão.
--                     Alterado o parametro v_cto_ch_status para aceitar todos os status de contrato.
--                     Alterado o parametro v_tre_bo_status para aceitar todos os status de renegociacao
--                     Feito alguns ajustes no group by dos select's para considerar todos os empreendimentos.
-- 26/09/2003 - Janaina : Formato estava trazendo errado vl_base de 'NTP' na emissao por projeto.
-- 13/04/2005 - Cleiton : Formato estava trazendo errado vl_base de 'NTP' na emissao para todos os
--                        Empreendimentos, alterado alias das tabelas e reorganizado o select.ch 7036.
-- 18/12/2006 - Eduardo Correa: Redesenvolvida a procedure para melhor manutenção e ralizado tratamento para buscar somente
--                              registros de filiais ativas Ch. 13709
-- 12/03/2008 - Paulo Chaves: corrigido retorno dos campos "vl_futuro" e "vl_base" para que seja
--                            multiplicado pelo campo "cndit_in_parcela" para retornar o valor total da
--                            série. Retirado a utilização da view mgcar.car_vw_contrato pois continha
--                            problemas de performance. Inserido filtro por código de empreendimento no
--                            select de contratos com parcelas TP. Inserido o campo ren_obs. Ch 19975
-- 30/04/2008 - Eliani Andrade: Inserida a função para tratamento de empreendimentos sincronizados. ch. 21145
-- 17/11/2008 - Eduardo N Santos: - Inserido o parâmetro para considerar bens de terceiro
--                                - Inserido decode que relaciona a tabela mgcar.car_contrato_envolvido com a view mgrel.vw_glo_estrutura
--                                - Alterado selects que utilizavam a view mgrel.vw_rel_estrutura_contrato para mgrel.vw_glo_estrutura
--                                - Inserido tratamento nvl() para o campo est.emp_in_codigo.
--                                - Deletado a condição if, que separava os selects em  emissão por empreendimento ou por projeto, realizado este tratamento
--                                  usando filtros no where.Ch. 260780
-- 20/02/2009 - Eduardo Santos : Corridigo calculo do vl_base para parcelas nao tp: (cdi.cndit_re_valorbase * cdi.cndit_in_parcela) vl_base Ch. 28307
-- 27/04/2010 - Marino: Adequado procedure ao processo SAC. Ch. 32972.
--                      Inserida a função NVL em campos somatórios que poderiam ficar incorretos quando um dos fatores fosse nulo. (Fatos Relevantes V03 Item 3 Todos os Módulos)
--                      Alterado o filtro de projeto para considerar o código extenso. (Fatos Relevantes V03 Item 6 Todos os Módulos)
--                      Alterada a view "mgrel.vw_glo_estrutura" para a view "mgrel.vw_car_estrutura". (Fatos Relevantes V03 Item 7 Mód. Carteira)
-- 25/04/2011 - Eliani: Incluso o parâmetro "p_parc_caucao", bem como o tratamento para considerar contratos com parcelas caucionadas, não caucionadas
--              ou todos. Excluída a tabela "mgcar.car_parcela", so select de renegociação de parcelas SACOC. Ch. 43411
-- 19/05/2011 - Richard: Retirada a tabela "mgcar.car_caucao_parcela", do select de renegociação de parcelas SACOC. Ch. 46463.
-- 11/10/2012 - Guilherme Chiconato: No select que tras parcelas renegociadas do tipo SACOC, foi colocada a condição_item em um sub-select. Ch. 361556
-- 23/10/2012 - Guilherme Chiconato: Removido o sub-select de condição_item e adicionado no group by os campo cnd_in_codigo e cndit_in_codigo
--                                   Adicionado o sub-select ant para renegociações de tabela price para calcular o saldo corrigido com base na somatória das parcelas
--                                   Adicionado mais um select para renegociações de tabela price para multiplicar o valor_base pelo número de parcelas quando a data
--                                   de amortização for maior que a data da renegociação. Ch. 362894
-- 18/02/2016 - Peterson R. de Pauli: Corrigido chamadas da fnc_car_corrige_serietp_ren passando tre_re_taxa ao invés do tre_re_jroren. PRP 18/02/2016 PINC-3133
-- 03/07/2017 - André Luiz Carraro: Corrigido SUM no Select de renegociação TP. PINC-4584
--------------------------------------------------------------------------------------------------------------------

v_dt_ini      DATE;
v_dt_fim      DATE;
tab           NUMBER(22);
pad           NUMBER(22);
--cod           NUMBER(22);
tau           VARCHAR2(3);
org_cod       VARCHAR2(4000);
fil_cod       VARCHAR2(4000);
usu           NUMBER(22);
comp          VARCHAR2(100);
v_sinc        CHAR(1);
bens          CHAR(1);
v_fil         NUMBER(7);
v_pad         NUMBER(3);
v_pro_ext     VARCHAR2(25);
v_parc_caucao CHAR(1);

CURSOR org_emp IS
  SELECT Decode(agn.agn_bo_consolidador, 'E', agn.agn_in_codigo
                                            , agn.pai_agn_in_codigo) org_in_codigo
                                            , agn.agn_in_codigo     fil_in_codigo
  FROM mgglo.glo_filial_ativa fil
     , mgglo.glo_agentes      agn
  WHERE fil.usu_in_codigo = usu
    AND fil.comp_st_nome  = comp
    AND agn.agn_tab_in_codigo = fil.fil_tab_in_codigo
    AND agn.agn_pad_in_codigo = fil.fil_pad_in_codigo
    AND agn.agn_in_codigo     = fil.fil_in_codigo
    AND agn.agn_bo_consolidador IN ('E', 'F');

BEGIN

  IF tipo_relat = 'P' THEN

    v_fil     := fil_in_codigo;
    -- intervalo de projetos
    v_pro_ext := cd_proj;

    -- selecionar o projeto padrão - v_pad
    BEGIN
      -- Select para informar o projeto padrão
      SELECT def.pad_in_codigo
      INTO v_pad
      FROM mgglo.glo_padrao    pdr
         , mgglo.glo_tabela    tbl
         , mgglo.glo_definicao def
      WHERE def.fil_in_codigo = v_fil
        AND tbl.tab_in_codigo = 57 -- código fixo da tabela de projeto
        AND def.tab_in_codigo = tbl.tab_in_codigo
        AND pdr.tab_in_codigo = def.tab_in_codigo
        AND pdr.pad_in_codigo = def.pad_in_codigo
        AND def.def_dt_inicio = (SELECT MAX(dfi.def_dt_inicio)
                                 FROM mgglo.glo_definicao dfi
                                 WHERE dfi.org_tab_in_codigo = def.org_tab_in_codigo
                                   AND dfi.org_pad_in_codigo = def.org_pad_in_codigo
                                   AND dfi.org_in_codigo     = def.org_in_codigo
                                   AND dfi.org_tau_st_codigo = def.org_tau_st_codigo
                                   AND dfi.fil_in_codigo     = def.fil_in_codigo
                                   AND dfi.tab_in_codigo     = def.tab_in_codigo
                                   AND dfi.def_dt_inicio    <= trunc(SYSDATE));
    EXCEPTION WHEN OTHERS THEN
      v_pad := 1;
    END;

  ELSE
    tab := org_tab_in_codigo;
    pad := org_pad_in_codigo;
    --cod := org_in_codigo;
    tau := org_tau_st_codigo;
  END IF;

  v_dt_ini := TO_DATE(v_dt_ren_ini, 'DD/MM/YYYY');
  v_dt_fim := TO_DATE(v_dt_ren_fim, 'DD/MM/YYYY');
  comp     := comp_st_nome;
  usu      := usu_in_codigo;
  org_cod  := '#';
  fil_cod  := '#';
  v_sinc   := v_sincroniza;
  bens     := v_bens;
  v_parc_caucao := p_parc_caucao;

  FOR oem IN org_emp LOOP
    org_cod := org_cod || TO_CHAR(oem.org_in_codigo) || '#';
    fil_cod := fil_cod || TO_CHAR(oem.fil_in_codigo) || '#';
  END LOOP;

  OPEN retorno FOR
    -- Select de contratos que possuem parcelas renegociadas do tipo TP
    SELECT ren.cto_in_codigo
         , ren.tre_dt_renegociacao re_dt_renegociacao
         , ren.tre_bo_status re_bo_status
         , ren.tre_in_codigo re_in_codigo
         , NVL(ren.tre_in_parrenegociadas,0)                                           par_ren_tp
         , NVL(ren.tre_re_vlrparren,0)                                                 vlr_ren_tp
         , pro.pro_in_reduzido
         , pro.pro_st_extenso
         , pro.pro_st_descricao
         , agn.agn_st_nome
         , est.blo_in_codigo                                                           blo_codigo
         , est.blo_st_codigo                                                           blo_st_codigo
         , est.blo_st_nome                                                             blo_nome
         , est.emp_st_codigo                                                           emp_st_codigo
         , est.emp_st_nome                                                             emp_nome
         , est.und_st_codigo                                                           und_st_codigo
         , NVL(est.emp_in_codigo,0)                                                    emp_codigo
         , cdi.cndit_in_parcela
         , cdi.cndit_st_observacao
         , SUM(cdi.cndit_re_valorbase)                                                 vl_base
         , SUM(cdi.cndit_re_valorfuturo)     vl_futuro
         , mgrel.pck_rel_fnc.fnc_car_corrige_serietp_ren( ren.org_tab_in_codigo
                                                        , ren.org_pad_in_codigo
                                                        , ren.org_in_codigo
                                                        , ren.org_tau_st_codigo
                                                        , ren.cnd_in_codigo
                                                        , ren.cndit_in_codigo
                                                        , ren.tre_dt_renegociacao
                                                        , 'RP'
                                                        , NVL(ren.tre_bo_multa,'N')
                                                        , NVL(ren.tre_bo_juros,'N')
                                                        , NVL(ren.tre_re_taxa,-999)) sld_renegociar
         , 'TP' tp_tabela
         , NVL(cdi.cndit_bo_statustp,'N')                                              Status_Aplic_TP
         , NVL(cdi.cndit_re_txtabprice,0)                                              Taxa_TP
         , ren.tre_re_saldoren                                                         sld_renegociar2
         , est.estrutura
         , CAST('' AS VARCHAR2 (220))                                                  ren_obs
         , NVL(pla.reneg_cont_a, 0)                                                    reneg_cont_a
         , NVL(pla.reneg_cont_b, 0)                                                    reneg_cont_b
         , ant.vlr_corrigido_data

    FROM (SELECT cpl.org_tab_in_codigo
               , cpl.org_pad_in_codigo
               , cpl.org_in_codigo
               , cpl.org_tau_st_codigo
               , cpl.cto_in_codigo
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'A', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_a
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'B', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_b
          FROM mgcar.car_contrato_planilha cpl
          WHERE cpl.pla_ch_tipomovimento IN ('A', 'B')
            AND cpl.pla_dt_movimento >= v_dt_fim
          GROUP BY cpl.org_tab_in_codigo
                 , cpl.org_pad_in_codigo
                 , cpl.org_in_codigo
                 , cpl.org_tau_st_codigo
                 , cpl.cto_in_codigo)  pla
       , ( SELECT cto.org_tab_in_codigo
                , cto.org_pad_in_codigo
                , cto.org_in_codigo
                , cto.org_tau_st_codigo
                , cto.cto_in_codigo
                , ren.cnd_in_codigo
                , ren.cndit_in_codigo
                , ren.tre_in_codigo
                , SUM( ROUND( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                               , par.org_pad_in_codigo
                                                               , par.org_in_codigo
                                                               , par.org_tau_st_codigo
                                                               , par.cto_in_codigo
                                                               , par.par_in_codigo
                                                               , ren.tre_dt_renegociacao
                                                               , 'RP'
                                                               , DECODE('S', 'N', 'M'
                                                                                   , 'S', 'A'
                                                                                        , 'A')
                                                               , -1), 2)) vlr_corrigido_data
           FROM mgcar.car_tabprice_ren         ren
              , mgcar.car_tabprice_ren_parcela rep
              , mgcar.car_parcela              par
              , mgcar.car_contrato             cto

           WHERE cto.org_tab_in_codigo = decode(tipo_relat, 'E', tab, cto.org_tab_in_codigo)
             AND cto.org_pad_in_codigo = decode(tipo_relat, 'E', pad, cto.org_pad_in_codigo)
             AND org_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.org_in_codigo) || '#%', org_cod)
             AND fil_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.fil_in_codigo) || '#%', fil_cod)
             AND cto.org_tau_st_codigo = decode(tipo_relat, 'E', tau, cto.org_tau_st_codigo)

             AND cto.org_tab_in_codigo = par.org_tab_in_codigo
             AND cto.org_pad_in_codigo = par.org_pad_in_codigo
             AND cto.org_in_codigo     = par.org_in_codigo
             AND cto.org_tau_st_codigo = par.org_tau_st_codigo
             AND cto.cto_in_codigo     = par.cto_in_codigo

             AND par.org_tab_in_codigo = rep.org_tab_in_codigo
             AND par.org_pad_in_codigo = rep.org_pad_in_codigo
             AND par.org_in_codigo     = rep.org_in_codigo
             AND par.org_tau_st_codigo = rep.org_tau_st_codigo
             AND par.cto_in_codigo     = rep.cto_in_codigo
             AND par.par_in_codigo     = rep.par_in_codigo

             AND rep.org_tab_in_codigo = ren.org_tab_in_codigo
             AND rep.org_pad_in_codigo = ren.org_pad_in_codigo
             AND rep.org_in_codigo     = ren.org_in_codigo
             AND rep.org_tau_st_codigo = ren.org_tau_st_codigo
             AND rep.cto_in_codigo     = ren.cto_in_codigo
             AND rep.cnd_in_codigo     = ren.cnd_in_codigo
             AND rep.cndit_in_codigo   = ren.cndit_in_codigo
             AND rep.tre_in_codigo     = ren.tre_in_codigo

           GROUP BY cto.org_tab_in_codigo
                  , cto.org_pad_in_codigo
                  , cto.org_in_codigo
                  , cto.org_tau_st_codigo
                  , cto.cto_in_codigo
                  , ren.cnd_in_codigo
                  , ren.cndit_in_codigo
                  , ren.tre_in_codigo) ant
       , mgglo.glo_projetos            pro
       , mgdbm.dbm_condicao_item       cdi
       , mgcar.car_tabprice_ren_series ser
       , mgcar.car_tabprice_ren        ren
       , mgglo.glo_agentes             agn
       , mgglo.glo_agentes_id          aid
       , mgcar.car_contrato_cliente    cli
       , mgcar.car_contrato            cto
       , mgrel.vw_car_estrutura        est

    WHERE cto.org_tab_in_codigo = decode(tipo_relat, 'E', tab, cto.org_tab_in_codigo)
      AND cto.org_pad_in_codigo = decode(tipo_relat, 'E', pad, cto.org_pad_in_codigo)
      AND org_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.org_in_codigo) || '#%', org_cod)
      AND fil_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.fil_in_codigo) || '#%', fil_cod)
      AND cto.org_tau_st_codigo = decode(tipo_relat, 'E', tau, cto.org_tau_st_codigo)

      AND pro.pro_pad_in_codigo = decode(tipo_relat, 'P', v_pad, pro.pro_pad_in_codigo)
      AND pro.pro_st_extenso    = decode(tipo_relat, 'P', v_pro_ext, pro.pro_st_extenso)

      AND cto.cto_ch_status = DECODE(v_cto_ch_status, 'TUDO', cto.cto_ch_status, v_cto_ch_status)

      AND ( est.emp_in_codigo = decode(tipo_relat, 'E', decode(v_blo_codigo, 0, est.emp_in_codigo, v_blo_codigo), est.emp_in_codigo)
          OR est.emp_in_codigo IS NULL )

      AND NVL(mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                               , est.org_pad_in_codigo
                                                               , est.org_in_codigo
                                                               , est.org_tau_st_codigo
                                                               , nvl(est.emp_in_codigo,0)
                                                               , v_sinc
                                                               ), 0) = nvl(est.emp_in_codigo,0)

      AND est.estrutura IN ('U', 'G', bens)

      AND ren.tre_dt_renegociacao >= v_dt_ini
      AND ren.tre_dt_renegociacao <= v_dt_fim
      AND ren.tre_bo_status        = DECODE(v_tre_bo_status, 'T', ren.tre_bo_status, v_tre_bo_status)

      AND ((  cdi.cndit_dt_referenciatp < ren.tre_dt_renegociacao) OR cdi.cndit_dt_referenciatp IS NULL)

      --  contratos com parcelas caucionadas
      AND ( ( v_parc_caucao = 'C' AND EXISTS( SELECT cau.ctc_in_codigo
                                              FROM mgcar.car_caucao_parcela cau
                                                 , mgcar.car_parcela        par
                                              WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                AND par.org_in_codigo     = cto.org_in_codigo
                                                AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                AND par.cto_in_codigo     = cto.cto_in_codigo

                                                AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                AND cau.org_in_codigo     = par.org_in_codigo
                                                AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                AND cau.cto_in_codigo     = par.cto_in_codigo
                                                AND cau.par_in_codigo     = par.par_in_codigo))
       -- contratos sem parcelas caucionada
        OR ( v_parc_caucao = 'N' AND NOT EXISTS( SELECT cau.ctc_in_codigo
                                                 FROM mgcar.car_caucao_parcela cau
                                                    , mgcar.car_parcela        par
                                                 WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                   AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                   AND par.org_in_codigo     = cto.org_in_codigo
                                                   AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                   AND par.cto_in_codigo     = cto.cto_in_codigo

                                                   AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                   AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                   AND cau.org_in_codigo     = par.org_in_codigo
                                                   AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                   AND cau.cto_in_codigo     = par.cto_in_codigo
                                                   AND cau.par_in_codigo     = par.par_in_codigo ))
        -- todos os contratos
        OR ( v_parc_caucao = 'T'))

      AND est.cto_org_tab_in_codigo = cto.org_tab_in_codigo
      AND est.cto_org_pad_in_codigo = cto.org_pad_in_codigo
      AND est.cto_org_in_codigo     = cto.org_in_codigo
      AND est.cto_org_tau_st_codigo = cto.org_tau_st_codigo
      AND est.cto_in_codigo         = cto.cto_in_codigo

      AND cli.org_tab_in_codigo = cto.org_tab_in_codigo
      AND cli.org_pad_in_codigo = cto.org_pad_in_codigo
      AND cli.org_in_codigo     = cto.org_in_codigo
      AND cli.org_tau_st_codigo = cto.org_tau_st_codigo
      AND cli.cto_in_codigo     = cto.cto_in_codigo

      AND aid.agn_tab_in_codigo = cli.agn_tab_in_codigo
      AND aid.agn_pad_in_codigo = cli.agn_pad_in_codigo
      AND aid.agn_in_codigo     = cli.agn_in_codigo
      AND aid.agn_tau_st_codigo = cli.agn_tau_st_codigo

      AND agn.agn_tab_in_codigo = aid.agn_tab_in_codigo
      AND agn.agn_pad_in_codigo = aid.agn_pad_in_codigo
      AND agn.agn_in_codigo     = aid.agn_in_codigo

      AND ren.org_tab_in_codigo = cto.org_tab_in_codigo
      AND ren.org_pad_in_codigo = cto.org_pad_in_codigo
      AND ren.org_in_codigo     = cto.org_in_codigo
      AND ren.org_tau_st_codigo = cto.org_tau_st_codigo
      AND ren.cto_in_codigo     = cto.cto_in_codigo

      AND ren.org_tab_in_codigo = ant.org_tab_in_codigo
      AND ren.org_pad_in_codigo = ant.org_pad_in_codigo
      AND ren.org_in_codigo     = ant.org_in_codigo
      AND ren.org_tau_st_codigo = ant.org_tau_st_codigo
      AND ren.cto_in_codigo     = ant.cto_in_codigo
      AND ren.cnd_in_codigo     = ant.cnd_in_codigo
      AND ren.cndit_in_codigo   = ant.cndit_in_codigo
      AND ren.tre_in_codigo     = ant.tre_in_codigo

      AND ser.org_tab_in_codigo = ren.org_tab_in_codigo
      AND ser.org_pad_in_codigo = ren.org_pad_in_codigo
      AND ser.org_in_codigo     = ren.org_in_codigo
      AND ser.org_tau_st_codigo = ren.org_tau_st_codigo
      AND ser.cnd_in_codigo     = ren.cnd_in_codigo
      AND ser.cndit_in_codigo   = ren.cndit_in_codigo
      AND ser.cto_in_codigo     = ren.cto_in_codigo

      AND cdi.org_tab_in_codigo = ser.org_tab_in_codigo
      AND cdi.org_pad_in_codigo = ser.org_pad_in_codigo
      AND cdi.org_in_codigo     = ser.org_in_codigo
      AND cdi.org_tau_st_codigo = ser.org_tau_st_codigo
      AND cdi.cnd_in_codigo     = ser.cnd_in_codigo
      AND cdi.cndit_in_codigo   = ser.tre_in_novaserie

      AND pro.pro_tab_in_codigo = cto.pro_tab_in_codigo
      AND pro.pro_pad_in_codigo = cto.pro_pad_in_codigo
      AND pro.pro_ide_st_codigo = cto.pro_ide_st_codigo
      AND pro.pro_in_reduzido   = cto.pro_in_reduzido

      AND pla.org_tab_in_codigo (+)= cto.org_tab_in_codigo
      AND pla.org_pad_in_codigo (+)= cto.org_pad_in_codigo
      AND pla.org_in_codigo     (+)= cto.org_in_codigo
      AND pla.org_tau_st_codigo (+)= cto.org_tau_st_codigo
      AND pla.cto_in_codigo     (+)= cto.cto_in_codigo

    GROUP BY ren.cto_in_codigo
         , ren.tre_dt_renegociacao
         , ren.tre_bo_status
         , ren.tre_in_codigo
         , NVL(ren.tre_in_parrenegociadas,0)
         , NVL(ren.tre_re_vlrparren,0)
         , pro.pro_in_reduzido
         , pro.pro_st_extenso
         , pro.pro_st_descricao
         , agn.agn_st_nome
         , est.blo_in_codigo
         , est.blo_st_codigo
         , est.blo_st_nome
         , est.emp_st_codigo
         , est.emp_st_nome
         , est.und_st_codigo
         , nvl(est.emp_in_codigo,0)
         , cdi.cnd_in_codigo
         , cdi.cndit_in_codigo
         , cdi.cndit_in_parcela
         , cdi.cndit_st_observacao
         , (cdi.cndit_re_valorbase)
         , (cdi.cndit_re_valorfuturo)
         , mgrel.pck_rel_fnc.fnc_car_corrige_serietp_ren( ren.org_tab_in_codigo
                                                        , ren.org_pad_in_codigo
                                                        , ren.org_in_codigo
                                                        , ren.org_tau_st_codigo
                                                        , ren.cnd_in_codigo
                                                        , ren.cndit_in_codigo
                                                        , ren.tre_dt_renegociacao
                                                        , 'RP'
                                                        , nvl(ren.tre_bo_multa,'N')
                                                        , nvl(ren.tre_bo_juros,'N')
                                                        , nvl(ren.tre_re_taxa,-999))
         , 'TP'
         , NVL(cdi.cndit_bo_statustp,'N')
         , NVL(cdi.cndit_re_txtabprice,0)
         , ren.tre_re_saldoren
         , est.estrutura
         , CAST('' AS VARCHAR2 (220))
         , NVL(pla.reneg_cont_a, 0)
         , NVL(pla.reneg_cont_b, 0)
         , ant.vlr_corrigido_data

    UNION ALL

    SELECT ren.cto_in_codigo
         , ren.tre_dt_renegociacao re_dt_renegociacao
         , ren.tre_bo_status re_bo_status
         , ren.tre_in_codigo re_in_codigo
         , NVL(ren.tre_in_parrenegociadas,0)                                           par_ren_tp
         , NVL(ren.tre_re_vlrparren,0)                                                 vlr_ren_tp
         , pro.pro_in_reduzido
         , pro.pro_st_extenso
         , pro.pro_st_descricao
         , agn.agn_st_nome
         , est.blo_in_codigo                                                           blo_codigo
         , est.blo_st_codigo                                                           blo_st_codigo
         , est.blo_st_nome                                                             blo_nome
         , est.emp_st_codigo                                                           emp_st_codigo
         , est.emp_st_nome                                                             emp_nome
         , est.und_st_codigo                                                           und_st_codigo
         , nvl(est.emp_in_codigo,0)                                                    emp_codigo
         , cdi.cndit_in_parcela
         , cdi.cndit_st_observacao
         , SUM(cdi.cndit_re_valorbase)                                                 vl_base
         , SUM(cdi.cndit_re_valorfuturo)                                               vl_futuro
         , mgrel.pck_rel_fnc.fnc_car_corrige_serietp_ren( ren.org_tab_in_codigo
                                                        , ren.org_pad_in_codigo
                                                        , ren.org_in_codigo
                                                        , ren.org_tau_st_codigo
                                                        , ren.cnd_in_codigo
                                                        , ren.cndit_in_codigo
                                                        , ren.tre_dt_renegociacao
                                                        , 'RP'
                                                        , nvl(ren.tre_bo_multa,'N')
                                                        , nvl(ren.tre_bo_juros,'N')
                                                        , nvl(ren.tre_re_taxa,-999)) sld_renegociar
         , 'TP' tp_tabela
         , NVL(cdi.cndit_bo_statustp,'N')                                              Status_Aplic_TP
         , NVL(cdi.cndit_re_txtabprice,0)                                              Taxa_TP
         , ren.tre_re_saldoren                                                         sld_renegociar2
         , est.estrutura
         , CAST('' AS VARCHAR2 (220))                                                  ren_obs
         , NVL(pla.reneg_cont_a, 0)                                                    reneg_cont_a
         , NVL(pla.reneg_cont_b, 0)                                                    reneg_cont_b
         , ant.vlr_corrigido_data

    FROM (SELECT cpl.org_tab_in_codigo
               , cpl.org_pad_in_codigo
               , cpl.org_in_codigo
               , cpl.org_tau_st_codigo
               , cpl.cto_in_codigo
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'A', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_a
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'B', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_b
          FROM mgcar.car_contrato_planilha cpl
          WHERE cpl.pla_ch_tipomovimento IN ('A', 'B')
            AND cpl.pla_dt_movimento >= v_dt_fim
          GROUP BY cpl.org_tab_in_codigo
                 , cpl.org_pad_in_codigo
                 , cpl.org_in_codigo
                 , cpl.org_tau_st_codigo
                 , cpl.cto_in_codigo)  pla
       , ( SELECT cto.org_tab_in_codigo
                , cto.org_pad_in_codigo
                , cto.org_in_codigo
                , cto.org_tau_st_codigo
                , cto.cto_in_codigo
                , ren.cnd_in_codigo
                , ren.cndit_in_codigo
                , ren.tre_in_codigo
                , SUM( ROUND( mgcar.pck_car_fnc.fnc_car_corrige( par.org_tab_in_codigo
                                                               , par.org_pad_in_codigo
                                                               , par.org_in_codigo
                                                               , par.org_tau_st_codigo
                                                               , par.cto_in_codigo
                                                               , par.par_in_codigo
                                                               , ren.tre_dt_renegociacao
                                                               , 'RP'
                                                               , DECODE('S', 'N', 'M'
                                                                                   , 'S', 'A'
                                                                                        , 'A')
                                                               , -1), 2)) vlr_corrigido_data
           FROM mgcar.car_tabprice_ren         ren
              , mgcar.car_tabprice_ren_parcela rep
              , mgcar.car_parcela              par
              , mgcar.car_contrato             cto

           WHERE cto.org_tab_in_codigo = decode(tipo_relat, 'E', tab, cto.org_tab_in_codigo)
             AND cto.org_pad_in_codigo = decode(tipo_relat, 'E', pad, cto.org_pad_in_codigo)
             AND org_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.org_in_codigo) || '#%', org_cod)
             AND fil_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.fil_in_codigo) || '#%', fil_cod)
             AND cto.org_tau_st_codigo = decode(tipo_relat, 'E', tau, cto.org_tau_st_codigo)

             AND cto.org_tab_in_codigo = par.org_tab_in_codigo
             AND cto.org_pad_in_codigo = par.org_pad_in_codigo
             AND cto.org_in_codigo     = par.org_in_codigo
             AND cto.org_tau_st_codigo = par.org_tau_st_codigo
             AND cto.cto_in_codigo     = par.cto_in_codigo

             AND par.org_tab_in_codigo = rep.org_tab_in_codigo
             AND par.org_pad_in_codigo = rep.org_pad_in_codigo
             AND par.org_in_codigo     = rep.org_in_codigo
             AND par.org_tau_st_codigo = rep.org_tau_st_codigo
             AND par.cto_in_codigo     = rep.cto_in_codigo
             AND par.par_in_codigo     = rep.par_in_codigo

             AND rep.org_tab_in_codigo = ren.org_tab_in_codigo
             AND rep.org_pad_in_codigo = ren.org_pad_in_codigo
             AND rep.org_in_codigo     = ren.org_in_codigo
             AND rep.org_tau_st_codigo = ren.org_tau_st_codigo
             AND rep.cto_in_codigo     = ren.cto_in_codigo
             AND rep.cnd_in_codigo     = ren.cnd_in_codigo
             AND rep.cndit_in_codigo   = ren.cndit_in_codigo
             AND rep.tre_in_codigo     = ren.tre_in_codigo

           GROUP BY cto.org_tab_in_codigo
                  , cto.org_pad_in_codigo
                  , cto.org_in_codigo
                  , cto.org_tau_st_codigo
                  , cto.cto_in_codigo
                  , ren.cnd_in_codigo
                  , ren.cndit_in_codigo
                  , ren.tre_in_codigo) ant
       , mgglo.glo_projetos            pro
       , mgdbm.dbm_condicao_item       cdi
       , mgcar.car_tabprice_ren_series ser
       , mgcar.car_tabprice_ren        ren
       , mgglo.glo_agentes             agn
       , mgglo.glo_agentes_id          aid
       , mgcar.car_contrato_cliente    cli
       , mgcar.car_contrato            cto
       , mgrel.vw_car_estrutura        est

    WHERE cto.org_tab_in_codigo = decode(tipo_relat, 'E', tab, cto.org_tab_in_codigo)
      AND cto.org_pad_in_codigo = decode(tipo_relat, 'E', pad, cto.org_pad_in_codigo)
      AND org_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.org_in_codigo) || '#%', org_cod)
      AND fil_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.fil_in_codigo) || '#%', fil_cod)
      AND cto.org_tau_st_codigo = decode(tipo_relat, 'E', tau, cto.org_tau_st_codigo)

      AND pro.pro_pad_in_codigo = decode(tipo_relat, 'P', v_pad, pro.pro_pad_in_codigo)
      AND pro.pro_st_extenso    = decode(tipo_relat, 'P', v_pro_ext, pro.pro_st_extenso)

      AND cto.cto_ch_status = DECODE(v_cto_ch_status, 'TUDO', cto.cto_ch_status, v_cto_ch_status)

      AND ( est.emp_in_codigo = decode(tipo_relat, 'E', decode(v_blo_codigo, 0, est.emp_in_codigo, v_blo_codigo), est.emp_in_codigo)
          OR est.emp_in_codigo IS NULL )

      AND NVL(mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                               , est.org_pad_in_codigo
                                                               , est.org_in_codigo
                                                               , est.org_tau_st_codigo
                                                               , nvl(est.emp_in_codigo,0)
                                                               , v_sinc
                                                               ), 0) = nvl(est.emp_in_codigo,0)

      AND est.estrutura IN ('U', 'G', bens)

      AND ren.tre_dt_renegociacao >= v_dt_ini
      AND ren.tre_dt_renegociacao <= v_dt_fim
      AND ren.tre_bo_status        = DECODE(v_tre_bo_status, 'T', ren.tre_bo_status, v_tre_bo_status)

      AND ( cdi.cndit_dt_referenciatp >= ren.tre_dt_renegociacao)

      --  contratos com parcelas caucionadas
      AND ( ( v_parc_caucao = 'C' AND EXISTS( SELECT cau.ctc_in_codigo
                                              FROM mgcar.car_caucao_parcela cau
                                                 , mgcar.car_parcela        par
                                              WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                AND par.org_in_codigo     = cto.org_in_codigo
                                                AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                AND par.cto_in_codigo     = cto.cto_in_codigo

                                                AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                AND cau.org_in_codigo     = par.org_in_codigo
                                                AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                AND cau.cto_in_codigo     = par.cto_in_codigo
                                                AND cau.par_in_codigo     = par.par_in_codigo))
       -- contratos sem parcelas caucionada
        OR ( v_parc_caucao = 'N' AND NOT EXISTS( SELECT cau.ctc_in_codigo
                                                 FROM mgcar.car_caucao_parcela cau
                                                    , mgcar.car_parcela        par
                                                 WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                   AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                   AND par.org_in_codigo     = cto.org_in_codigo
                                                   AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                   AND par.cto_in_codigo     = cto.cto_in_codigo

                                                   AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                   AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                   AND cau.org_in_codigo     = par.org_in_codigo
                                                   AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                   AND cau.cto_in_codigo     = par.cto_in_codigo
                                                   AND cau.par_in_codigo     = par.par_in_codigo ))
        -- todos os contratos
        OR ( v_parc_caucao = 'T'))

      AND est.cto_org_tab_in_codigo = cto.org_tab_in_codigo
      AND est.cto_org_pad_in_codigo = cto.org_pad_in_codigo
      AND est.cto_org_in_codigo     = cto.org_in_codigo
      AND est.cto_org_tau_st_codigo = cto.org_tau_st_codigo
      AND est.cto_in_codigo         = cto.cto_in_codigo

      AND cli.org_tab_in_codigo = cto.org_tab_in_codigo
      AND cli.org_pad_in_codigo = cto.org_pad_in_codigo
      AND cli.org_in_codigo     = cto.org_in_codigo
      AND cli.org_tau_st_codigo = cto.org_tau_st_codigo
      AND cli.cto_in_codigo     = cto.cto_in_codigo

      AND aid.agn_tab_in_codigo = cli.agn_tab_in_codigo
      AND aid.agn_pad_in_codigo = cli.agn_pad_in_codigo
      AND aid.agn_in_codigo     = cli.agn_in_codigo
      AND aid.agn_tau_st_codigo = cli.agn_tau_st_codigo

      AND agn.agn_tab_in_codigo = aid.agn_tab_in_codigo
      AND agn.agn_pad_in_codigo = aid.agn_pad_in_codigo
      AND agn.agn_in_codigo     = aid.agn_in_codigo

      AND ren.org_tab_in_codigo = cto.org_tab_in_codigo
      AND ren.org_pad_in_codigo = cto.org_pad_in_codigo
      AND ren.org_in_codigo     = cto.org_in_codigo
      AND ren.org_tau_st_codigo = cto.org_tau_st_codigo
      AND ren.cto_in_codigo     = cto.cto_in_codigo

      AND ren.org_tab_in_codigo = ant.org_tab_in_codigo
      AND ren.org_pad_in_codigo = ant.org_pad_in_codigo
      AND ren.org_in_codigo     = ant.org_in_codigo
      AND ren.org_tau_st_codigo = ant.org_tau_st_codigo
      AND ren.cto_in_codigo     = ant.cto_in_codigo
      AND ren.cnd_in_codigo     = ant.cnd_in_codigo
      AND ren.cndit_in_codigo   = ant.cndit_in_codigo
      AND ren.tre_in_codigo     = ant.tre_in_codigo

      AND ser.org_tab_in_codigo = ren.org_tab_in_codigo
      AND ser.org_pad_in_codigo = ren.org_pad_in_codigo
      AND ser.org_in_codigo     = ren.org_in_codigo
      AND ser.org_tau_st_codigo = ren.org_tau_st_codigo
      AND ser.cnd_in_codigo     = ren.cnd_in_codigo
      AND ser.cndit_in_codigo   = ren.cndit_in_codigo
      AND ser.cto_in_codigo     = ren.cto_in_codigo

      AND cdi.org_tab_in_codigo = ser.org_tab_in_codigo
      AND cdi.org_pad_in_codigo = ser.org_pad_in_codigo
      AND cdi.org_in_codigo     = ser.org_in_codigo
      AND cdi.org_tau_st_codigo = ser.org_tau_st_codigo
      AND cdi.cnd_in_codigo     = ser.cnd_in_codigo
      AND cdi.cndit_in_codigo   = ser.tre_in_novaserie

      AND pro.pro_tab_in_codigo = cto.pro_tab_in_codigo
      AND pro.pro_pad_in_codigo = cto.pro_pad_in_codigo
      AND pro.pro_ide_st_codigo = cto.pro_ide_st_codigo
      AND pro.pro_in_reduzido   = cto.pro_in_reduzido

      AND pla.org_tab_in_codigo (+)= cto.org_tab_in_codigo
      AND pla.org_pad_in_codigo (+)= cto.org_pad_in_codigo
      AND pla.org_in_codigo     (+)= cto.org_in_codigo
      AND pla.org_tau_st_codigo (+)= cto.org_tau_st_codigo
      AND pla.cto_in_codigo     (+)= cto.cto_in_codigo

    GROUP BY ren.cto_in_codigo
           , ren.tre_dt_renegociacao
           , ren.tre_bo_status
           , ren.tre_in_codigo
           , NVL(ren.tre_in_parrenegociadas,0)
           , NVL(ren.tre_re_vlrparren,0)
           , pro.pro_in_reduzido
           , pro.pro_st_extenso
           , pro.pro_st_descricao
           , agn.agn_st_nome
           , est.blo_in_codigo
           , est.blo_st_codigo
           , est.blo_st_nome
           , est.emp_st_codigo
           , est.emp_st_nome
           , est.und_st_codigo
           , nvl(est.emp_in_codigo,0)
           , cdi.cnd_in_codigo
           , cdi.cndit_in_codigo
           , cdi.cndit_in_parcela
           , cdi.cndit_st_observacao
           , (cdi.cndit_re_valorbase)
           , (cdi.cndit_re_valorfuturo)
           , mgrel.pck_rel_fnc.fnc_car_corrige_serietp_ren( ren.org_tab_in_codigo
                                                          , ren.org_pad_in_codigo
                                                          , ren.org_in_codigo
                                                          , ren.org_tau_st_codigo
                                                          , ren.cnd_in_codigo
                                                          , ren.cndit_in_codigo
                                                          , ren.tre_dt_renegociacao
                                                          , 'RP'
                                                          , nvl(ren.tre_bo_multa,'N')
                                                          , nvl(ren.tre_bo_juros,'N')
                                                          , nvl(ren.tre_re_taxa,-999))
           , 'TP'
           , NVL(cdi.cndit_bo_statustp,'N')
           , NVL(cdi.cndit_re_txtabprice,0)
           , ren.tre_re_saldoren
           , est.estrutura
           , CAST('' AS VARCHAR2 (220))
           , NVL(pla.reneg_cont_a, 0)
           , NVL(pla.reneg_cont_b, 0)
           , ant.vlr_corrigido_data

    UNION ALL

    -- Select de contratos que possuem parcelas renegociadas do tipo SAC
    SELECT ren.cto_in_codigo
         , TRUNC(ren.rsc_dt_cadastro)         re_dt_renegociacao
         , ren.rsc_ch_status                  re_bo_status
         , ren.rsc_in_codigo                  re_in_codigo
         , NVL(ren.rsc_in_parrenegociadas, 0) par_ren_tp
         , TO_NUMBER(NULL)                    vlr_ren_tp
         , pro.pro_in_reduzido
         , pro.pro_st_extenso
         , pro.pro_st_descricao
         , agn.agn_st_nome
         , est.blo_in_codigo                 blo_codigo
         , est.blo_st_codigo                 blo_st_codigo
         , est.blo_st_nome                   blo_nome
         , est.emp_st_codigo                 emp_st_codigo
         , est.emp_st_nome                   emp_nome
         , est.und_st_codigo                 und_st_codigo
         , nvl(est.emp_in_codigo,0)          emp_codigo
         , cdi.cndit_in_parcela
         , cdi.cndit_st_observacao
         , SUM(cdi.cndit_re_valorbase * cdi.cndit_in_parcela)                                 vl_base
         , SUM(cdi.cndit_re_valorfuturo)     vl_futuro
         , mgrel.pck_rel_fnc.fnc_car_corrige_serietp_ren( ren.org_tab_in_codigo
                                                        , ren.org_pad_in_codigo
                                                        , ren.org_in_codigo
                                                        , ren.org_tau_st_codigo
                                                        , ren.cnd_in_codigo
                                                        , ren.cndit_in_codigo
                                                        , ren.rsc_dt_cadastro
                                                        , 'RP'
                                                        , nvl(ren.rsc_bo_multa,'N')
                                                        , nvl(ren.rsc_bo_mora, 'N')
                                                        , nvl(ren.rsc_re_taxautilizada,-999)) sld_renegociar
         , 'SAC' tp_tabela
         , NVL(cdi.cndit_bo_statustp,'N')                                                     Status_Aplic_TP
         , NVL(cdi.cndit_re_txtabprice,0)                                                     Taxa_TP
         , ren.rsc_re_saldorenegociado                                                        sld_renegociar2
         , est.estrutura
         , CAST('' AS VARCHAR2 (220))                                                         ren_obs
         , NVL(pla.reneg_cont_a, 0)                                                           reneg_cont_a
         , NVL(pla.reneg_cont_b, 0)                                                           reneg_cont_b
         , TO_NUMBER(NULL)                                                                    vlr_corrigido_data

    FROM (SELECT cpl.org_tab_in_codigo
               , cpl.org_pad_in_codigo
               , cpl.org_in_codigo
               , cpl.org_tau_st_codigo
               , cpl.cto_in_codigo
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'A', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_a
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'B', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_b
          FROM mgcar.car_contrato_planilha cpl
          WHERE cpl.pla_ch_tipomovimento IN ('A', 'B')
            AND cpl.pla_dt_movimento >= v_dt_fim
          GROUP BY cpl.org_tab_in_codigo
                 , cpl.org_pad_in_codigo
                 , cpl.org_in_codigo
                 , cpl.org_tau_st_codigo
                 , cpl.cto_in_codigo)  pla
       , mgglo.glo_projetos            pro
       , mgdbm.dbm_condicao_item       cdi
       , mgcar.car_jurossac_renseries  ser
       , mgcar.car_jurossac_ren        ren
       , mgglo.glo_agentes             agn
       , mgglo.glo_agentes_id          aid
       , mgcar.car_contrato_cliente    cli
       , mgcar.car_contrato            cto
       , mgrel.vw_car_estrutura        est

    WHERE cto.org_tab_in_codigo = decode(tipo_relat, 'E', tab, cto.org_tab_in_codigo)
      AND cto.org_pad_in_codigo = decode(tipo_relat, 'E', pad, cto.org_pad_in_codigo)
      AND org_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.org_in_codigo) || '#%', org_cod)
      AND fil_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.fil_in_codigo) || '#%', fil_cod)
      AND cto.org_tau_st_codigo = decode(tipo_relat, 'E', tau, cto.org_tau_st_codigo)

      AND pro.pro_pad_in_codigo = decode(tipo_relat, 'P', v_pad, pro.pro_pad_in_codigo)
      AND pro.pro_st_extenso    = decode(tipo_relat, 'P', v_pro_ext, pro.pro_st_extenso)

      AND cto.cto_ch_status = DECODE(v_cto_ch_status, 'TUDO', cto.cto_ch_status, v_cto_ch_status)

      AND ( est.emp_in_codigo = decode(tipo_relat, 'E', decode(v_blo_codigo, 0, est.emp_in_codigo, v_blo_codigo), est.emp_in_codigo)
          OR est.emp_in_codigo IS NULL )

      AND NVL(mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                               , est.org_pad_in_codigo
                                                               , est.org_in_codigo
                                                               , est.org_tau_st_codigo
                                                               , nvl(est.emp_in_codigo,0)
                                                               , v_sinc
                                                               ), 0) = nvl(est.emp_in_codigo,0)

      AND est.estrutura IN ('U', 'G', bens)

      AND ren.rsc_dt_cadastro >= v_dt_ini
      AND ren.rsc_dt_cadastro <= v_dt_fim
      AND ren.rsc_ch_status    = DECODE(v_tre_bo_status, 'T', ren.rsc_ch_status, v_tre_bo_status)

      --  contratos com parcelas caucionadas
      AND ( ( v_parc_caucao = 'C' AND EXISTS( SELECT cau.ctc_in_codigo
                                              FROM mgcar.car_caucao_parcela cau
                                                 , mgcar.car_parcela        par
                                              WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                AND par.org_in_codigo     = cto.org_in_codigo
                                                AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                AND par.cto_in_codigo     = cto.cto_in_codigo

                                                AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                AND cau.org_in_codigo     = par.org_in_codigo
                                                AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                AND cau.cto_in_codigo     = par.cto_in_codigo
                                                AND cau.par_in_codigo     = par.par_in_codigo))
       -- contratos sem parcelas caucionada
        OR ( v_parc_caucao = 'N' AND NOT EXISTS( SELECT cau.ctc_in_codigo
                                                 FROM mgcar.car_caucao_parcela cau
                                                    , mgcar.car_parcela        par
                                                 WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                   AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                   AND par.org_in_codigo     = cto.org_in_codigo
                                                   AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                   AND par.cto_in_codigo     = cto.cto_in_codigo

                                                   AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                   AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                   AND cau.org_in_codigo     = par.org_in_codigo
                                                   AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                   AND cau.cto_in_codigo     = par.cto_in_codigo
                                                   AND cau.par_in_codigo     = par.par_in_codigo ))
        -- todos os contratos
        OR ( v_parc_caucao = 'T'))

      AND est.cto_org_tab_in_codigo = cto.org_tab_in_codigo
      AND est.cto_org_pad_in_codigo = cto.org_pad_in_codigo
      AND est.cto_org_in_codigo     = cto.org_in_codigo
      AND est.cto_org_tau_st_codigo = cto.org_tau_st_codigo
      AND est.cto_in_codigo         = cto.cto_in_codigo

      AND cli.org_tab_in_codigo = cto.org_tab_in_codigo
      AND cli.org_pad_in_codigo = cto.org_pad_in_codigo
      AND cli.org_in_codigo     = cto.org_in_codigo
      AND cli.org_tau_st_codigo = cto.org_tau_st_codigo
      AND cli.cto_in_codigo     = cto.cto_in_codigo

      AND aid.agn_tab_in_codigo = cli.agn_tab_in_codigo
      AND aid.agn_pad_in_codigo = cli.agn_pad_in_codigo
      AND aid.agn_in_codigo     = cli.agn_in_codigo
      AND aid.agn_tau_st_codigo = cli.agn_tau_st_codigo

      AND agn.agn_tab_in_codigo = aid.agn_tab_in_codigo
      AND agn.agn_pad_in_codigo = aid.agn_pad_in_codigo
      AND agn.agn_in_codigo     = aid.agn_in_codigo

      AND ren.org_tab_in_codigo = cto.org_tab_in_codigo
      AND ren.org_pad_in_codigo = cto.org_pad_in_codigo
      AND ren.org_in_codigo     = cto.org_in_codigo
      AND ren.org_tau_st_codigo = cto.org_tau_st_codigo
      AND ren.cto_in_codigo     = cto.cto_in_codigo

      AND ser.org_tab_in_codigo = ren.org_tab_in_codigo
      AND ser.org_pad_in_codigo = ren.org_pad_in_codigo
      AND ser.org_in_codigo     = ren.org_in_codigo
      AND ser.org_tau_st_codigo = ren.org_tau_st_codigo
      AND ser.cnd_in_codigo     = ren.cnd_in_codigo
      AND ser.cndit_in_codigo   = ren.cndit_in_codigo
      AND ser.cto_in_codigo     = ren.cto_in_codigo

      AND cdi.org_tab_in_codigo = ser.org_tab_in_codigo
      AND cdi.org_pad_in_codigo = ser.org_pad_in_codigo
      AND cdi.org_in_codigo     = ser.org_in_codigo
      AND cdi.org_tau_st_codigo = ser.org_tau_st_codigo
      AND cdi.cnd_in_codigo     = ser.cnd_in_codigo
      AND cdi.cndit_in_codigo   = ser.rsc_in_novaserie

      AND pro.pro_tab_in_codigo = cto.pro_tab_in_codigo
      AND pro.pro_pad_in_codigo = cto.pro_pad_in_codigo
      AND pro.pro_ide_st_codigo = cto.pro_ide_st_codigo
      AND pro.pro_in_reduzido   = cto.pro_in_reduzido

      AND pla.org_tab_in_codigo (+)= cto.org_tab_in_codigo
      AND pla.org_pad_in_codigo (+)= cto.org_pad_in_codigo
      AND pla.org_in_codigo     (+)= cto.org_in_codigo
      AND pla.org_tau_st_codigo (+)= cto.org_tau_st_codigo
      AND pla.cto_in_codigo     (+)= cto.cto_in_codigo

    GROUP BY ren.cto_in_codigo
         , ren.rsc_dt_cadastro
         , ren.rsc_ch_status
         , ren.rsc_in_codigo
         , NVL(ren.rsc_in_parrenegociadas, 0)
         , TO_NUMBER(NULL)
         , pro.pro_in_reduzido
         , pro.pro_st_extenso
         , pro.pro_st_descricao
         , agn.agn_st_nome
         , est.blo_in_codigo
         , est.blo_st_codigo
         , est.blo_st_nome
         , est.emp_st_codigo
         , est.emp_st_nome
         , est.und_st_codigo
         , nvl(est.emp_in_codigo,0)
         , cdi.cnd_in_codigo
         , cdi.cndit_in_codigo
         , cdi.cndit_in_parcela
         , cdi.cndit_st_observacao
         , (cdi.cndit_re_valorbase)
         , (cdi.cndit_re_valorfuturo)
         , mgrel.pck_rel_fnc.fnc_car_corrige_serietp_ren( ren.org_tab_in_codigo
                                                        , ren.org_pad_in_codigo
                                                        , ren.org_in_codigo
                                                        , ren.org_tau_st_codigo
                                                        , ren.cnd_in_codigo
                                                        , ren.cndit_in_codigo
                                                        , ren.rsc_dt_cadastro
                                                        , 'RP'
                                                        , nvl(ren.rsc_bo_multa,'N')
                                                        , nvl(ren.rsc_bo_mora, 'N')
                                                        , nvl(ren.rsc_re_taxautilizada,-999))
         , 'SAC'
         , NVL(cdi.cndit_bo_statustp,'N')
         , NVL(cdi.cndit_re_txtabprice,0)
         , ren.rsc_re_saldorenegociado
         , est.estrutura
         , CAST('' AS VARCHAR2 (220))
         , NVL(pla.reneg_cont_a, 0)
         , NVL(pla.reneg_cont_b, 0)

    UNION ALL
    -- Select de contratos que possuem parcelas renegociadas do tipo SACOC
    SELECT ren.cto_in_codigo
         , ren.ren_dt_geracao re_dt_renegociacao
         , ren.ren_ch_status  re_bo_status
         , ren.ren_in_codigo  re_in_codigo
         , TO_NUMBER(NULL)    par_ren_tp
         , TO_NUMBER(NULL)    vlr_ren_tp
         , pro.pro_in_reduzido
         , pro.pro_st_extenso
         , pro.pro_st_descricao
         , agn.agn_st_nome
         , est.blo_in_codigo blo_codigo
         , est.blo_st_codigo blo_st_codigo
         , est.blo_st_nome   blo_nome
         , est.emp_st_codigo emp_st_codigo
         , est.emp_st_nome   emp_nome
         , est.und_st_codigo und_st_codigo
         , nvl(est.emp_in_codigo,0) emp_codigo
         , cdi.cndit_in_parcela
         , cdi.cndit_st_observacao
         , (cdi.cndit_re_valorbase * cdi.cndit_in_parcela) vl_base
         , cdi.cndit_re_valorfuturo vl_futuro
         , SUM(rep.renpar_re_valorcorrigido)               sld_renegociar
         , 'NTP' tp_tabela
         , NVL(cdi.cndit_bo_statustp,'N')                  Status_Aplic_TP
         , NVL(cdi.cndit_re_txtabprice,0)                  Taxa_TP
         , SUM(rep.renpar_re_valorcorrigido)               sld_renegociar2
         , est.estrutura
         , CAST(ren.ren_st_observacao AS VARCHAR2 (220))   ren_obs
         , NVL(pla.reneg_cont_a, 0)                        reneg_cont_a
         , NVL(pla.reneg_cont_b, 0)                        reneg_cont_b
         , TO_NUMBER(NULL)                                 vlr_corrigido_data

    FROM (SELECT cpl.org_tab_in_codigo
               , cpl.org_pad_in_codigo
               , cpl.org_in_codigo
               , cpl.org_tau_st_codigo
               , cpl.cto_in_codigo
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'A', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_a
               , SUM ( DECODE ( cpl.pla_ch_tipomovimento, 'B', NVL(cpl.pla_re_varmonetaria, 0) + NVL(cpl.pla_re_varmonjuros, 0) ) ) reneg_cont_b
          FROM mgcar.car_contrato_planilha cpl
          WHERE cpl.pla_ch_tipomovimento IN ('A', 'B')
            AND cpl.pla_dt_movimento >= v_dt_fim
          GROUP BY cpl.org_tab_in_codigo
                 , cpl.org_pad_in_codigo
                 , cpl.org_in_codigo
                 , cpl.org_tau_st_codigo
                 , cpl.cto_in_codigo)   pla
       , mgglo.glo_projetos             pro
       , mgdbm.dbm_condicao_item        cdi
       , mgcar.car_renegociacao_parcela rep
       , mgcar.car_renegociacao         ren
       , mgglo.glo_agentes              agn
       , mgglo.glo_agentes_id           aid
       , mgcar.car_contrato_cliente     cli
       , mgcar.car_contrato             cto
       , mgrel.vw_car_estrutura         est

    WHERE cto.org_tab_in_codigo = decode(tipo_relat, 'E', tab, cto.org_tab_in_codigo)
      AND cto.org_pad_in_codigo = decode(tipo_relat, 'E', pad, cto.org_pad_in_codigo)
      AND org_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.org_in_codigo) || '#%', org_cod)
      AND fil_cod LIKE decode(tipo_relat, 'E','%#' || TO_CHAR(cto.fil_in_codigo) || '#%', fil_cod)
      AND cto.org_tau_st_codigo = decode(tipo_relat, 'E', tau, cto.org_tau_st_codigo)

      AND pro.pro_pad_in_codigo = decode(tipo_relat, 'P', v_pad, pro.pro_pad_in_codigo)
      AND pro.pro_st_extenso    = decode(tipo_relat, 'P', v_pro_ext, pro.pro_st_extenso)

      AND cto.cto_ch_status = DECODE(v_cto_ch_status, 'TUDO', cto.cto_ch_status, v_cto_ch_status)

      AND ( est.emp_in_codigo = decode(tipo_relat, 'E', decode(v_blo_codigo, 0, est.emp_in_codigo, v_blo_codigo), est.emp_in_codigo)
          OR est.emp_in_codigo IS NULL )

      AND NVL(mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                               , est.org_pad_in_codigo
                                                               , est.org_in_codigo
                                                               , est.org_tau_st_codigo
                                                               , nvl(est.emp_in_codigo,0)
                                                               , v_sinc
                                                               ), 0) = nvl(est.emp_in_codigo,0)

      AND est.estrutura IN ('U', 'G', bens)

      AND ren.ren_dt_geracao >= v_dt_ini
      AND ren.ren_dt_geracao <= v_dt_fim
      AND ren.ren_ch_status   = DECODE(v_tre_bo_status, 'T', ren.ren_ch_status, v_tre_bo_status)

      --  contratos com parcelas caucionadas
      AND ( ( v_parc_caucao = 'C' AND EXISTS( SELECT cau.ctc_in_codigo
                                              FROM mgcar.car_caucao_parcela cau
                                                 , mgcar.car_parcela        par
                                              WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                AND par.org_in_codigo     = cto.org_in_codigo
                                                AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                AND par.cto_in_codigo     = cto.cto_in_codigo

                                                AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                AND cau.org_in_codigo     = par.org_in_codigo
                                                AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                AND cau.cto_in_codigo     = par.cto_in_codigo
                                                AND cau.par_in_codigo     = par.par_in_codigo))
       -- contratos sem parcelas caucionada
        OR ( v_parc_caucao = 'N' AND NOT EXISTS( SELECT cau.ctc_in_codigo
                                                 FROM mgcar.car_caucao_parcela cau
                                                    , mgcar.car_parcela        par
                                                 WHERE par.org_tab_in_codigo = cto.org_tab_in_codigo
                                                   AND par.org_pad_in_codigo = cto.org_pad_in_codigo
                                                   AND par.org_in_codigo     = cto.org_in_codigo
                                                   AND par.org_tau_st_codigo = cto.org_tau_st_codigo
                                                   AND par.cto_in_codigo     = cto.cto_in_codigo

                                                   AND cau.org_tab_in_codigo = par.org_tab_in_codigo
                                                   AND cau.org_pad_in_codigo = par.org_pad_in_codigo
                                                   AND cau.org_in_codigo     = par.org_in_codigo
                                                   AND cau.org_tau_st_codigo = par.org_tau_st_codigo
                                                   AND cau.cto_in_codigo     = par.cto_in_codigo
                                                   AND cau.par_in_codigo     = par.par_in_codigo ))
        -- todos os contratos
        OR ( v_parc_caucao = 'T'))

      AND est.cto_org_tab_in_codigo = cto.org_tab_in_codigo
      AND est.cto_org_pad_in_codigo = cto.org_pad_in_codigo
      AND est.cto_org_in_codigo     = cto.org_in_codigo
      AND est.cto_org_tau_st_codigo = cto.org_tau_st_codigo
      AND est.cto_in_codigo         = cto.cto_in_codigo

      AND cli.org_tab_in_codigo = cto.org_tab_in_codigo
      AND cli.org_pad_in_codigo = cto.org_pad_in_codigo
      AND cli.org_in_codigo     = cto.org_in_codigo
      AND cli.org_tau_st_codigo = cto.org_tau_st_codigo
      AND cli.cto_in_codigo     = cto.cto_in_codigo

      AND aid.agn_tab_in_codigo = cli.agn_tab_in_codigo
      AND aid.agn_pad_in_codigo = cli.agn_pad_in_codigo
      AND aid.agn_in_codigo     = cli.agn_in_codigo
      AND aid.agn_tau_st_codigo = cli.agn_tau_st_codigo

      AND agn.agn_tab_in_codigo = aid.agn_tab_in_codigo
      AND agn.agn_pad_in_codigo = aid.agn_pad_in_codigo
      AND agn.agn_in_codigo     = aid.agn_in_codigo

      AND ren.org_tab_in_codigo = cto.org_tab_in_codigo
      AND ren.org_pad_in_codigo = cto.org_pad_in_codigo
      AND ren.org_in_codigo     = cto.org_in_codigo
      AND ren.org_tau_st_codigo = cto.org_tau_st_codigo
      AND ren.cto_in_codigo     = cto.cto_in_codigo

      AND rep.org_tab_in_codigo = ren.org_tab_in_codigo
      AND rep.org_pad_in_codigo = ren.org_pad_in_codigo
      AND rep.org_in_codigo     = ren.org_in_codigo
      AND rep.org_tau_st_codigo = ren.org_tau_st_codigo
      AND rep.cto_in_codigo     = ren.cto_in_codigo
      AND rep.ren_in_codigo     = ren.ren_in_codigo

      AND cdi.org_tab_in_codigo = ren.org_tab_in_codigo
      AND cdi.org_pad_in_codigo = ren.org_pad_in_codigo
      AND cdi.org_in_codigo     = ren.org_in_codigo
      AND cdi.org_tau_st_codigo = ren.org_tau_st_codigo
      AND cdi.cnd_in_codigo     = ren.cnd_in_codigo

      AND pro.pro_tab_in_codigo = cto.pro_tab_in_codigo
      AND pro.pro_pad_in_codigo = cto.pro_pad_in_codigo
      AND pro.pro_ide_st_codigo = cto.pro_ide_st_codigo
      AND pro.pro_in_reduzido   = cto.pro_in_reduzido

      AND pla.org_tab_in_codigo (+)= cto.org_tab_in_codigo
      AND pla.org_pad_in_codigo (+)= cto.org_pad_in_codigo
      AND pla.org_in_codigo     (+)= cto.org_in_codigo
      AND pla.org_tau_st_codigo (+)= cto.org_tau_st_codigo
      AND pla.cto_in_codigo     (+)= cto.cto_in_codigo

    GROUP BY ren.cto_in_codigo
           , ren.ren_dt_geracao
           , ren.ren_ch_status
           , ren.ren_in_codigo
           , TO_NUMBER(NULL)
           , TO_NUMBER(NULL)
           , pro.pro_in_reduzido
           , pro.pro_st_extenso
           , pro.pro_st_descricao
           , agn.agn_st_nome
           , est.blo_in_codigo
           , est.blo_st_codigo
           , est.blo_st_nome
           , est.emp_st_codigo
           , est.emp_st_nome
           , est.und_st_codigo
           , nvl(est.emp_in_codigo,0)
           , cdi.cnd_in_codigo
           , cdi.cndit_in_codigo
           , cdi.cndit_in_parcela
           , cdi.cndit_st_observacao
           , (cdi.cndit_re_valorbase)
           , cdi.cndit_re_valorfuturo
           , 'NTP'
           , NVL(cdi.cndit_bo_statustp,'N')
           , NVL(cdi.cndit_re_txtabprice,0)
           , est.estrutura
           , CAST(ren.ren_st_observacao AS VARCHAR2 (220))
           , NVL(pla.reneg_cont_a, 0)
           , NVL(pla.reneg_cont_b, 0)

      ORDER BY 14, 12, 1, 4;

END PRC_CAR_Rel_RenParcela;
