CREATE OR REPLACE PROCEDURE MGCUSTOM.PRC_CAR_SITUACAO_UNIDADES(
  retorno           IN OUT mgrel.pck_resultado.result,
  org_tab_in_codigo NUMBER,
  org_pad_in_codigo NUMBER,
  org_in_codigo     NUMBER,
  org_tau_st_codigo VARCHAR2,
  v_bloco           NUMBER,
  v_calc_parc       VARCHAR2,
  v_emp             NUMBER,
  v_st_disp         CHAR,
  v_st_ind          CHAR,
  v_st_res          CHAR,
  v_st_vend         CHAR,
  v_st_per          CHAR,
  v_st_alu          CHAR,
  v_ind_usu         CHAR,
  fil_in_codigo     NUMBER,
  usu_in_codigo     NUMBER,
  comp_st_nome      VARCHAR2,
  p_dt_base         VARCHAR2,
  v_garagem         VARCHAR2
) AS
------------------------------------------------------------------------------------------------------------------------------------------------
-- Criação:
-- Autor:     José
-- Relatório: API Exporta Relatório
------------------------------------------------------------------------------------------------------------------------------------------------
-- Parâmetros:
-- v_calc_parc : parâmetro que vem com valor fixo do relatório(R_GLO_Unidades.rpt)
-- v_bloco     : código do bloco. Se passado "0", serão considerados todos os blocos do empreendimento;
-- v_emp       : código do empreendimento;
-- v_st_disp   : filtro pos status da unidade. D = Disponível
-- v_st_ind    : filtro pos status da unidade. I = Indisponível
-- v_st_res    : filtro pos status da unidade. R = Reservada
-- v_st_vend   : filtro pos status da unidade. V = Vendida
-- v_st_per    : filtro pos status da unidade. P = Permutada
-- v_st_alu    : filtro pos status da unidade. A = Alugada
-- v_ind_usu   : Considerar prospect de indisponibilizações. S para marcado e N desmarcado.
-- v_dt_base   : Faz o filtro pela data base digitada.
------------------------------------------------------------------------------------------------------------------------------------------------

v_tab        NUMBER(3);
v_pad        NUMBER(3);
v_tau        VARCHAR2(3);
v_disp       CHAR(1);
v_ind        CHAR(1);
v_res        CHAR(1);
v_vend       CHAR(1);
v_per        CHAR(1);
v_alu        CHAR(1);
v_vemp       NUMBER(22);
v_vbloco     NUMBER(22);
v_vcalc_parc VARCHAR2(10);
v_vind_usu   CHAR(1);
v_usu        NUMBER(22);
v_comp       VARCHAR2(255);
v_dt_base    DATE;

BEGIN

  v_tab        := org_tab_in_codigo;
  v_pad        := org_pad_in_codigo;
  v_tau        := org_tau_st_codigo;
  v_disp       := v_st_disp;
  v_ind        := v_st_ind;
  v_res        := v_st_res;
  v_vend       := v_st_vend;
  v_per        := v_st_per;
  v_alu        := v_st_alu;
  v_vemp       := v_emp;
  v_vbloco     := v_bloco;
  v_vcalc_parc := v_calc_parc;
  v_vind_usu   := v_ind_usu;
  v_comp       := comp_st_nome;
  v_usu        := usu_in_codigo;
  v_dt_base    := TO_DATE( p_dt_base, 'dd/mm/yyyy' );

  OPEN retorno FOR
    WITH TBL_UNIDADE_GARAGEM as (
      select
        u.org_tab_in_codigo,
        u.org_pad_in_codigo,
        u.org_in_codigo,
        u.org_tau_st_codigo,
        u.est_in_codigo,
        u.und_re_areatotal,
        u.und_re_peso,
        u.und_in_andar,
        u.und_st_matricula
      from mgdbm.dbm_unidade u
      union all
      select
        g.org_tab_in_codigo,
        g.org_pad_in_codigo,
        g.org_in_codigo,
        g.org_tau_st_codigo,
        g.est_in_codigo,
        g.gar_re_area  und_re_areatotal,
        g.gar_re_peso  und_re_peso,
        g.gar_in_andar und_in_andar,
        '' und_st_matricula
      from mgdbm.dbm_garagem g
      where v_garagem = 'S'
        AND g.und_est_in_codigo is null
    )

  -- Unidades/Garagens vendidas
  SELECT
    est.emp_in_codigo,
    est.emp_st_codigo,
    est.emp_st_nome,
    est.etp_in_codigo,
    est.etp_st_codigo,
    est.etp_st_nome,
    est.blo_in_codigo,
    est.blo_st_codigo,
    est.blo_st_nome,
    ent.ent_dt_entrega,
    est.und_in_codigo,
    est.und_st_codigo,
    est.und_st_nome,
    est.estrutura as tipo_estrutura,
    und.und_st_matricula,
    mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data(
      est.org_tab_in_codigo,
      est.org_pad_in_codigo,
      est.org_in_codigo,
      est.org_tau_st_codigo,
      est.und_in_codigo,
      v_dt_base,
      'N'
    ) und_ch_status,
    und.und_re_areatotal,
    und.und_re_peso,
    und.und_in_andar,
    -- Dados do contrato
    cto.cto_in_codigo,
    cto.cto_dt_cadastro,
    NVL(mgrel.pck_rel_fnc.fnc_car_valor_contrato(
      cto.org_tab_in_codigo,
      cto.org_pad_in_codigo,
      cto.org_in_codigo,
      cto.org_tau_st_codigo,
      cto.cto_in_codigo,
      'D',
      v_dt_base,
      '',
      ''), 0
    ) as cto_re_valorcontrato,
    agn.cliente               nome_cliente,
    agr.agn_st_nome           nome_corretor,
    agr.agn_ch_tipopessoafj   agn_ch_tipopessoafj,
    'N'                       tipo_reserva
  FROM mgdbm.dbm_entrega_obra         ent
     , mgglo.glo_agentes              agr
     , mgdbm.dbm_condicao_responsavel res
     , mgdbm.dbm_condicao             cnd
     , ( -- Cliente atual
         SELECT age.agn_st_nome        cliente
              , ctt.org_tab_in_codigo
              , ctt.org_pad_in_codigo
              , ctt.org_in_codigo
              , ctt.org_tau_st_codigo
              , ctt.cto_in_codigo
              , NULL
          FROM mgglo.glo_agentes            age
             , mgglo.glo_agentes_id         aid
             , mgcar.car_contrato_cliente   ctc
             , mgcar.car_contrato           ctt
        WHERE NVL((SELECT MAX(ctr_dt_processo)
                 FROM mgcar.car_cliente_transferido cli
                WHERE cli.org_tab_in_codigo = ctt.org_tab_in_codigo
                  AND cli.org_pad_in_codigo = ctt.org_pad_in_codigo
                  AND cli.org_in_codigo     = ctt.org_in_codigo
                  AND cli.org_tau_st_codigo = ctt.org_tau_st_codigo
                  AND cli.cto_in_codigo     = ctt.cto_in_codigo), ctt.cto_dt_cadastro) <= v_dt_base

          AND ctt.org_tab_in_codigo = ctc.org_tab_in_codigo
          AND ctt.org_pad_in_codigo = ctc.org_pad_in_codigo
          AND ctt.org_in_codigo     = ctc.org_in_codigo
          AND ctt.org_tau_st_codigo = ctc.org_tau_st_codigo
          AND ctt.cto_in_codigo     = ctc.cto_in_codigo

          AND ctc.agn_tab_in_codigo = aid.agn_tab_in_codigo
          AND ctc.agn_pad_in_codigo = aid.agn_pad_in_codigo
          AND ctc.agn_in_codigo     = aid.agn_in_codigo
          AND ctc.agn_tau_st_codigo = aid.agn_tau_st_codigo

          AND aid.agn_tab_in_codigo = age.agn_tab_in_codigo
          AND aid.agn_pad_in_codigo = age.agn_pad_in_codigo
          AND aid.agn_in_codigo     = age.agn_in_codigo
        UNION ALL
         -- Cliente antigo(Cessão de Direitos via botão)
         SELECT age.agn_st_nome       cliente
              , cct.org_tab_in_codigo
              , cct.org_pad_in_codigo
              , cct.org_in_codigo
              , cct.org_tau_st_codigo
              , cct.cto_in_codigo
              , MAX(ctr.ctr_in_codigo) DATA

          FROM mgglo.glo_agentes             age
             , mgglo.glo_agentes_id          aid
             , mgcar.car_cliente_transferido ctr
             , mgcar.car_contrato            cct

         WHERE (SELECT MIN(ctr_in_codigo)
                  FROM mgcar.car_cliente_transferido cit
                 WHERE cit.org_tab_in_codigo = cct.org_tab_in_codigo
                   AND cit.org_pad_in_codigo = cct.org_pad_in_codigo
                   AND cit.org_in_codigo     = cct.org_in_codigo
                   AND cit.org_tau_st_codigo = cct.org_tau_st_codigo
                   AND cit.cto_in_codigo     = cct.cto_in_codigo
                   AND cit.ctr_dt_processo   > v_dt_base) = ctr.ctr_in_codigo

           AND cct.org_tab_in_codigo = ctr.org_tab_in_codigo
           AND cct.org_pad_in_codigo = ctr.org_pad_in_codigo
           AND cct.org_in_codigo     = ctr.org_in_codigo
           AND cct.org_tau_st_codigo = ctr.org_tau_st_codigo
           AND cct.cto_in_codigo     = ctr.cto_in_codigo

           AND ctr.agn_tab_in_codigo = aid.agn_tab_in_codigo
           AND ctr.agn_pad_in_codigo = aid.agn_pad_in_codigo
           AND ctr.agn_in_codigo     = aid.agn_in_codigo
           AND ctr.agn_tau_st_codigo = aid.agn_tau_st_codigo

           AND aid.agn_tab_in_codigo = age.agn_tab_in_codigo
           AND aid.agn_pad_in_codigo = age.agn_pad_in_codigo
           AND aid.agn_in_codigo     = age.agn_in_codigo

      GROUP BY age.agn_st_nome
             , cct.org_tab_in_codigo
             , cct.org_pad_in_codigo
             , cct.org_in_codigo
             , cct.org_tau_st_codigo
             , cct.cto_in_codigo)     agn
     , mgdbm.dbm_condicao_envolvido   env
     , mgcar.car_contrato             cto
     , TBL_UNIDADE_GARAGEM            und
     , mgdbm.dbm_estrutura            ett
     , (SELECT fia.fil_tab_in_codigo  org_tab_in_codigo
               , fia.fil_pad_in_codigo  org_pad_in_codigo
               , DECODE( agn.agn_bo_consolidador, 'E', agn.agn_in_codigo, agn.pai_agn_in_codigo) org_in_codigo
               , agn.agn_in_codigo fil_in_codigo

          FROM mgglo.glo_filial_ativa fia
             , mgglo.glo_agentes      agn
          WHERE fia.usu_in_codigo     = v_usu
            AND fia.comp_st_nome      = v_comp

            AND agn.agn_tab_in_codigo = fia.fil_tab_in_codigo
            AND agn.agn_pad_in_codigo = fia.fil_pad_in_codigo
            AND agn.agn_in_codigo     = fia.fil_in_codigo
            AND agn.agn_bo_consolidador IN ('E', 'F')) ati
     , mgrel.vw_car_estrutura         est
  WHERE est.org_tab_in_codigo = v_tab
    AND est.org_pad_in_codigo = v_pad
    AND est.org_tau_st_codigo = v_tau

    AND est.blo_in_codigo     = DECODE( v_vbloco, 0, est.blo_in_codigo, v_vbloco)
    AND est.emp_in_codigo     = DECODE( v_vemp, 0, est.emp_in_codigo, v_emp)

    AND est.ctoenv_ch_origem  in ('U','G')
    AND ett.est_dt_cadastro <= v_dt_base
    AND cto.cto_dt_cadastro <= v_dt_base

    AND mgrel.pck_rel_fnc.fnc_car_status_cto_data( cto.org_tab_in_codigo
                                                 , cto.org_pad_in_codigo
                                                 , cto.org_in_codigo
                                                 , cto.org_tau_st_codigo
                                                 , cto.cto_in_codigo
                                                 , v_dt_base
                                                 , 'N') IN ('A', 'U', 'Q')

    AND v_vcalc_parc = 'S'

    AND mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                      , est.org_pad_in_codigo
                                                      , est.org_in_codigo
                                                      , est.org_tau_st_codigo
                                                      , est.und_in_codigo
                                                      , v_dt_base
                                                      , 'N') IN (v_disp, v_ind, v_res, v_vend, v_per, v_alu)

    AND est.org_tab_in_codigo = ati.org_tab_in_codigo
    AND est.org_pad_in_codigo = ati.org_pad_in_codigo
    AND est.org_in_codigo     = ati.org_in_codigo
    AND est.fil_in_codigo     = ati.fil_in_codigo

    AND und.org_tab_in_codigo = est.org_tab_in_codigo
    AND und.org_pad_in_codigo = est.org_pad_in_codigo
    AND und.org_in_codigo     = est.org_in_codigo
    AND und.org_tau_st_codigo = est.org_tau_st_codigo
    AND und.est_in_codigo     = est.und_in_codigo

    AND ett.org_tab_in_codigo = und.org_tab_in_codigo
    AND ett.org_pad_in_codigo = und.org_pad_in_codigo
    AND ett.org_in_codigo     = und.org_in_codigo
    AND ett.org_tau_st_codigo = und.org_tau_st_codigo
    AND ett.est_in_codigo     = und.est_in_codigo

    AND cto.org_tab_in_codigo = est.cto_org_tab_in_codigo
    AND cto.org_pad_in_codigo = est.cto_org_pad_in_codigo
    AND cto.org_in_codigo     = est.cto_org_in_codigo
    AND cto.org_tau_st_codigo = est.cto_org_tau_st_codigo
    AND cto.cto_in_codigo     = est.cto_in_codigo

    AND cto.org_tab_in_codigo = agn.org_tab_in_codigo
    AND cto.org_pad_in_codigo = agn.org_pad_in_codigo
    AND cto.org_in_codigo     = agn.org_in_codigo
    AND cto.org_tau_st_codigo = agn.org_tau_st_codigo
    AND cto.cto_in_codigo     = agn.cto_in_codigo

    AND env.est_org_tab_in_codigo = est.org_tab_in_codigo
    AND env.est_org_pad_in_codigo = est.org_pad_in_codigo
    AND env.est_org_in_codigo     = est.org_in_codigo
    AND env.est_org_tau_st_codigo = est.org_tau_st_codigo
    AND env.est_in_codigo         = est.und_in_codigo
    AND env.cnd_in_codigo         = ( SELECT MAX(eve.cnd_in_codigo)
                                      FROM mgdbm.dbm_condicao           cnd
                                         , mgdbm.dbm_condicao_envolvido eve
                                      WHERE cnd.cnd_ch_status = 'G'
                                        AND eve.est_org_tab_in_codigo = env.est_org_tab_in_codigo
                                        AND eve.est_org_pad_in_codigo = env.est_org_pad_in_codigo
                                        AND eve.est_org_in_codigo     = env.est_org_in_codigo
                                        AND eve.est_org_tau_st_codigo = env.est_org_tau_st_codigo
                                        AND eve.est_in_codigo         = env.est_in_codigo

                                        AND cnd.org_tab_in_codigo = eve.org_tab_in_codigo
                                        AND cnd.org_pad_in_codigo = eve.org_pad_in_codigo
                                        AND cnd.org_in_codigo     = eve.org_in_codigo
                                        AND cnd.org_tau_st_codigo = eve.org_tau_st_codigo
                                        AND cnd.cnd_in_codigo     = eve.cnd_in_codigo)

    AND cnd.org_tab_in_codigo = env.org_tab_in_codigo
    AND cnd.org_pad_in_codigo = env.org_pad_in_codigo
    AND cnd.org_in_codigo     = env.org_in_codigo
    AND cnd.org_tau_st_codigo = env.org_tau_st_codigo
    AND cnd.cnd_in_codigo     = env.cnd_in_codigo

    AND res.org_tab_in_codigo (+)= cnd.org_tab_in_codigo
    AND res.org_pad_in_codigo (+)= cnd.org_pad_in_codigo
    AND res.org_in_codigo     (+)= cnd.org_in_codigo
    AND res.org_tau_st_codigo (+)= cnd.org_tau_st_codigo
    AND res.cnd_in_codigo     (+)= cnd.cnd_in_codigo
    AND (res.cor_in_codigo       = ( SELECT MAX(ree.cor_in_codigo)
                                     FROM mgdbm.dbm_condicao_responsavel ree
                                     WHERE ree.org_tab_in_codigo = res.org_tab_in_codigo
                                       AND ree.org_pad_in_codigo = res.org_pad_in_codigo
                                       AND ree.org_in_codigo     = res.org_in_codigo
                                       AND ree.org_tau_st_codigo = res.org_tau_st_codigo
                                       AND ree.cnd_in_codigo     = res.cnd_in_codigo)
         OR res.cor_in_codigo IS NULL)

    AND agr.agn_tab_in_codigo (+)= res.agn_tab_in_codigo
    AND agr.agn_pad_in_codigo (+)= res.agn_pad_in_codigo
    AND agr.agn_in_codigo     (+)= res.agn_in_codigo

    AND ent.org_tab_in_codigo (+)= est.org_tab_in_codigo
    AND ent.org_pad_in_codigo (+)= est.org_pad_in_codigo
    AND ent.org_in_codigo     (+)= est.org_in_codigo
    AND ent.org_tau_st_codigo (+)= est.org_tau_st_codigo
    AND ent.est_in_codigo     (+)= est.blo_in_codigo
    AND (ent.ent_dt_cadastro     = (
      SELECT MAX(eto.ent_dt_cadastro)
      FROM mgdbm.dbm_entrega_obra eto
      WHERE eto.org_tab_in_codigo = ent.org_tab_in_codigo
        AND eto.org_pad_in_codigo = ent.org_pad_in_codigo
        AND eto.org_in_codigo     = ent.org_in_codigo
        AND eto.org_tau_st_codigo = ent.org_tau_st_codigo
        AND eto.est_in_codigo     = ent.est_in_codigo
      ) OR ent.ent_dt_cadastro is null)
  UNION ALL
  -- Unidades/Garagens em estoque
  SELECT DISTINCT est.emp_in_codigo
                , est.emp_st_codigo
                , est.emp_st_nome
                , est.etp_in_codigo
                , est.etp_st_codigo
                , est.etp_st_nome
                , est.blo_in_codigo
                , est.blo_st_codigo
                , est.blo_st_nome
                , ent.ent_dt_entrega
                , est.und_in_codigo
                , est.und_st_codigo
                , est.und_st_nome
                , est.estrutura as tipo_estrutura
                , und.und_st_matricula
                , mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                , est.org_pad_in_codigo
                                                                , est.org_in_codigo
                                                                , est.org_tau_st_codigo
                                                                , est.und_in_codigo
                                                                , v_dt_base
                                                                , 'N') und_ch_status
                , und.und_re_areatotal
                , und.und_re_peso
                , und.und_in_andar
                -- Dados do contrato
                , TO_NUMBER(NULL) cto_in_codigo
                , TO_DATE(NULL)   cto_dt_cadastro
                , 0               cto_re_valorcontrato
                -- Dados do cliente
                , DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                        , est.org_pad_in_codigo
                                                                        , est.org_in_codigo
                                                                        , est.org_tau_st_codigo
                                                                        , est.und_in_codigo
                                                                        , v_dt_base
                                                                        , 'N'), 'R', DECODE( agc.agn_st_nome, NULL, pro.pro_st_nome, agc.agn_st_nome), DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                                                                                                                                                             , est.org_pad_in_codigo
                                                                                                                                                                                                             , est.org_in_codigo
                                                                                                                                                                                                             , est.org_tau_st_codigo
                                                                                                                                                                                                             , est.und_in_codigo
                                                                                                                                                                                                             , v_dt_base
                                                                                                                                                                                                             , 'N'), 'I', DECODE(agc.agn_st_nome, NULL, pro.pro_st_nome, agc.agn_st_nome), TO_CHAR(NULL))) nome_cliente
                -- Dados imobiliária/corretor
                , DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                        , est.org_pad_in_codigo
                                                                        , est.org_in_codigo
                                                                        , est.org_tau_st_codigo
                                                                        , est.und_in_codigo
                                                                        , v_dt_base
                                                                        , 'N'), 'R', DECODE( agc.agn_st_nome, NULL, TO_CHAR(NULL), agr.agn_st_nome), DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                                                                                                                                                           , est.org_pad_in_codigo
                                                                                                                                                                                                           , est.org_in_codigo
                                                                                                                                                                                                           , est.org_tau_st_codigo
                                                                                                                                                                                                           , est.und_in_codigo
                                                                                                                                                                                                           , v_dt_base
                                                                                                                                                                                                           , 'N'), 'I', DECODE(agc.agn_st_nome, NULL, TO_CHAR(NULL), agr.agn_st_nome), TO_CHAR(NULL))) nome_corretor
                , DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                        , est.org_pad_in_codigo
                                                                        , est.org_in_codigo
                                                                        , est.org_tau_st_codigo
                                                                        , est.und_in_codigo
                                                                        , v_dt_base
                                                                        , 'N'), 'R', DECODE( agc.agn_st_nome, NULL, TO_CHAR(NULL), agr.agn_ch_tipopessoafj), DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                                                                                                                                                                   , est.org_pad_in_codigo
                                                                                                                                                                                                                   , est.org_in_codigo
                                                                                                                                                                                                                   , est.org_tau_st_codigo
                                                                                                                                                                                                                   , est.und_in_codigo
                                                                                                                                                                                                                   , v_dt_base
                                                                                                                                                                                                                   , 'N'), 'I', DECODE(agc.agn_st_nome, NULL, TO_CHAR(NULL), agr.agn_ch_tipopessoafj), TO_CHAR(NULL))) agn_ch_tipopessoafj
                , DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                        , est.org_pad_in_codigo
                                                                        , est.org_in_codigo
                                                                        , est.org_tau_st_codigo
                                                                        , est.und_in_codigo
                                                                        , v_dt_base
                                                                        , 'N'), 'R', DECODE( agc.agn_st_nome, NULL, 'O', 'P'), DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                                                                                                                                                     , est.org_pad_in_codigo
                                                                                                                                                                                     , est.org_in_codigo
                                                                                                                                                                                     , est.org_tau_st_codigo
                                                                                                                                                                                     , est.und_in_codigo
                                                                                                                                                                                     , v_dt_base
                                                                                                                                                                                     , 'N'), 'I', DECODE( agc.agn_st_nome, NULL, 'O', 'P'), 'N')) tipo_reserva

  FROM mgdbm.dbm_entrega_obra       ent
     , mgcmr.cmr_prospect           pro
     , mgdbm.dbm_ocorrenciaprospect ocp
     -- Select para trazer a ultima ocorrencia de reserva/indisponibilidade não gerada pelo sistema
     , ( SELECT get.org_tab_in_codigo
              , get.org_pad_in_codigo
              , get.org_in_codigo
              , get.org_tau_st_codigo
              , get.und_in_codigo
              , MAX(ger.oco_in_codigo) oco_in_codigo
              , MAX(ger.oco_dt_cadastro) oco_dt_cadastro
         FROM mgrel.vw_glo_estrutura        get
            , mgdbm.dbm_ocorrenciaestrutura oce
            , mgdbm.dbm_geraocorrencia      ger

         WHERE v_vind_usu        = 'S'
           AND ger.ocs_in_modulo = 3
           AND ger.ocs_in_codigo = DECODE( mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( get.org_tab_in_codigo
                                                                                         , get.org_pad_in_codigo
                                                                                         , get.org_in_codigo
                                                                                         , get.org_tau_st_codigo
                                                                                         , get.und_in_codigo
                                                                                         , v_dt_base
                                                                                         , 'N'), v_res, 1, 3)

           AND oce.org_tab_in_codigo = get.org_tab_in_codigo
           AND oce.org_pad_in_codigo = get.org_pad_in_codigo
           AND oce.org_in_codigo     = get.org_in_codigo
           AND oce.org_tau_st_codigo = get.org_tau_st_codigo
           AND oce.est_in_codigo     = get.und_in_codigo

           AND ger.oco_in_codigo     = oce.oco_in_codigo
         GROUP BY get.org_tab_in_codigo
                , get.org_pad_in_codigo
                , get.org_in_codigo
                , get.org_tau_st_codigo
                , get.und_in_codigo ) oce
     , mgglo.glo_agentes              agr
     , mgdbm.dbm_condicao_responsavel res
     , mgglo.glo_agentes              agc
     , mgdbm.dbm_condicao_cliente     cli
     , mgdbm.dbm_condicao             cnd
     , mgdbm.dbm_condicao_envolvido   env
     , TBL_UNIDADE_GARAGEM            und
     , mgdbm.dbm_estrutura            ett
     , (SELECT fia.fil_tab_in_codigo  org_tab_in_codigo
               , fia.fil_pad_in_codigo  org_pad_in_codigo
               , DECODE( agn.agn_bo_consolidador, 'E', agn.agn_in_codigo, agn.pai_agn_in_codigo) org_in_codigo
               , agn.agn_in_codigo fil_in_codigo

          FROM mgglo.glo_filial_ativa fia
             , mgglo.glo_agentes      agn
          WHERE fia.usu_in_codigo     = v_usu
            AND fia.comp_st_nome      = v_comp

            AND agn.agn_tab_in_codigo = fia.fil_tab_in_codigo
            AND agn.agn_pad_in_codigo = fia.fil_pad_in_codigo
            AND agn.agn_in_codigo     = fia.fil_in_codigo
            AND agn.agn_bo_consolidador IN ('E', 'F')) ati
     , mgrel.vw_glo_estrutura         est

  WHERE est.org_tab_in_codigo = v_tab
    AND est.org_pad_in_codigo = v_pad
    AND est.org_tau_st_codigo = v_tau

    AND est.blo_in_codigo     = DECODE( v_vbloco, 0, est.blo_in_codigo, v_vbloco)
    AND est.emp_in_codigo     = DECODE( v_vemp, 0, est.emp_in_codigo, v_vemp)
    AND est.estrutura         in ('U','G')
    AND mgrel.pck_rel_glo_fnc.fnc_glo_status_und_data ( est.org_tab_in_codigo
                                                      , est.org_pad_in_codigo
                                                      , est.org_in_codigo
                                                      , est.org_tau_st_codigo
                                                      , est.und_in_codigo
                                                      , v_dt_base
                                                      , 'N') IN (v_disp, v_ind, v_res, v_vend, v_per, v_alu)

    AND ett.est_dt_cadastro <= v_dt_base

    AND NOT EXISTS(SELECT 1
                   FROM mgcar.car_contrato_envolvido cte
                      , mgcar.car_contrato     cto
                   WHERE cte.org_tab_in_codigo = cto.org_tab_in_codigo
                     AND cte.org_pad_in_codigo = cto.org_pad_in_codigo
                     AND cte.org_in_codigo     = cto.org_in_codigo
                     AND cte.org_tau_st_codigo = cto.org_tau_st_codigo
                     AND cte.cto_in_codigo     = cto.cto_in_codigo

                     AND est.org_tab_in_codigo = cte.est_org_tab_in_codigo
                     AND est.org_pad_in_codigo = cte.est_org_pad_in_codigo
                     AND est.org_in_codigo     = cte.est_org_in_codigo
                     AND est.org_tau_st_codigo = cte.est_org_tau_st_codigo
                     AND est.und_in_codigo     = cte.est_in_codigo
                     AND cte.cto_in_codigo     = cto.cto_in_codigo

                     AND cto.cto_dt_cadastro <= v_dt_base
                     AND cto.cto_ch_status IN ('A','Q','U'))

    AND est.org_tab_in_codigo = ati.org_tab_in_codigo
    AND est.org_pad_in_codigo = ati.org_pad_in_codigo
    AND est.org_in_codigo     = ati.org_in_codigo
    AND est.fil_in_codigo     = ati.fil_in_codigo

    AND und.org_tab_in_codigo = est.org_tab_in_codigo
    AND und.org_pad_in_codigo = est.org_pad_in_codigo
    AND und.org_in_codigo     = est.org_in_codigo
    AND und.org_tau_st_codigo = est.org_tau_st_codigo
    AND und.est_in_codigo     = est.und_in_codigo

    AND ett.org_tab_in_codigo = und.org_tab_in_codigo
    AND ett.org_pad_in_codigo = und.org_pad_in_codigo
    AND ett.org_in_codigo     = und.org_in_codigo
    AND ett.org_tau_st_codigo = und.org_tau_st_codigo
    AND ett.est_in_codigo     = und.est_in_codigo

    AND env.est_org_tab_in_codigo (+)= und.org_tab_in_codigo
    AND env.est_org_pad_in_codigo (+)= und.org_pad_in_codigo
    AND env.est_org_in_codigo     (+)= und.org_in_codigo
    AND env.est_org_tau_st_codigo (+)= und.org_tau_st_codigo
    AND env.est_in_codigo         (+)= und.est_in_codigo

    AND cnd.org_tab_in_codigo (+)= env.org_tab_in_codigo
    AND cnd.org_pad_in_codigo (+)= env.org_pad_in_codigo
    AND cnd.org_in_codigo     (+)= env.org_in_codigo
    AND cnd.org_tau_st_codigo (+)= env.org_tau_st_codigo
    AND cnd.cnd_in_codigo     (+)= env.cnd_in_codigo
    AND cnd.cnd_ch_status     (+)= 'A'

    AND cli.org_tab_in_codigo (+)= cnd.org_tab_in_codigo
    AND cli.org_pad_in_codigo (+)= cnd.org_pad_in_codigo
    AND cli.org_in_codigo     (+)= cnd.org_in_codigo
    AND cli.org_tau_st_codigo (+)= cnd.org_tau_st_codigo
    AND cli.cnd_in_codigo     (+)= cnd.cnd_in_codigo

    AND agc.agn_tab_in_codigo (+)= cli.agn_tab_in_codigo
    AND agc.agn_pad_in_codigo (+)= cli.agn_pad_in_codigo
    AND agc.agn_in_codigo     (+)= cli.agn_in_codigo

    AND res.org_tab_in_codigo (+)= cnd.org_tab_in_codigo
    AND res.org_pad_in_codigo (+)= cnd.org_pad_in_codigo
    AND res.org_in_codigo     (+)= cnd.org_in_codigo
    AND res.org_tau_st_codigo (+)= cnd.org_tau_st_codigo
    AND res.cnd_in_codigo     (+)= cnd.cnd_in_codigo
    AND NVL(res.cor_in_codigo, 0)=  NVL(( SELECT MAX(ree.cor_in_codigo)
                                          FROM mgdbm.dbm_condicao_responsavel ree
                                          WHERE ree.org_tab_in_codigo = res.org_tab_in_codigo
                                            AND ree.org_pad_in_codigo = res.org_pad_in_codigo
                                            AND ree.org_in_codigo     = res.org_in_codigo
                                            AND ree.org_tau_st_codigo = res.org_tau_st_codigo
                                            AND ree.cnd_in_codigo     = res.cnd_in_codigo), 0)

    AND agr.agn_tab_in_codigo (+)= res.agn_tab_in_codigo
    AND agr.agn_pad_in_codigo (+)= res.agn_pad_in_codigo
    AND agr.agn_in_codigo     (+)= res.agn_in_codigo

    AND oce.org_tab_in_codigo (+)= est.org_tab_in_codigo
    AND oce.org_pad_in_codigo (+)= est.org_pad_in_codigo
    AND oce.org_in_codigo     (+)= est.org_in_codigo
    AND oce.org_tau_st_codigo (+)= est.org_tau_st_codigo
    AND oce.und_in_codigo     (+)= est.und_in_codigo

    AND ocp.oco_in_codigo (+)= oce.oco_in_codigo

    AND pro.pro_in_codigo (+)= ocp.pro_in_codigo

    AND ent.org_tab_in_codigo (+)= est.org_tab_in_codigo
    AND ent.org_pad_in_codigo (+)= est.org_pad_in_codigo
    AND ent.org_in_codigo     (+)= est.org_in_codigo
    AND ent.org_tau_st_codigo (+)= est.org_tau_st_codigo
    AND ent.est_in_codigo     (+)= est.blo_in_codigo
    AND (ent.ent_dt_cadastro     = (SELECT MAX(eto.ent_dt_cadastro)
                                    FROM mgdbm.dbm_entrega_obra eto
                                    WHERE eto.org_tab_in_codigo = ent.org_tab_in_codigo
                                      AND eto.org_pad_in_codigo = ent.org_pad_in_codigo
                                      AND eto.org_in_codigo     = ent.org_in_codigo
                                      AND eto.org_tau_st_codigo = ent.org_tau_st_codigo
                                      AND eto.est_in_codigo     = ent.est_in_codigo)
         OR ent.ent_dt_cadastro is null)
  ORDER BY
    tipo_estrutura desc,
    und_st_codigo
  ;


END PRC_CAR_SITUACAO_UNIDADES;
