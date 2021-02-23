CREATE OR REPLACE PROCEDURE MGCUSTOM.PRC_CAR_RELACAODISTRATO ( retorno IN OUT mgrel.pck_resultado.result
                                                         , org_tab_in_codigo NUMBER
                                                         , org_pad_in_codigo NUMBER
                                                         , org_in_codigo     NUMBER
                                                         , org_tau_st_codigo VARCHAR2
                                                         , v_cod_proj        VARCHAR2
                                                         , v_cod_emp         NUMBER
                                                         , v_emitir_por      VARCHAR2
                                                         , v_data_ini        VARCHAR2
                                                         , v_data_fim        VARCHAR2
                                                         , unidade           VARCHAR2
                                                         , garagem           VARCHAR2
                                                         , bens              VARCHAR2
                                                         , tipo_termo        VARCHAR2
                                                         , tipo_classif      NUMBER
                                                         , fil_in_codigo     NUMBER
                                                         , usu_in_codigo     NUMBER
                                                         , comp_st_nome      VARCHAR2
                                                         , v_sincroniza      CHAR
                                                         , v_representante   CHAR
                                                         , p_tpu_in_codigo   NUMBER
                                                         , p_cons_perc_part  CHAR
                                                         , p_parc_caucao     CHAR
                                                         , p_conf_divida     CHAR
                                                         , p_classif_dis     NUMBER
                                                         , p_distrato_estudo CHAR) AS

-------------------------------------------------------------------------------------------------------------
-- Inclusão   : 17/03/2003
-- Responsável: Edson
-- Formatos   : R_CAR_Relacao_Distrato.rpt
-- Alterado   : 19/03/2003
-- Responsável: Edson
-------------------------------------------------------------------------------------------------------------
-- Fatos Relevantes:
-- V.08 - Revisor: 12/12/2012 - Guilherme Chiconato
-- V.08 - Auditor: 15/05/2013 - Guilherme Chiconato
-- V.09 - Revisor: 12/11/2013 - Anacris Kosinski
-- V.12 - Revisor: 30/08/2016 - André Luiz Carraro
-----------------------------------------------------------------------------------------------------------------------------------
-- Parâmetros:
-- v_cod_proj   = Codigo do projeto
-- v_cod_emp    = Codigo do empreendimento
-- v_emitir_por = 'E' - Seleciona contratos do empreendimento
--              = 'P' - Seleciona contratos por projeto
-- data_ini   = data inicial da pesquisa dos distratos
-- data_fim   = data final da pesquisa dos distratos
-- unidade      = Se deseja listar as unidades
-- garagem      = Se deseja listar as garagens
-- bens         = Se deseja listar os bens de terceiros
-- tipo_termo   = Tipo de termos 'P' - Termos contratuais
--                               'I' - No contratuais
--                               'T' - Todos
-- p_tipo_classif = cdigo da classificao do contrato
-- p_sincroniza: Considera Empreendimentos Sincronizados: 'N' - Não
--                                                        'S' - Sim
--                                                        'T' - Todos
-- p_tipologia    = Traz a Tipologia da Unidade
--                  0(zero) para todos
-- p_agrupa_tipol = Agrupa por Tipologia das Unidades ou não
--                  'N' Não
--                  'S' Sim
-- p_parc_caucao  = Tipo Parcela:
--                  Todas           "T"
--                  Caucionadas     "C"
--                  Não Caucionadas "N"
-- p_conf_divifa = Opção de considerar parcelas de confissão de dívida (S ou N)
-- p_classif_dis = Classiaficação do distrato - 0 para listar todas
-- p_distrato_estudo = Opção para considerar distratos em estudo (S ou N)
-------------------------------------------------------------------------------------------------------------
-- Alterado:
-- 19/03/2003 - Edson: Colocado campo valor contrato conforme chamado 758
-- 10/04/2003 - Cleber: Alterado o campo cto_nr_parcelas para trazer as parcelas do distrato
--                      da tabela condio item - campo cndit_in_parcela.
-- 21/10/2003 - Alexandre: Adequado o cabealho da procedure ao padro MEGA
--                         Inserido opo de listar todos os empreendimentos, opo "0" - chamado 2550 - Atlntica
-- 06/12/2003 - Cleber: Alterado para trazer a rea privativa da unidade e o valor de venda do contrato,
--                      que considera o valor do contrato mais o valor de termo.
-- 19/04/2004 - Joo: Includo o parmetro "tipo_classif" para filtrar pelo tipo de classificao do contrato
--                    Aletrado o select para trazer o cdigo e a descrio da classificao do contrato
-- 19/05/2004 - Janaina: Incluso o parmetro fil_in_codigo.
-- 23/08/2004 - Janaina: Corrigido o filtro do parmetro por empreendimento. ch. 5070.
-- 14/09/2004 - ALEXANDRE RIEPER:
--              > alterado o select para os tens aparecerem apenas na filial na qual o contrato foi cadastrado
--                ou seja, linkar os parmetros aos orgs do contrato e no da estrutura. - chamado 5318
-- 21/09/2004 - Janaina: Incluso o campo cto_ch_tipo no select. ch 5379
-- 30/09/2004 - Cleiton: Incluso os campos ren_ch_tipomulta e ren_re_fruicao  no select. ch 5463
-- 15/02/2007 - Eduardo Correa:
--              Inserido o parmetro ren_ch_encargo na funo mgrel.pck_rel_fnc.fnc_car_saldopago_contrato ch. 14614
-- 29/06/2007 - Eduardo Correa: Realizado tratamento para emitir por consolidadora (conceito de filial ativa) Ch 15798
-- 19/12/2007 - Paulo Chaves: Inserida a tabela mgdbm.dbm_condicao com alias prc para buscar valor total da condicao. Ch 18949.
-- 21/02/2008 - Eduardo Correa: Insiro o retorno dos campos ren_st_formadevolucao e ren_bo_observacao. Inseridoo parametro
--              v_observacao para tratar supresso no relatrio. Ch 17216
-- 07/04/2008 - Cleiton: alterado view mgrel.vw_rel_estrutura_contrato para mgrel.vw_rel_estrutura, retirado tabela de unidades
--              retirado outter join da tabela de classificacao e outter join da tabela de projetos, retirado tabelas mgdbm.dbm_condicao e mgdbm.dbm_unidade
--              retirado if/else que separava v_emitir_por Empreendimento/Projeto. ch. 20778
-- 07/05/2008 - Jefferson: Inserida a funo para tratamento de empreendimentos sincronizados. ch. 21145.
-- 19/05/2008 - Paulo Chaves: Inserido relacionamento de contrato com tabela de agentes para retornar a filial do contrato. Ch 20459
-- 07/10/2008 - Eduardo N Santos: Alterado o Vinculo da tabela contrato envolvido com o view estrtura, colocado mais um join e DECODE para filtrar corretamente. Ch.24712
-- 16/03/2009 - Eduardo Santos: Inserido campo de observao da tela de distrato que representa o codigo do agente que deve ser retornado caso parametrizado ch 26244.
-- 23/04/2009 - Eduardo Santos: Inserido tratamento de DECODE para a variavel considerar representantes de distrato ch 29542
-- 06/08/2009 - Cssia: Alterado o campo prc.cnd_re_valor para cdi.cndit_re_valorbase  no  prc_re_valor e abreviado o Cesso de Direitos para C. Direitos. Ch.31772
-- 25/09/2009 - Eduardo Santos: Inserido campo de retorno ren_bo_outrafilial para ser utilizado no formato padro. Ch. 33116
-- 19/11/2009 - Adilson Mir: Acrescentado o parmetro p_tpu_in_codigo, p_agrupa_tipol, a varivel v_tipologia e acrescentado as tabelas mgdbm.dbm_tipologia_unidade; mgdbm.dbm_unidade ch.31161
-- 11/01/2010 - Eliani: Alterado o relacionamento entre as tabelas "mgrel.vw_glo_estrutura", "mgdbm.dbm_unidade" e "mgdbm.dbm_tipologia_unidade" para outer join, para que seja apresentadas as informaes
--              de garagens e bens de terceiros. Ch. 35237
-- 11/08/2010 - Marino: Substituído o campo "cto_re_valorcontrato" pela função "mgrel.pck_rel_fnc.fnc_car_valor_contrato".
--              Alterado o filtro de projeto para considerar o código extenso. (Fatos Relevantes V04 Item 6 Todos os Módulos).
--              Alterada a view "mgrel.vw_glo_estrutura" para a view "mgrel.vw_car_estrutura". (Fatos Relevantes V04 Item 6 Mód. Carteira). Ch. 35671.
-- 08/10/2010 - Marino: Inserido o parâmetro "p_cons_perc_part" para considerar o Percentual de Investimento. Ch. 39503.
-- 25/04/2011 - Eliani: Incluso o parâmetro "p_parc_caucao",bem como o tratamento para considerar contratos com parcelas caucionadas, não caucionadas
--              ou todos. Ch. 43413
-- 26/05/2011 - Camilla Camatta: Adicionado retorno agn_ds_cpfcnpj no select principal. Ch.45214
-- 02/08/2011 - Jaqueline Silva - Alterado o "DECODE(cto.cto_ch_status..." para que mostre o status(DISTRATADO) do contrato e não o motivo do Distrato(DEVOLUÇÃO).
--              Alterado também para passar a mostrar a classificação do distrato, extraído da tabela "mgcar.car_classificacao_distrato". Ch. 47492.
-- 12/07/2012 - Eliani: Inclusos os filtros "p_conf_divida" e "p_classif_dis", e o union para trazer somente parcelas de confissão de divida. Ch 334070
-- 14/12/2012 - Guilherme Chiconato: Adicionado o parâmetro p_distrato_estudo para filtrar contratos com distrato em estudo
--                                   Alterado o conceito do filtro de confissão de dívida.
--                                   Separado o select em cursor, trocada a data de status do contrato pela data de geração do distrato Ch. 361835
-- 15/05/2013 - Guilherme Chiconato: passado fatos relevantes e pdt, retirado formato de cliente de SP do comentário. Ch. 382708
-- 12/11/2013 - Anacris Kosinski: Retirado o select de filial ativa do cursor e incluído como sub select no cursor de contratos. Ch: 63724.
-- 23/09/2016 - André Luiz Carraro: Retirado parâmetro [p_agrupa_tipol] da procedure e mantido somente no RPT.
--                                  Adicionado campo [ren_bo_corrigevlrpago].
--                                  Adicionado parâmetro para considerar ou não carta de crédito na chamada da função [fnc_car_saldopago_contrato]. PINC-3795
-- 29/09/2016 - André Luiz Carraro: Corrigido chamada da funçao [fnc_car_saldopago_contrato]. PINC-3909
-- 27/01/2017 - Alexandre W. Rieper: Desconsidero no campo "valor_contrato", os termos, pois o mesmo é somado por fora. PINC-4261
-- 16/02/2017 - Alexandre W. Rieper: Considerado no VALOR_PAGO, o juros do contrato atualizado, caso parametrizado. PINC-4320
-- 11/09/2018 - André Luiz Carraro: Retirado PINC-4320. POPF-345
-- 23/10/2019 - André Luiz Carraro: Retirado percentual de participante da taxa de distrato, para calcular a porcentagem de multa direto no RPT. POPF-886 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------

v_tab                        NUMBER(3);
v_pad                        NUMBER(3);
v_cod                        NUMBER(7);
v_tau                        VARCHAR2(3);
v_fil                        NUMBER(7);
v_usu                        NUMBER(22);
v_comp                       VARCHAR2(255);
v_pro_ext                    VARCHAR2(25);
v_emp                        NUMBER(22);
v_emitir                     CHAR(1);
v_dt_ini                     DATE;
v_dt_fim                     DATE;
v_unidade                    CHAR(1);
v_garagem                    CHAR(1);
v_bens                       CHAR(1);
v_tipo_termo                 VARCHAR2(10);
v_tipo_classif               NUMBER(22);
v_sinc                       CHAR(1);
v_represent                  CHAR(1);
v_tipologia                  NUMBER(3);
v_cons_perc_part             CHAR(1);
v_parc_caucao                CHAR(1);
v_classif_dis                NUMBER(22);
v_conf_divida                CHAR(1);
v_distrato_estudo            CHAR(1);
v_perc_inv                   NUMBER(22, 8);
v_prc_re_valor               NUMBER(22, 8);
v_cto_re_valorvenda          NUMBER(22, 8);
v_cto_re_saldoquitacao       NUMBER(22, 8);
v_mul_re_percentual          NUMBER(22, 8);
v_cto_re_valorpago_corrigido NUMBER(22, 8);
v_cto_re_valorpago           NUMBER(22, 8);
v_cndit_re_valorbase         NUMBER(22, 8);
v_ren_re_fruicao             NUMBER(22, 8);
v_ren_re_multa               NUMBER(22, 8);
v_ren_re_atraso              NUMBER(22, 8);
v_ren_re_valorcorrigido      NUMBER(22, 8);
v_ren_re_pp_valordevolucao   NUMBER(22, 8);
v_ren_re_pp_taxadistrato     NUMBER(22, 8);

CURSOR contratos IS
  SELECT est.org_tab_in_codigo
       , est.org_pad_in_codigo
       , est.org_in_codigo
       , est.org_tau_st_codigo
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
       , DECODE( NVL( est.und_bo_consinvestidor, 'N'), 'S', est.und_in_codigo, est.blo_in_codigo) und_blo_codigo
       , NVL( est.und_re_areaprivativa,0)                                                         und_re_areaprivativa
       , aid.agn_tab_in_codigo
       , aid.agn_pad_in_codigo
       , aid.agn_in_codigo
       , aid.agn_tau_st_codigo
       , age.agn_st_fantasia
       , age.agn_st_nome
       , age.agn_ch_tipopessoafj
       , DECODE( age.agn_ch_tipopessoafj, 'F', pfi.agn_st_cpf
                                        , 'J', age.agn_st_cgc
                                        , age.agn_ch_tipopessoafj)                                agn_ds_cpfcnpj
       , cto.cto_in_codigo
       , cto.cto_ch_status
       , DECODE( cto.cto_ch_tipo, 'M', 'A', cto.cto_ch_tipo)                                      cto_ch_tipo
       , DECODE( cto.cto_ch_status, 'A', 'Ativo'
                                  , 'U', 'Inadimplente'
                                  , 'D', 'Distratado'
                                  , 'T', 'Transferido'
                                  , 'C', 'C. Direitos'
                                  , cto.cto_ch_status)                                            cto_ds_status
       , cto.cto_dt_cadastro                                                                      cto_dt_cadastro
       , ren.ren_dt_geracao                                                                       ren_dt_geracao
       , NVL( ren.ren_ch_tipomulta,'P')                                                           ren_ch_tipomulta
       , NVL( ren.ren_re_pp_taxadistrato, 0)                                                      ren_re_pp_taxadistrato
       , NVL( ren.ren_re_pp_valordevolucao, 0)                                                    ren_re_pp_valordevolucao
       , NVL( ren.ren_re_valorcorrigido, 0)                                                       ren_re_valorcorrigido
       , NVL( ren.ren_re_atraso, 0)                                                               ren_re_atraso
       , NVL( ren.ren_re_multa, 0)                                                                ren_re_multa
       , NVL( ren.ren_re_fruicao, 0)                                                              ren_re_fruicao
       , NVL( cdi.cndit_re_valorbase, 0)                                                          cndit_re_valorbase
       , NVL( cdi.cndit_in_parcela, 0)                                                            cto_nr_parcelas
       , NVL( mgrel.pck_rel_fnc.fnc_car_saldopago_contrato( cto.org_tab_in_codigo
                                                          , cto.org_pad_in_codigo
                                                          , cto.org_in_codigo
                                                          , cto.org_tau_st_codigo
                                                          , cto.cto_in_codigo
                                                          , cto.cto_dt_status
                                                          , 'S'
                                                          , ren.ren_ch_encargo
                                                          , 'S'
                                                          , 'N'), 0)               cto_re_valorpago
       , NVL( mgrel.pck_rel_fnc.fnc_car_saldopago_contrato( cto.org_tab_in_codigo
                                                          , cto.org_pad_in_codigo
                                                          , cto.org_in_codigo
                                                          , cto.org_tau_st_codigo
                                                          , cto.cto_in_codigo
                                                          , cto.cto_dt_status
                                                          , 'SC'
                                                          , ren.ren_ch_encargo
                                                          , 'S'
                                                          , 'N'), 0)               cto_re_valorpago_corrigido
       , mul.mul_ch_tipo                                                                          mul_ch_tipo
       , NVL(mul.mul_re_percentual, 0)                                                            mul_re_percentual
       , cdi.cndit_ch_receita                                                                     cndit_ch_receita
       , DECODE( cdi.cndit_ch_receita, 'C', 'Carteira'
                                    , 'K', 'Carta de Crédito'
                                    , cdi.cndit_ch_receita)                                       cndit_ds_receita
       , pro.pro_tab_in_codigo                                                                    pro_tab_in_codigo
       , pro.pro_pad_in_codigo                                                                    pro_pad_in_codigo
       , pro.pro_ide_st_codigo                                                                    pro_ide_st_codigo
       , pro.pro_in_reduzido                                                                      pro_in_reduzido
       , pro.pro_st_extenso                                                                       pro_st_extenso
       , pro.pro_st_apelido                                                                       pro_st_apelido
       , pro.pro_st_descricao                                                                     pro_st_descricao
       , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato( cto.org_tab_in_codigo
                                                      , cto.org_pad_in_codigo
                                                      , cto.org_in_codigo
                                                      , cto.org_tau_st_codigo
                                                      , cto.cto_in_codigo
                                                      , 'O'  -- considero aqui SEM termos. PINC-4261
                                                      , SYSDATE
                                                      , ''
                                                      , ''), 0)                                   valor_contrato
       , NVL( mgrel.pck_rel_fnc.fnc_car_valortermo( cto.org_tab_in_codigo
                                                  , cto.org_pad_in_codigo
                                                  , cto.org_in_codigo
                                                  , cto.org_tau_st_codigo
                                                  , cto.cto_in_codigo
                                                  , v_tipo_termo), 0)                             valor_termo
       , cla.csf_in_codigo                                                                        csf_in_codigo
       , cla.csf_st_descricao                                                                     csf_st_descricao
       , NVL( cdi.cndit_re_valorbase, 0)                                                          prc_re_valor
       , ren.ren_st_formadevolucao                                                                ren_st_formadevolucao
       , DECODE( ltrim( ren.ren_st_formadevolucao, ' '), NULL, 'N', 'S')                          ren_bo_observacao
       , ren.ren_bo_outrafilial                                                                   ren_bo_outrafilial
       , rgn.agn_in_codigo                                                                        fil_in_codigo
       , rgn.agn_st_fantasia                                                                      fil_st_fantasia
       , rgn.agn_st_nome                                                                          fil_st_nome
       , NVL( agd.agn_st_nome,'X')                                                                agd_distrato
       , NVL( agd.agn_in_codigo,0)                                                                agd_in_codigo
       , NVL( tpu.tpu_in_codigo, DECODE( est.estrutura, 'G', 0
                                                      , 'B', 999))                                tpu_in_codigo
       , tpu.tpu_st_estendida
       , NVL( tpu.tpu_st_descricao, DECODE( est.estrutura, 'G', 'Garagem'
                                                         , 'B', 'Bens de Terceiros'
                                                         , ''))                                   tpu_st_descricao
       , 0                                                                                        identificador
       , ccd.ccd_st_descricao
       , NVL(ren.ren_bo_corrigevlrpago, 'N')                                                      ren_bo_corrigevlrpago

  FROM (SELECT fia.fil_tab_in_codigo  tab_in_codigo
             , fia.fil_pad_in_codigo  pad_in_codigo
             , DECODE(agn.agn_bo_consolidador, 'E', agn.agn_in_codigo, agn.pai_agn_in_codigo) org_in_codigo
             , agn.agn_in_codigo fil_in_codigo

        FROM mgglo.glo_filial_ativa fia
           , mgglo.glo_agentes      agn
        WHERE fia.usu_in_codigo     = v_usu
          AND fia.comp_st_nome      = v_comp

          AND agn.agn_tab_in_codigo = fia.fil_tab_in_codigo
          AND agn.agn_pad_in_codigo = fia.fil_pad_in_codigo
          AND agn.agn_in_codigo     = fia.fil_in_codigo
          AND agn.agn_bo_consolidador IN ('E', 'F')) ati --Traz todas as filiais ativas para o usuário e computador logado.

     , mgcar.car_classificacao_distrato  ccd
     , mgcar.car_multa_distrato          mul
     , mgdbm.dbm_condicao_item           cdi
     , mgglo.glo_pessoa_fisica           pfi
     , mgdbm.dbm_tipologia_unidade       tpu
     , mgdbm.dbm_unidade                 und
     , mgdbm.dbm_condicao                prc
     , mgglo.glo_agentes                 rgn
     , mgcar.car_renegociacao            ren
     , mgdbm.dbm_classificacao           cla
     , mgglo.glo_projetos                pro
     , mgglo.glo_agentes                 agd
     , mgglo.glo_agentes                 age
     , mgglo.glo_agentes_id              aid
     , mgcar.car_contrato_cliente        ctc
     , mgcar.car_contrato                cto
     , mgrel.vw_car_estrutura            est

  WHERE cto.org_tab_in_codigo = DECODE( v_emitir, 'E', v_tab, cto.org_tab_in_codigo)
    AND cto.org_pad_in_codigo = DECODE( v_emitir, 'E', v_pad, cto.org_pad_in_codigo)
    AND cto.org_tau_st_codigo = DECODE( v_emitir, 'E', v_tau, cto.org_tau_st_codigo)

    AND NVL( est.emp_in_codigo, 0) = DECODE( v_emitir, 'E', DECODE( v_emp, 0, NVL( est.emp_in_codigo, 0), v_emp)
                                                          , NVL( est.emp_in_codigo, 0))

    AND est.ctoenv_ch_origem IN( v_unidade, v_garagem, v_bens)

    AND cto.csf_in_codigo = DECODE( v_tipo_classif, 0, cto.csf_in_codigo, v_tipo_classif)

    AND pro.pro_pad_in_codigo = DECODE( v_emitir, 'P', v_pad, pro.pro_pad_in_codigo)
    AND pro.pro_st_extenso    = DECODE( v_emitir, 'P', v_pro_ext, pro.pro_st_extenso)

    AND ren.ren_dt_geracao BETWEEN v_dt_ini AND v_dt_fim

    AND NVL( ccd.ccd_in_codigo, 0) = DECODE( v_classif_dis, 0, NVL( ccd.ccd_in_codigo, 0), v_classif_dis)

    -- Filtro para considerar distratos em estudo, não utiliza a função status na data pois o relatório não possui data base
    AND (( v_distrato_estudo = 'S'
     AND (( ren.ren_ch_status = 'E'
        AND cto.cto_ch_status IN('A', 'U'))
         OR cto.cto_ch_status IN ( 'D', 'T', 'C')))
      OR ( v_distrato_estudo = 'N'
       AND cto.cto_ch_status IN( 'D', 'T', 'C')))

    AND ren.ren_ch_tipo = 'D'

    AND NVL( mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                              , est.org_pad_in_codigo
                                                              , est.org_in_codigo
                                                              , est.org_tau_st_codigo
                                                              , est.emp_in_codigo
                                                              , v_sinc
                                                              ), 0) = NVL( est.emp_in_codigo, 0)

    -- NVL necessário para trazer os contratos cujas unidades não apresentam tipologia
    AND NVL( tpu.tpu_in_codigo,0) = NVL( DECODE( v_tipologia, 0, tpu.tpu_in_codigo, v_tipologia),0)

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

    AND cto.org_tab_in_codigo = ctc.org_tab_in_codigo
    AND cto.org_pad_in_codigo = ctc.org_pad_in_codigo
    AND cto.org_in_codigo     = ctc.org_in_codigo
    AND cto.org_tau_st_codigo = ctc.org_tau_st_codigo
    AND cto.cto_in_codigo     = ctc.cto_in_codigo

    AND ctc.agn_tab_in_codigo = aid.agn_tab_in_codigo
    AND ctc.agn_pad_in_codigo = aid.agn_pad_in_codigo
    AND ctc.agn_in_codigo     = aid.agn_in_codigo
    AND ctc.agn_tau_st_codigo = aid.agn_tau_st_codigo

    AND aid.agn_tab_in_codigo = age.agn_tab_in_codigo
    AND aid.agn_pad_in_codigo = age.agn_pad_in_codigo
    AND aid.agn_in_codigo     = age.agn_in_codigo

    AND cto.pro_tab_in_codigo = pro.pro_tab_in_codigo
    AND cto.pro_pad_in_codigo = pro.pro_pad_in_codigo
    AND cto.pro_ide_st_codigo = pro.pro_ide_st_codigo
    AND cto.pro_in_reduzido   = pro.pro_in_reduzido

    AND cto.csf_in_codigo = cla.csf_in_codigo

    AND cto.org_tab_in_codigo = ren.org_tab_in_codigo
    AND cto.org_pad_in_codigo = ren.org_pad_in_codigo
    AND cto.org_in_codigo     = ren.org_in_codigo
    AND cto.org_tau_st_codigo = ren.org_tau_st_codigo
    AND cto.cto_in_codigo     = ren.cto_in_codigo

    AND cto.org_tab_in_codigo = rgn.agn_tab_in_codigo
    AND cto.org_pad_in_codigo = rgn.agn_pad_in_codigo
    AND cto.fil_in_codigo     = rgn.agn_in_codigo

    AND ren.org_tab_in_codigo = prc.org_tab_in_codigo
    AND ren.org_pad_in_codigo = prc.org_pad_in_codigo
    AND ren.org_in_codigo     = prc.org_in_codigo
    AND ren.org_tau_st_codigo = prc.org_tau_st_codigo
    AND ren.cnd_in_codigo     = prc.cnd_in_codigo

    -- Tabela de Filial Ativa
    AND cto.org_tab_in_codigo = ati.tab_in_codigo
    AND cto.org_pad_in_codigo = ati.pad_in_codigo
    AND cto.org_in_codigo     = ati.org_in_codigo
    AND cto.fil_in_codigo     = ati.fil_in_codigo

    AND est.org_tab_in_codigo = und.org_tab_in_codigo(+)
    AND est.org_pad_in_codigo = und.org_pad_in_codigo(+)
    AND est.org_in_codigo     = und.org_in_codigo    (+)
    AND est.org_tau_st_codigo = und.org_tau_st_codigo(+)
    AND est.und_in_codigo     = und.est_in_codigo    (+)

    AND cto.org_tab_in_codigo = mul.org_tab_in_codigo(+)
    AND cto.org_pad_in_codigo = mul.org_pad_in_codigo(+)
    AND cto.org_in_codigo     = mul.org_in_codigo    (+)
    AND cto.org_tau_st_codigo = mul.org_tau_st_codigo(+)
    AND ( mul.mul_in_codigo      = ( SELECT MAX( b.mul_in_codigo)
                                     FROM mgcar.car_multa_distrato b
                                     WHERE b.org_tab_in_codigo = mul.org_tab_in_codigo
                                       AND b.org_pad_in_codigo = mul.org_pad_in_codigo
                                       AND b.org_in_codigo     = mul.org_in_codigo
                                       AND b.org_tau_st_codigo = mul.org_tau_st_codigo)
       OR mul.mul_in_codigo IS NULL)

    AND ren.org_tab_in_codigo = cdi.org_tab_in_codigo(+)
    AND ren.org_pad_in_codigo = cdi.org_pad_in_codigo(+)
    AND ren.org_in_codigo     = cdi.org_in_codigo    (+)
    AND ren.org_tau_st_codigo = cdi.org_tau_st_codigo(+)
    AND ren.cnd_in_codigo     = cdi.cnd_in_codigo    (+)

    --Relacionamento entre Renegociação e Classificação Contrato
    AND ren.ccd_in_codigo  = ccd.ccd_in_codigo (+)

    AND age.agn_tab_in_codigo = pfi.agn_tab_in_codigo (+)
    AND age.agn_pad_in_codigo = pfi.agn_pad_in_codigo (+)
    AND age.agn_in_codigo     = pfi.agn_in_codigo     (+)
    AND pfi.agn_ch_tipo    (+)= 'P'

    AND und.tpu_in_codigo = tpu.tpu_in_codigo(+)

    AND agd.agn_in_codigo (+)= DECODE( v_represent, 'S', NVL( mgrel.pck_rel_glo_fnc.fnc_glo_retorna_numero( REPLACE( ren.ren_st_retorno, ' ', '')), 0), 0);

CURSOR confisao_divida( v_tab NUMBER, v_pad NUMBER, v_cod NUMBER, v_tau VARCHAR, v_cto NUMBER) IS
  SELECT cto.org_tab_in_codigo
       , cto.org_pad_in_codigo
       , cto.org_in_codigo
       , cto.org_tau_st_codigo
       , cto.cto_in_codigo
       , par.par_bo_confdivida
       , SUM( ROUND(NVL(par.par_re_valorpago, 0), 2)
            + ROUND(NVL(par.par_re_credito, 0), 2)
            + ROUND(NVL(par.par_re_residuocobranca, 0), 2)
            + ROUND(NVL(par.par_re_valormulta, 0), 2)
            + ROUND(NVL(par.par_re_valoratraso, 0), 2)
            - ROUND(NVL(par.par_re_valordesconto, 0), 2)
            -- NOVO valor de resíduo de reajuste anual
            + ROUND(NVL(par.par_re_valorcorrecao_atr, 0), 2)
            + ROUND(NVL(mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                    , par.org_pad_in_codigo
                                                                    , par.org_in_codigo
                                                                    , par.org_tau_st_codigo
                                                                    , par.cto_in_codigo
                                                                    , par.par_in_codigo), 0), 2)) vl_conf_divida
       , SUM(ROUND(NVL(mgcar.pck_car_fnc.fnc_car_corrigecto( par.org_tab_in_codigo
                                                           , par.org_pad_in_codigo
                                                           , par.org_in_codigo
                                                           , par.org_tau_st_codigo
                                                           , par.cto_in_codigo
                                                           , par.par_in_codigo
                                                           , cto.cto_dt_status
                                                           , 'RP'
                                                           , 'A'
                                                           , ren.ren_ch_encargo), 0), 2)
            + ROUND(NVL(mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                    , par.org_pad_in_codigo
                                                                    , par.org_in_codigo
                                                                    , par.org_tau_st_codigo
                                                                    , par.cto_in_codigo
                                                                    , par.par_in_codigo), 0), 2)) vl_conf_divida_cor

  FROM mgcar.car_confissao_divida con
     , mgcar.car_renegociacao     ren
     , mgcar.car_parcela          par
     , mgcar.car_contrato         cto

  WHERE cto.org_tab_in_codigo = v_tab
    AND cto.org_pad_in_codigo = v_pad
    AND cto.org_in_codigo     = v_cod
    AND cto.org_tau_st_codigo = v_tau
    AND cto.cto_in_codigo     = v_cto

    AND par.par_bo_confdivida = 'S'

    AND ren.ren_ch_tipo = 'D'

    AND cto.org_tab_in_codigo = par.org_tab_in_codigo
    AND cto.org_pad_in_codigo = par.org_pad_in_codigo
    AND cto.org_in_codigo     = par.org_in_codigo
    AND cto.org_tau_st_codigo = par.org_tau_st_codigo
    AND cto.cto_in_codigo     = par.cto_in_codigo

    AND cto.org_tab_in_codigo = ren.org_tab_in_codigo
    AND cto.org_pad_in_codigo = ren.org_pad_in_codigo
    AND cto.org_in_codigo     = ren.org_in_codigo
    AND cto.org_tau_st_codigo = ren.org_tau_st_codigo
    AND cto.cto_in_codigo     = ren.cto_in_codigo

    AND con.org_tab_in_codigo = cto.org_tab_in_codigo
    AND con.org_pad_in_codigo = cto.org_pad_in_codigo
    AND con.org_in_codigo     = cto.org_in_codigo
    AND con.org_tau_st_codigo = cto.org_tau_st_codigo
    AND con.cto_in_codigo     = cto.cto_in_codigo
    and con.cdi_ch_status     = 'A'

  GROUP BY cto.org_tab_in_codigo
         , cto.org_pad_in_codigo
         , cto.org_in_codigo
         , cto.org_tau_st_codigo
         , cto.cto_in_codigo
         , par.par_bo_confdivida;

BEGIN

  v_emp             := v_cod_emp;
  v_emitir          := v_emitir_por;
  v_unidade         := unidade;
  v_garagem         := garagem;
  v_bens            := bens;
  v_tipo_termo      := tipo_termo;
  v_tipo_classif    := tipo_classif;
  v_dt_ini          := TO_DATE( v_data_ini, 'DD/MM/YYYY');
  v_dt_fim          := TO_DATE( v_data_fim, 'DD/MM/YYYY');
  v_comp            := comp_st_nome;
  v_usu             := usu_in_codigo;
  v_sinc            := v_sincroniza;
  v_represent       := v_representante;
  v_tipologia       := p_tpu_in_codigo;
  v_cons_perc_part  := p_cons_perc_part;
  v_parc_caucao     := p_parc_caucao;
  v_conf_divida     := p_conf_divida;
  v_classif_dis     := p_classif_dis;
  v_distrato_estudo := p_distrato_estudo;

  IF v_emitir_por = 'P' THEN
    v_fil     := fil_in_codigo;
    v_pro_ext := v_cod_proj;
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
    v_tab := org_tab_in_codigo;
    v_pad := org_pad_in_codigo;
    v_cod := org_in_codigo;
    v_tau := org_tau_st_codigo;
  END IF;

  DELETE FROM mgcustom.rel_dados_contrato_api;
  COMMIT;

  FOR cto IN contratos LOOP
    v_cto_re_saldoquitacao := cto.valor_contrato;
    v_cto_re_valorvenda    := cto.valor_contrato + cto.valor_termo;

    IF v_cons_perc_part = 'S' THEN
      v_perc_inv := ( NVL( mgrel.pck_rel_fnc.fnc_car_busca_perc_part_dist( cto.org_tab_in_codigo
                                                                         , cto.org_pad_in_codigo
                                                                         , cto.org_in_codigo
                                                                         , cto.org_tau_st_codigo
                                                                         , cto.und_blo_codigo
                                                                         , 0),0)/100);
      v_prc_re_valor               := cto.prc_re_valor * v_perc_inv;
      v_cto_re_valorvenda          := v_cto_re_valorvenda * v_perc_inv;
      v_cto_re_saldoquitacao       := v_cto_re_saldoquitacao * v_perc_inv;
      v_mul_re_percentual          := cto.mul_re_percentual * v_perc_inv;
      v_cto_re_valorpago_corrigido := cto.cto_re_valorpago_corrigido * v_perc_inv;
      v_cto_re_valorpago           := cto.cto_re_valorpago * v_perc_inv;
      v_cndit_re_valorbase         := cto.cndit_re_valorbase * v_perc_inv;
      v_ren_re_fruicao             := cto.ren_re_fruicao * v_perc_inv;
      v_ren_re_multa               := cto.ren_re_multa * v_perc_inv;
      v_ren_re_atraso              := cto.ren_re_atraso * v_perc_inv;
      v_ren_re_valorcorrigido      := cto.ren_re_valorcorrigido * v_perc_inv;
      v_ren_re_pp_valordevolucao   := cto.ren_re_pp_valordevolucao * v_perc_inv;
      v_ren_re_pp_taxadistrato     := cto.ren_re_pp_taxadistrato;
    ELSE
      v_prc_re_valor               := cto.prc_re_valor;
      v_mul_re_percentual          := cto.mul_re_percentual;
      v_cto_re_valorpago_corrigido := cto.cto_re_valorpago_corrigido;
      v_cto_re_valorpago           := cto.cto_re_valorpago;
      v_cndit_re_valorbase         := cto.cndit_re_valorbase;
      v_ren_re_fruicao             := cto.ren_re_fruicao;
      v_ren_re_multa               := cto.ren_re_multa;
      v_ren_re_atraso              := cto.ren_re_atraso;
      v_ren_re_valorcorrigido      := cto.ren_re_valorcorrigido;
      v_ren_re_pp_valordevolucao   := cto.ren_re_pp_valordevolucao;
      v_ren_re_pp_taxadistrato     := cto.ren_re_pp_taxadistrato;
    END IF;

    IF v_conf_divida = 'N' THEN
      FOR con IN confisao_divida( cto.org_tab_in_codigo, cto.org_pad_in_codigo, cto.org_in_codigo, cto.org_tau_st_codigo, cto.cto_in_codigo) LOOP
        v_cto_re_valorpago_corrigido := v_cto_re_valorpago_corrigido - con.vl_conf_divida_cor;
        v_cto_re_valorpago           := v_cto_re_valorpago - con.vl_conf_divida;
        v_prc_re_valor               := v_prc_re_valor - con.vl_conf_divida_cor;

        v_ren_re_valorcorrigido      := v_ren_re_valorcorrigido - con.vl_conf_divida_cor;
        v_ren_re_pp_valordevolucao   := v_ren_re_pp_valordevolucao - con.vl_conf_divida;
      END LOOP;
    END IF;

      INSERT INTO mgcustom.rel_dados_contrato_api( est_org_tab_in_codigo
                                          , est_org_pad_in_codigo
                                          , est_org_in_codigo
                                          , est_org_tau_st_codigo
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
                                          , und_re_peso
                                          , agn_tab_in_codigo
                                          , agn_pad_in_codigo
                                          , agn_in_codigo
                                          , agn_tau_st_codigo
                                          , agn_st_fantasia
                                          , agn_st_nome
                                          , agn_ch_tipopessoafj
                                          , agn_cpf_cnpj
                                          , cto_in_codigo
                                          , cto_ch_status
                                          , cto_ch_tipo
                                          , cto_ds_status
                                          , cto_dt_cadastro
                                          , cto_dt_status
                                          , cto_ch_tipomulta
                                          , par_re_valorresiduo
                                          , par_re_valorpagar_atraso
                                          , par_re_valor_corrigido
                                          , par_re_valor_atraso
                                          , par_re_valormulta
                                          , par_re_valor_antchave
                                          , par_re_valororiginal
                                          , cto_in_indice
                                          , par_re_valorpago
                                          , par_re_valor_pagocorrigido
                                          , cnd_ch_tipo
                                          , par_re_credito
                                          , par_ch_origem
                                          , par_ds_origem
                                          , pro_tab_in_codigo
                                          , pro_pad_in_codigo
                                          , pro_ide_st_codigo
                                          , pro_in_reduzido
                                          , pro_st_extenso
                                          , pro_st_apelido
                                          , pro_st_descricao
                                          , par_re_saldoquitacao
                                          , par_re_valor_totalpago
                                          , csf_in_codigo
                                          , csf_st_descricao
                                          , par_re_valorvencer
                                          , oco_st_complemento
                                          , cto_bo_taxaempr
                                          , par_bo_contratual
                                          , fil_in_codigo
                                          , par_st_observacao
                                          , agn_st_bairro
                                          , agn_st_representante
                                          , ocr_in_codigo
                                          , tte_in_codigo
                                          , oco_st_ocorrencia
                                          , ocs_st_descricao
                                          , par_nr_parcelasatraso
                                          , ctt_ds_tipo
                                          , ren_bo_corrigevlrpago)
      VALUES ( cto.org_tab_in_codigo
             , cto.org_pad_in_codigo
             , cto.org_in_codigo
             , cto.org_tau_st_codigo
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
             , cto.und_re_areaprivativa
             , cto.agn_tab_in_codigo
             , cto.agn_pad_in_codigo
             , cto.agn_in_codigo
             , cto.agn_tau_st_codigo
             , cto.agn_st_fantasia
             , cto.agn_st_nome
             , cto.agn_ch_tipopessoafj
             , cto.agn_ds_cpfcnpj
             , cto.cto_in_codigo
             , cto.cto_ch_status
             , cto.cto_ch_tipo
             , cto.cto_ds_status
             , cto.cto_dt_cadastro
             , cto.ren_dt_geracao
             , cto.ren_ch_tipomulta
             , v_ren_re_pp_taxadistrato
             , v_ren_re_pp_valordevolucao
             , v_ren_re_valorcorrigido
             , v_ren_re_atraso
             , v_ren_re_multa
             , v_ren_re_fruicao
             , v_cndit_re_valorbase
             , cto.cto_nr_parcelas
             , v_cto_re_valorpago
             , v_cto_re_valorpago_corrigido
             , cto.mul_ch_tipo
             , v_mul_re_percentual
             , cto.cndit_ch_receita
             , cto.cndit_ds_receita
             , cto.pro_tab_in_codigo
             , cto.pro_pad_in_codigo
             , cto.pro_ide_st_codigo
             , cto.pro_in_reduzido
             , cto.pro_st_extenso
             , cto.pro_st_apelido
             , cto.pro_st_descricao
             , v_cto_re_saldoquitacao
             , v_cto_re_valorvenda
             , cto.csf_in_codigo
             , cto.csf_st_descricao
             , v_prc_re_valor
             , cto.ren_st_formadevolucao
             , cto.ren_bo_observacao
             , cto.ren_bo_outrafilial
             , cto.fil_in_codigo
             , cto.fil_st_fantasia
             , cto.fil_st_nome
             , cto.agd_distrato
             , cto.agd_in_codigo
             , cto.tpu_in_codigo
             , cto.tpu_st_estendida
             , cto.tpu_st_descricao
             , cto.identificador
             , cto.ccd_st_descricao
             , cto.ren_bo_corrigevlrpago);
    COMMIT;
  END LOOP;

OPEN retorno FOR
  SELECT est_org_tab_in_codigo                org_tab_in_codigo
       , est_org_pad_in_codigo                org_pad_in_codigo
       , est_org_in_codigo                    org_in_codigo
       , est_org_tau_st_codigo                org_tau_st_codigo
       , emp_in_codigo                        emp_in_codigo
       , emp_st_codigo                        emp_st_codigo
       , emp_st_nome                          emp_st_nome
       , etp_in_codigo                        etp_in_codigo
       , etp_st_codigo                        etp_st_codigo
       , etp_st_nome                          etp_st_nome
       , blo_in_codigo                        blo_in_codigo
       , blo_st_codigo                        blo_st_codigo
       , blo_st_nome                          blo_st_nome
       , und_in_codigo                        und_in_codigo
       , und_st_codigo                        und_st_codigo
       , und_st_nome                          und_st_nome
       , und_re_peso                          und_re_areaprivativa
       , agn_tab_in_codigo                    agn_tab_in_codigo
       , agn_pad_in_codigo                    agn_pad_in_codigo
       , agn_in_codigo                        agn_in_codigo
       , agn_tau_st_codigo                    agn_tau_st_codigo
       , agn_st_fantasia                      agn_st_fantasia
       , agn_st_nome                          agn_st_nome
       , agn_ch_tipopessoafj                  agn_ch_tipopessoafj
       , agn_cpf_cnpj                         agn_ds_cpfcnpj
       , cto_in_codigo                        cto_in_codigo
       , cto_ch_status                        cto_ch_status
       , cto_ch_tipo                          cto_ch_tipo
       , CAST( cto_ds_status AS VARCHAR2(50)) cto_ds_status
       , cto_dt_cadastro                      cto_dt_cadastro
       , cto_dt_status                        ren_dt_geracao
       , cto_ch_tipomulta                     ren_ch_tipomulta
       , par_re_valorresiduo                  ren_re_pp_taxadistrato
       , par_re_valorpagar_atraso             ren_re_pp_valordevolucao
       , par_re_valor_corrigido               ren_re_valorcorrigido
       , par_re_valor_atraso                  ren_re_atraso
       , par_re_valormulta                    ren_re_multa
       , par_re_valor_antchave                ren_re_fruicao
       , par_re_valororiginal                 cndit_re_valorbase
       , cto_in_indice                        cto_nr_parcelas
       , par_re_valorpago                     cto_re_valorpago
       , par_re_valor_pagocorrigido           cto_re_valorpago_corrigido
       , cnd_ch_tipo                          mul_ch_tipo
       , par_re_credito                       mul_re_percentual
       , par_ch_origem                        cndit_ch_receita
       , par_ds_origem                        cndit_ds_receita
       , pro_tab_in_codigo                    pro_tab_in_codigo
       , pro_pad_in_codigo                    pro_pad_in_codigo
       , pro_ide_st_codigo                    pro_ide_st_codigo
       , pro_in_reduzido                      pro_in_reduzido
       , pro_st_extenso                       pro_st_extenso
       , pro_st_apelido                       pro_st_apelido
       , pro_st_descricao                     pro_st_descricao
       , par_re_saldoquitacao                 cto_re_saldoquitacao
       , par_re_valor_totalpago               cto_re_valorvenda
       , csf_in_codigo                        csf_in_codigo
       , csf_st_descricao                     csf_st_descricao
       , par_re_valorvencer                   prc_re_valor
       , oco_st_complemento                   ren_st_formadevolucao
       , cto_bo_taxaempr                      ren_bo_observacao
       , par_bo_contratual                    ren_bo_outrafilial
       , fil_in_codigo                        fil_in_codigo
       , agn_st_fantasia                      fil_st_fantasia
       , agn_st_nome                          fil_st_nome
       , agn_st_representante                 agd_distrato
       , ocr_in_codigo                        agd_in_codigo
       , tte_in_codigo                        tpu_in_codigo
       , oco_st_ocorrencia                    tpu_st_estendida
       , ocs_st_descricao                     tpu_st_descricao
       , par_nr_parcelasatraso                identificador
       , CAST( ctt_ds_tipo AS VARCHAR2(70))   ccd_st_descricao
       , ren_bo_corrigevlrpago                ren_bo_corrigevlrpago

  FROM mgcustom.rel_dados_contrato_api rel

  UNION ALL
  -- Retornar linha para obtenção do cabeçalho
  -- Select alterado para apresentar o cabeçalho mesmo não havendo registros a serem retornados. Ch.45792.
  SELECT est.org_tab_in_codigo
       , est.org_pad_in_codigo
       , est.org_in_codigo
       , est.org_tau_st_codigo
       , DECODE( v_emp, 0,  0, est.emp_in_codigo)     emp_in_codigo
       , DECODE( v_emp, 0, '', est.emp_st_codigo)     emp_st_codigo
       , DECODE( v_emp, 0, '', est.emp_st_nome)       emp_st_nome
       , 0                                            etp_in_codigo
       , ''                                           etp_st_codigo
       , ''                                           etp_st_nome
       , 0                                            blo_in_codigo
       , ''                                           blo_st_codigo
       , ''                                           blo_st_nome
       , 0                                            und_in_codigo
       , ''                                           und_st_codigo
       , ''                                           und_st_nome
       , 0                                            und_re_areaprivativa
       , 0                                            agn_tab_in_codigo
       , 0                                            agn_pad_in_codigo
       , 0                                            agn_in_codigo
       , ''                                           agn_tau_st_codigo
       , ''                                           agn_st_fantasia
       , ''                                           agn_st_nome
       , ''                                           agn_ch_tipopessoafj
       , ''                                           agn_ds_cpfcnpj
       , NULL                                         cto_in_codigo
       , ''                                           cto_ch_status
       , ''                                           cto_ch_tipo
       , ''                                           cto_ds_status
       , to_date( NULL, 'dd/mm/yyyy')                 cto_dt_cadastro
       , to_date( NULL, 'dd/mm/yyyy')                 ren_dt_geracao
       , ''                                           ren_ch_tipomulta
       , 0                                            ren_re_pp_taxadistrato
       , 0                                            ren_re_pp_valordevolucao
       , 0                                            ren_re_valorcorrigido
       , 0                                            ren_re_atraso
       , 0                                            ren_re_multa
       , 0                                            ren_re_fruicao
       , 0                                            cndit_re_valorbase
       , 0                                            cto_nr_parcelas
       , 0                                            cto_re_valorpago
       , 0                                            cto_re_valorpago_corrigido
       , ''                                           mul_ch_tipo
       , 0                                            mul_re_percentual
       , ''                                           cndit_ch_receita
       , ''                                           cndit_ds_receita
       , pro.pro_tab_in_codigo                        pro_tab_in_codigo
       , pro.pro_pad_in_codigo                        pro_pad_in_codigo
       , pro.pro_ide_st_codigo                        pro_ide_st_codigo
       , pro.pro_in_reduzido                          pro_in_reduzido
       , pro.pro_st_extenso                           pro_st_extenso
       , pro.pro_st_apelido                           pro_st_apelido
       , pro.pro_st_descricao                         pro_st_descricao
       , 0                                            cto_re_saldoquitacao
       , 0                                            cto_re_valorvenda
       , 0                                            csf_in_codigo
       , ''                                           csf_st_descricao
       , 0                                            prc_re_valor
       , ''                                           ren_st_formadevolucao
       , ''                                           ren_bo_observacao
       , ''                                           ren_bo_outrafilial
       , est.fil_in_codigo                            fil_in_codigo
       , ''                                           fil_st_fantasia
       , ''                                           fil_st_nome
       , ''                                           agd_distrato
       , 0                                            agd_in_codigo
       , 0                                            tpu_in_codigo
       , ''                                           tpu_st_estendida
       , ''                                           tpu_st_descricao
       , 1                                            identificador
       , ''                                           ccd_st_descricao
       , ''                                           ren_bo_corrigevlrpago

  FROM (SELECT fia.fil_tab_in_codigo  tab_in_codigo
             , fia.fil_pad_in_codigo  pad_in_codigo
             , DECODE(agn.agn_bo_consolidador, 'E', agn.agn_in_codigo, agn.pai_agn_in_codigo) org_in_codigo
             , agn.agn_in_codigo fil_in_codigo

          FROM mgglo.glo_filial_ativa fia
             , mgglo.glo_agentes      agn
          WHERE fia.usu_in_codigo     = v_usu
            AND fia.comp_st_nome      = v_comp

            AND agn.agn_tab_in_codigo = fia.fil_tab_in_codigo
            AND agn.agn_pad_in_codigo = fia.fil_pad_in_codigo
            AND agn.agn_in_codigo     = fia.fil_in_codigo
            AND agn.agn_bo_consolidador IN ('E', 'F')) ati -- Filial Ativa

     , mgglo.glo_projetos            pro
     , mgrel.vw_car_estrutura        est
  WHERE est.org_tab_in_codigo = DECODE( v_emitir, 'E', v_tab, est.org_tab_in_codigo)
    AND est.org_pad_in_codigo = DECODE( v_emitir, 'E', v_pad, est.org_pad_in_codigo)
    AND est.org_tau_st_codigo = DECODE( v_emitir, 'E', v_tau, est.org_tau_st_codigo)

    AND NVL(est.emp_in_codigo, 0) = DECODE( v_emitir, 'E',(DECODE( v_emp, 0, NVL(est.emp_in_codigo, 0), v_emp))
                                                         , NVL(est.emp_in_codigo, 0))

    AND NVL(mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                             , est.org_pad_in_codigo
                                                             , est.org_in_codigo
                                                             , est.org_tau_st_codigo
                                                             , est.emp_in_codigo
                                                             , v_sinc
                                                             ), 0) = NVL(est.emp_in_codigo, 0)

    AND pro.pro_pad_in_codigo = DECODE( v_emitir, 'P', v_pad, pro.pro_pad_in_codigo)
    AND pro.pro_st_extenso    = DECODE( v_emitir, 'P', v_pro_ext, pro.pro_st_extenso)

   AND rownum = 1

   AND est.pro_tab_in_codigo = pro.pro_tab_in_codigo
   AND est.pro_pad_in_codigo = pro.pro_pad_in_codigo
   AND est.pro_ide_st_codigo = pro.pro_ide_st_codigo
   AND est.pro_in_reduzido   = pro.pro_in_reduzido

   -- Tabela de Filial Ativa
   AND est.org_tab_in_codigo = ati.tab_in_codigo
   AND est.org_pad_in_codigo = ati.pad_in_codigo
   AND est.org_in_codigo     = ati.org_in_codigo
   AND est.fil_in_codigo     = ati.fil_in_codigo;

END PRC_CAR_RelacaoDistrato;
