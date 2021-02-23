SELECT est.emp_in_codigo
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
       , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato( cto.org_tab_in_codigo
                                                      , cto.org_pad_in_codigo
                                                      , cto.org_in_codigo
                                                      , cto.org_tau_st_codigo
                                                      , cto.cto_in_codigo
                                                      , 'D' -- Valor Original do Cto + Termos do tipo Altera o Valor do Contrato
                                                      , :v_dt_base
                                                      , ''
                                                      , ''), 0) cto_re_valorcontrato
       , NVL( mgrel.pck_rel_fnc.fnc_car_valor_contrato( cto.org_tab_in_codigo
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
       , mgrel.pck_rel_fnc.fnc_car_status_cto_data ( cto.org_tab_in_codigo
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
                                         , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
                                                                                          , cto.org_pad_in_codigo
                                                                                          , cto.org_in_codigo
                                                                                          , cto.org_tau_st_codigo
                                                                                          , cto.cto_in_codigo
                                                                                          , :v_data_base
                                                                                          , 'C'))) agn_in_codigo --PINC-3803
       , agn.agn_st_fantasia
       , DECODE(cto.cto_bo_cessaodir, 'N', agn.agn_st_nome
                                         , mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
                                                                                , cto.org_pad_in_codigo
                                                                                , cto.org_in_codigo
                                                                                , cto.org_tau_st_codigo
                                                                                , cto.cto_in_codigo
                                                                                , :v_data_base
                                                                                , 'N')) agn_st_nome --PINC-3803
       , agn.agn_ch_tipopessoafj
       , agn.agn_st_email
       , CAST( NVL( mgrel.pck_rel_glo_fnc.fnc_glo_concatena_telagente( agn.agn_tab_in_codigo
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

  FROM mgcon.con_centro_custo       cus
     , mgglo.glo_projetos           pro
     , mgglo.glo_agentes            agn
     , mgglo.glo_agentes_id         aid
     , mgcar.car_contrato_cliente   cli
     , mgdbm.dbm_condicao           cnd
     , mgrel.vw_car_estrutura       est
     , mgcar.car_contrato           cto

  WHERE cto.org_tab_in_codigo = :v_tab
    AND cto.org_pad_in_codigo = :v_pad
    AND cto.org_in_codigo     = :v_cod
    AND cto.org_tau_st_codigo = :v_tau
    AND cto.fil_in_codigo     = :v_fil

    AND NVL( est.emp_in_codigo, 0) = DECODE( nvl(:v_emp, 0)  , 0, NVL( est.emp_in_codigo, 0), :v_emp)
    AND NVL( est.blo_in_codigo, 0) = DECODE( nvl(:v_blc, 0)  , 0, NVL( est.blo_in_codigo, 0), :v_blc)
    AND cto.cto_in_codigo          = DECODE( nvl(:v_cto, 0)  , 0, cto.cto_in_codigo, :v_cto)
    AND agn.agn_in_codigo          = DECODE( NVL( :v_agn_in_codigo, 0), 0, DECODE(cto.cto_bo_cessaodir, 'N', agn.agn_in_codigo
                                                                                                          , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
                                                                                                                                                           , cto.org_pad_in_codigo
                                                                                                                                                           , cto.org_in_codigo
                                                                                                                                                           , cto.org_tau_st_codigo
                                                                                                                                                           , cto.cto_in_codigo
                                                                                                                                                           , :v_data_base
                                                                                                                                                           , 'C'))) -- PINC-3803
                                                                        , NVL( :v_agn_in_codigo, 0))

    -- Evita que realiza a consulta para todos empreendimentos
    AND (( nvl(:v_emp, 0) <> 0) OR ( nvl(:v_blc, 0) <> 0) OR ( nvl(:v_cto, 0) <> 0) OR ( nvl(:v_agn_in_codigo, 0) <> 0))

    AND mgrel.pck_rel_fnc.fnc_car_status_cto_data( cto.org_tab_in_codigo
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
                                                                , TO_NUMBER(mgcwb.pck_cwb_fnc.FNC_CWB_Nome_Agente( cto.org_tab_in_codigo
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
    AND cus.cus_in_reduzido   = cto.cus_in_reduzido


v_dt_base
v_data_base
v_tab
v_pad
v_cod
v_tau
v_fil
v_emp
v_blc
v_cto
v_agn_in_codigo
v_ativo
v_quit
v_inadim
v_distr
v_cessao
v_trans

v_tab          := org_tab_in_codigo;
v_pad          := org_pad_in_codigo;
v_cod          := org_in_codigo;
v_fil          := fil_in_codigo;
v_tau          := org_tau_st_codigo;
v_emp          := cod_est;
v_cto          := cod_cto;
v_blc          := cod_blc;
v_dt_base      := to_date( v_data_base,'dd/mm/yyyy');
v_termo        := v_st_termo;
v_des          := v_tdes;
v_des1         := v_tdes1;
v_con          := v_tcon;
v_con1         := v_tcon1;
v_reajuste     := v_descongela;
v_ctrl_cheq    := v_ctrl_cheque;
v_descapit_tp  := v_descap_tp;
v_descapit_sac := v_descap_sac;
v_tp_par       := v_tipo_parcela;
v_ativo        := v_ati;
v_quit         := v_qui;
v_inadim       := v_ina;
v_distr        := v_dis;
v_cessao       := v_ces;
v_trans        := v_tra;
v_cons_taxa    := p_cons_taxa;
v_cons_bonif   := p_cons_bonif;
v_vl_corrigido := p_vl_corrigido;
v_consinv      := p_investidor;
v_conf_divida  := p_cons_confdivida;
v_agn_in_codigo:= p_agn_in_codigo;
v_par_secur    := p_par_secur;
vsConsJrsTPSAC := p_cons_jr_tp_sac;