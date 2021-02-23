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
          , cto.cto_org_tab_in_codigo       --14
          , cto.cto_org_pad_in_codigo       --15
          , cto.cto_org_in_codigo           --16
          , cto.cto_org_tau_st_codigo       --17
          , cto.cto_in_codigo               --18
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

          AND par.org_tab_in_codigo = cto.cto_org_tab_in_codigo
          AND par.org_pad_in_codigo = cto.cto_org_pad_in_codigo
          AND par.org_in_codigo     = cto.cto_org_in_codigo
          AND par.org_tau_st_codigo = cto.cto_org_tau_st_codigo
          AND par.cto_in_codigo     = cto.cto_in_codigo

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
          AND tpb.par_in_codigo     (+)= par.par_in_codigo;