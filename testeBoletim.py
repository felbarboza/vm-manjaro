import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt

cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
con = cx_Oracle.connect("life", "lli8339e", "megacloud")
cursor = con.cursor()

temp='''
v_tab             NUMBER(3);
v_pad             NUMBER(3);
v_tau             VARCHAR2(3);
v_fil             NUMBER(7);
v_dt_ini          DATE;
v_dt_fim          DATE;
v_por_data_baixa  VARCHAR2(1);
v_cto_venda       CHAR(1);
v_cto_permuta     CHAR(1);
v_cto_aluguel     CHAR(1);
v_emitir_por      CHAR(1);
v_rec_cheque      CHAR(1);
v_rec_dinheiro    CHAR(1);
v_rec_bens        CHAR(1);
v_rec_deposito    CHAR(1);
v_rec_carta       CHAR(1);
v_rec_permuta     CHAR(1);
v_rec_boleto      CHAR(1);
v_rec_ted         CHAR(1);
v_rec_doc         CHAR(1);
v_rec_boleto_av   CHAR(1);
v_rec_subsidio    CHAR(1);
v_rec_fin_dir     CHAR(1);
v_csf_cto         NUMBER(22);
v_dt_assi         DATE;
v_dt_assf         DATE;
v_cod             NUMBER(7);
v_cta_fin         NUMBER(7);
VConsT            VARCHAR2(255);
VDescT            VARCHAR2(255);
v_contrl_cheque   VARCHAR2(2);
v_sinc            CHAR(1);
v_cto_unid        CHAR(1);
v_cto_gara        CHAR(1);
v_cto_bens        CHAR(1);
v_mov_naointeg    CHAR(1);
v_tp_dt_cto       CHAR(1);
v_ch_parcela      CHAR(1);
v_proj            VARCHAR2(25);
v_usu             NUMBER(22);
v_comp            VARCHAR2(255);
v_calc_online     CHAR(1);
v_cons_investid   CHAR(1);
v_ch_rec_securit  CHAR(1);
v_cod_emp         NUMBER(22);
v_tre_carteira    CHAR(1);
v_tre_finan       CHAR(1);
v_tre_fgts        CHAR(1);
v_tre_bens        CHAR(1);
v_tre_carta       CHAR(1);
v_tre_permuta     CHAR(1);
v_termo           VARCHAR2(6);
v_cto_ativ        VARCHAR2(1);
v_cto_inad        VARCHAR2(1);
v_cto_dist        VARCHAR2(1);
v_cto_tran        VARCHAR2(1);
v_cto_cesd        VARCHAR2(1);
v_cto_quit        VARCHAR2(1);
v_confdivida      VARCHAR2(1);
v_parc_caucao     VARCHAR2(1);
v_cod_und         NUMBER(22);
v_rec_repasse     CHAR(1);
v_mostra_par_smov CHAR(1);
v_emitir_inv      CHAR(1);
v_inv             NUMBER (22);
v_perc_inv        NUMBER(22, 8);
v_cons_cessao     CHAR(1);
v_tem_cessao      NUMBER(22);
V_mov_integrados  CHAR(1);
v_cons_sts_atual  CHAR(1);

BEGIN
  v_comp          := comp_st_nome;
  v_usu           := usu_in_codigo;
  v_cod           := v_cod_cto;
  v_calc_online   := calcula_on_line;
  v_cons_investid := atribui_perc_part;
  v_emitir_por    := emitir_por;
  v_cons_cessao   := p_cons_cessao;
  V_mov_integrados:= p_mov_integrados;

  IF (v_tdes <> '0' AND v_tdes1 <> '0') THEN
    VDescT := '-'|| v_tdes || '-' || v_tdes1 || '-';
  ELSIF (v_tdes <> '0' AND v_tdes1 = '0') THEN
    VDescT := '-'|| v_tdes || '-';
  ELSIF (v_tdes = '0' AND v_tdes1 <> '0') THEN
    VDescT := '-' || v_tdes1 || '-';
  ELSE
    VDescT := '';
  END IF;

  IF (v_tcon <> '0' AND v_tcon1 <> '0') THEN
    VConsT := '-'|| v_tcon || '-' || v_tcon1 || '-';
  ELSIF (v_tcon <> '0' AND v_tcon1 = '0') THEN
    VConsT := '-'|| v_tcon || '-';
  ELSIF (v_tcon = '0' AND v_tcon1 <> '0') THEN
    VConsT := '-' || v_tcon1 || '-';
  ELSE
    VConsT := '';
  END IF;

  IF v_emitir_por = 'P' THEN
    v_proj := cd_proj;
    BEGIN
      SELECT def.pad_in_codigo
      INTO v_pad
      FROM mgglo.glo_padrao    pdr
         , mgglo.glo_tabela    tbl
         , mgglo.glo_definicao def
      WHERE def.fil_in_codigo = v_fil
        AND tbl.tab_in_codigo = 57
        AND def.tab_in_codigo = tbl.tab_in_codigo
        AND pdr.tab_in_codigo = def.tab_in_codigo
        AND pdr.pad_in_codigo = def.pad_in_codigo
        AND def.def_dt_inicio = ( SELECT MAX(dfi.def_dt_inicio)
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
    v_tau := org_tau_st_codigo;
  END IF;

  v_dt_ini          := TO_DATE(v_data_ini, 'dd/mm/yyyy');
  v_dt_fim          := TO_DATE(v_data_fim, 'dd/mm/yyyy');
  v_por_data_baixa  := v_por_dt_baixa;
  v_cto_venda       := cto_venda;
  v_cto_permuta     := cto_permuta;
  v_cto_aluguel     := cto_aluguel;
  v_rec_cheque      := rec_cheque;
  v_rec_dinheiro    := rec_dinheiro;
  v_rec_bens        := rec_bens;
  v_rec_deposito    := rec_deposito;
  v_rec_carta       := rec_carta;
  v_rec_permuta     := rec_permuta;
  v_rec_boleto      := rec_boleto;
  v_rec_ted         := rec_ted;
  v_rec_doc         := rec_doc;
  v_rec_boleto_av   := rec_boleto_avulso;
  v_rec_subsidio    := p_rec_subsidio;
  v_rec_fin_dir     := p_rec_fin_dir;
  v_csf_cto         := csf_cto;
  v_dt_assi         := TO_DATE(pdt_ass_ini, 'dd/mm/yyyy');
  v_dt_assf         := TO_DATE(pdt_ass_fim, 'dd/mm/yyyy');
  v_contrl_cheque   := v_ctrl_cheque;
  v_cto_unid        := v_cto_und;
  v_cto_gara        := v_cto_gar;
  v_cto_bens        := v_cto_bem;
  v_cta_fin         := cta_fin;
  v_mov_naointeg    := v_mov_naointegrados;
  v_sinc            := v_sincroniza;
  v_tp_dt_cto       := p_tp_dt_cto;
  v_ch_parcela      := p_ch_parcela;
  v_ch_rec_securit  := p_ch_rec_securit;
  v_cod_emp         := cod_emp;
  v_tre_carteira    := tre_carteira;
  v_tre_finan       := tre_finan;
  v_tre_fgts        := tre_fgts;
  v_tre_bens        := tre_bens;
  v_tre_carta       := tre_carta;
  v_tre_permuta     := tre_permuta;
  v_termo           := v_st_termo;
  v_cto_ativ        := v_ativ;
  v_cto_inad        := v_inad;
  v_cto_dist        := v_dist;
  v_cto_tran        := v_tran;
  v_cto_cesd        := v_cesd;
  v_cto_quit        := v_quit;
  v_confdivida      := pconfdivida;
  v_parc_caucao     := pparc_caucao;
  v_cod_und         := pcod_und;
  v_rec_repasse     := rec_repasse;
  v_mostra_par_smov := p_mostra_par_smov;
  v_emitir_inv      := p_emitir_inv;
  v_tem_cessao      := 0;
  v_cons_sts_atual  := p_cons_sts_atual;


  IF Nvl(p_inv, '0') = '0' THEN
    v_inv := to_number( p_inv);
  ELSE
    v_inv := to_number(substr(p_inv, instr(p_inv, '#') + 1, length ( p_inv)));
  END IF;

  DELETE FROM mgcustom.rel_dados_contrato_api;
  COMMIT;

  FOR inv IN investidores LOOP
    FOR cto IN emp_cto( inv.org_tab_in_codigo, inv.org_pad_in_codigo, inv.org_in_codigo, inv.org_tau_st_codigo, inv.cto_in_codigo) LOOP
      IF v_calc_online = 'S' THEN
        mgcar.pck_car_contabil.prc_car_calcula_baixatp( cto.org_tab_in_codigo
                                                      , cto.org_pad_in_codigo
                                                      , cto.org_in_codigo
                                                      , cto.org_tau_st_codigo
                                                      , cto.cto_in_codigo);
      END IF;
      v_tem_cessao := 0;
      BEGIN
        SELECT COUNT (*)
          INTO v_tem_cessao
          FROM mgcar.car_cliente_transferido trr
         WHERE trr.org_tab_in_codigo = cto.org_tab_in_codigo
           AND trr.org_pad_in_codigo = cto.org_pad_in_codigo
           AND trr.org_in_codigo     = cto.org_in_codigo
           AND trr.org_tau_st_codigo = cto.org_tau_st_codigo
           AND trr.cto_in_codigo     = cto.cto_in_codigo;
      EXCEPTION WHEN OTHERS THEN
      v_tem_cessao := 0;
      END;

      FOR par IN parcelas( cto.org_tab_in_codigo, cto.org_pad_in_codigo, cto.org_in_codigo, cto.org_tau_st_codigo, cto.cto_in_codigo, cto.ent_dt_entrega, cto.cto_dt_ini, cto.cto_dt_fim, cto.pco_bo_jrotpsaldovo) LOOP
         IF v_cons_investid = 'N' THEN
          v_perc_inv := 100;
         ELSE
          v_perc_inv :=  NVL( mgrel.pck_rel_fnc.fnc_car_busca_perc_part_dist( inv.org_tab_in_codigo
                                                                            , inv.org_pad_in_codigo
                                                                            , inv.org_in_codigo
                                                                            , inv.org_tau_st_codigo
                                                                            , cto.und_blo_codigo
                                                                            , inv.agn_in_codigo
                                                                            , par.org_tab_in_codigo
                                                                            , par.org_pad_in_codigo
                                                                            , par.org_in_codigo
                                                                            , par.org_tau_st_codigo
                                                                            , par.cto_in_codigo
                                                                            , par.par_in_codigo), 0);
        END IF;
       
      END LOOP;
    END LOOP;
  END LOOP;


    ORDER BY und_st_codigo
           , par_dt_vencimento;'''


cursorInvestidores='''CURSOR investidores IS
  SELECT 
       , agn.agn_st_nome   inv_st_nome
       , agn.agn_in_codigo inv_in_codigo
  FROM  mgglo.glo_agentes       agn
     -- Filial Ativa

  WHERE est.org_tab_in_codigo = DECODE( v_emitir_por, 'E', v_tab, est.org_tab_in_codigo)
    AND est.org_pad_in_codigo = DECODE( v_emitir_por, 'E', v_pad, est.org_pad_in_codigo)
    AND est.org_tau_st_codigo = DECODE( v_emitir_por, 'E', v_tau, est.org_tau_st_codigo)
    AND est.cto_in_codigo = DECODE( v_cod, 0, est.cto_in_codigo, v_cod)
    
    AND mgrel.pck_rel_fnc.fnc_car_status_cto_data( est.org_tab_in_codigo
                                                 , est.org_pad_in_codigo
                                                 , est.org_in_codigo
                                                 , est.org_tau_st_codigo
                                                 , est.cto_in_codigo
                                                 , TRUNC(DECODE( v_cons_sts_atual, 'N', v_dt_fim, SYSDATE))
                                                 , 'N'
                                                 , 'S') IN ( v_cto_ativ, v_cto_inad, v_cto_dist, v_cto_tran, v_cto_cesd, v_cto_quit)

    AND DECODE( v_emitir_por, 'E', DECODE(NVL(est.emp_in_codigo, 0), DECODE( v_cod_emp, 0, NVL(est.emp_in_codigo, 0), v_cod_emp), 1
                                                                   , 0, 1
                                                                   , 0 )
                            , 'P', 1 ) = 1

    AND DECODE( v_emitir_por, 'E', DECODE(NVL(est.blo_in_codigo, 0), DECODE( 0, 0, NVL(est.blo_in_codigo, 0), 0), 1
                                                                   , 0, 1
                                                                   , 0 )
                            , 'P', 1 ) = 1

    AND DECODE( v_emitir_por, 'E', DECODE(NVL(est.und_in_codigo, 0), DECODE( 0, 0, NVL(est.und_in_codigo, 0), 0), 1
                                                                   , 0 , 1
                                                                   , 0 )
                            , 'P', 1 ) = 1

    AND NVL( mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                              , est.org_pad_in_codigo
                                                              , est.org_in_codigo
                                                              , est.org_tau_st_codigo
                                                              , est.emp_in_codigo
                                                              , v_sinc
                                                              ), 0) = NVL( est.emp_in_codigo, 0)

    AND est.ctoenv_ch_origem IN ( v_cto_unid, v_cto_gara, v_cto_bens)

    AND inv.agn_in_codigo = DECODE( v_inv, 0, inv.agn_in_codigo, v_inv)

    AND v_emitir_inv = 'S'
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

  FROM mgglo.glo_agentes            oag -- CNPJ da filial
     , mgglo.glo_agentes_id         oai -- filial
     -- Filial Ativa
     , (SELECT fia.fil_tab_in_codigo  tab_in_codigo
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
          AND agn.agn_bo_consolidador IN ('E', 'F')) ati
     , mgcar.car_contrato           cto

  WHERE cto.org_tab_in_codigo = DECODE( v_emitir_por, 'E', v_tab, cto.org_tab_in_codigo)
    AND cto.org_pad_in_codigo = DECODE( v_emitir_por, 'E', v_pad, cto.org_pad_in_codigo)
    AND cto.org_tau_st_codigo = DECODE( v_emitir_por, 'E', v_tau, cto.org_tau_st_codigo)
    AND cto.cto_in_codigo = DECODE( v_cod, 0, cto.cto_in_codigo, v_cod)


    AND mgrel.pck_rel_fnc.fnc_car_status_cto_data( cto.org_tab_in_codigo
                                                 , cto.org_pad_in_codigo
                                                 , cto.org_in_codigo
                                                 , cto.org_tau_st_codigo
                                                 , cto.cto_in_codigo
                                                 , TRUNC(DECODE( v_cons_sts_atual, 'N', v_dt_fim, SYSDATE))
                                                 , 'N'
                                                 , 'S') IN ( v_cto_ativ, v_cto_inad, v_cto_dist, v_cto_tran, v_cto_cesd, v_cto_quit)

    AND cto.cto_ch_tipo IN ( v_cto_venda, v_cto_permuta, v_cto_aluguel)

    AND DECODE( v_tp_dt_cto, 'C', cto.cto_dt_cadastro, cto.cto_dt_assinatura) >= v_dt_assi
    AND DECODE( v_tp_dt_cto, 'C', cto.cto_dt_cadastro, cto.cto_dt_assinatura) <= v_dt_assf

    AND ( v_inv = 0 OR
        ( v_inv <> 0 AND v_emitir_inv = 'N'))


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
    AND oai.agn_in_codigo     = oag.agn_in_codigo;'''

cursorContratos='''CURSOR emp_cto( :v_tab NUMBER, :v_pad NUMBER, :v_cod NUMBER, :v_tau VARCHAR2, :v_cto NUMBER) IS
  SELECT cto.org_tab_in_codigo
       , cto.org_pad_in_codigo
       , cto.org_in_codigo
       , cto.org_tau_st_codigo
       , cto.cto_in_codigo
       , cto.fil_in_codigo
       , agf.agn_st_nome fil_st_nome
       , mgrel.pck_rel_fnc.fnc_car_status_cto_data( cto.org_tab_in_codigo
                                                  , cto.org_pad_in_codigo
                                                  , cto.org_in_codigo
                                                  , cto.org_tau_st_codigo
                                                  , cto.cto_in_codigo
                                                  , v_dt_fim
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
       , NVL(age.cto_dt_fim, v_data_fim) cto_dt_fim
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

  FROM mgdbm.dbm_parametro_contabilidade pco
     , mgdbm.dbm_classificacao           cla
     , mgcon.con_centro_custo            ccc
     , mgglo.glo_projetos                pro
     , mgdbm.dbm_entrega_obra            ent
     , mgglo.glo_agentes                 agf
     , mgglo.glo_agentes_id              aif
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
                      FROM mgcar.car_cliente_transferido cc
                     WHERE cc.ctr_in_codigo = (SELECT MAX(ctr.ctr_in_codigo)
                                                 FROM mgcar.car_cliente_transferido ctr
                                                WHERE ctr.org_tab_in_codigo = ctt.org_tab_in_codigo
                                                  AND ctr.org_pad_in_codigo = ctt.org_pad_in_codigo
                                                  AND ctr.org_in_codigo     = ctt.org_in_codigo
                                                  AND ctr.org_tau_st_codigo = ctt.org_tau_st_codigo
                                                  AND ctr.cto_in_codigo     = ctt.cto_in_codigo
                                                  AND ctr.ctr_in_codigo     < ctt.ctr_in_codigo)
                       AND cc.cto_in_codigo = ctt.cto_in_codigo), cto.cto_dt_cadastro ) cto_dt_ini
               , (ctt.ctr_dt_processo) -1 cto_dt_fim
            FROM mgglo.glo_agentes             agg
             , mgglo.glo_agentes_id          aid
             , mgcar.car_cliente_transferido ctt
             , mgcar.car_contrato            cto
            WHERE cto.org_tab_in_codigo = :v_tab
            AND cto.org_pad_in_codigo = :v_pad
            AND cto.org_in_codigo     = :v_cod
            AND cto.org_tau_st_codigo = :v_tau
            AND cto.cto_in_codigo     = :v_cto

            AND v_cons_cessao = 'S' -- Considero clientes de cessão de direitos

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
                   FROM mgcar.car_cliente_transferido ctr
                  WHERE ctr.org_tab_in_codigo = cte.org_tab_in_codigo
                    AND ctr.org_pad_in_codigo = cte.org_pad_in_codigo
                    AND ctr.org_in_codigo     = cte.org_in_codigo
                    AND ctr.org_tau_st_codigo = cte.org_tau_st_codigo
                    AND ctr.cto_in_codigo     = cte.cto_in_codigo), v_dt_ini) cto_dt_ini
              , v_dt_fim cto_dt_fim
            FROM mgglo.glo_agentes          aag
             , mgglo.glo_agentes_id       ida
             , mgcar.car_contrato_cliente cte
             , mgcar.car_contrato         cto
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

     , mgcar.car_contrato_cliente        ccl
     , mgrel.vw_car_estrutura            est
     , mgcar.car_contrato                cto

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

    AND NVL( mgrel.pck_rel_glo_fnc.fnc_glo_empreendimento_sinc( est.org_tab_in_codigo
                                                              , est.org_pad_in_codigo
                                                              , est.org_in_codigo
                                                              , est.org_tau_st_codigo
                                                              , est.emp_in_codigo
                                                              , :v_sinc
                                                              ), 0) = NVL( est.emp_in_codigo, 0)

    AND est.ctoenv_ch_origem IN ( :v_cto_unid, :v_cto_gara, :v_cto_bens)

    AND cla.csf_in_codigo = DECODE( :v_csf_cto, 0, cla.csf_in_codigo, :v_csf_cto)

    AND mgrel.pck_rel_fnc.fnc_car_status_cto_data( cto.org_tab_in_codigo
                                                 , cto.org_pad_in_codigo
                                                 , cto.org_in_codigo
                                                 , cto.org_tau_st_codigo
                                                 , cto.cto_in_codigo
                                                 , TRUNC(DECODE( :v_cons_sts_atual, 'N', :v_dt_fim, SYSDATE))
                                                 , 'N'
                                                 , 'S') IN ( :v_cto_ativ, :v_cto_inad, :v_cto_dist, :v_cto_tran, :v_cto_cesd, :v_cto_quit)

    AND cto.cto_ch_tipo IN ( v_cto_venda, v_cto_permuta, v_cto_aluguel)

    AND DECODE( v_tp_dt_cto, 'C', cto.cto_dt_cadastro, cto.cto_dt_assinatura) >= v_dt_assi
    AND DECODE( v_tp_dt_cto, 'C', cto.cto_dt_cadastro, cto.cto_dt_assinatura) <= v_dt_assf

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
                                 FROM mgdbm.dbm_entrega_obra eob
                                 WHERE eob.org_tab_in_codigo = ent.org_tab_in_codigo
                                   AND eob.org_pad_in_codigo = ent.org_pad_in_codigo
                                   AND eob.org_in_codigo     = ent.org_in_codigo
                                   AND eob.org_tau_st_codigo = ent.org_tau_st_codigo
                                   AND eob.est_in_codigo     = ent.est_in_codigo))
         OR (ent.ent_dt_cadastro IS NULL))

    AND pco.org_tab_in_codigo (+)= est.org_tab_in_codigo
    AND pco.org_pad_in_codigo (+)= est.org_pad_in_codigo
    AND pco.org_in_codigo     (+)= est.fil_in_codigo
    AND pco.org_tau_st_codigo (+)= est.org_tau_st_codigo;'''

cursorParcelas='''CURSOR parcelas( v_tab NUMBER, v_pad NUMBER, v_cod NUMBER, v_tau VARCHAR2, v_cto NUMBER, v_ent_dt_entrega DATE, v_cto_dt_ini DATE, v_cto_dt_fim DATE, v_fil_jrotpsaldovo VARCHAR2) IS
  SELECT DISTINCT par.org_tab_in_codigo
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
                , ROUND(NVL(mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
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
                , DECODE(SIGN(par.par_dt_baixa - v_ent_dt_entrega), 1, ROUND(NVL(mgcar.pck_car_fnc.fnc_car_valorcorrecao( par.org_tab_in_codigo
                                                                                                                          , par.org_pad_in_codigo
                                                                                                                          , par.org_in_codigo
                                                                                                                          , par.org_tau_st_codigo
                                                                                                                          , par.cto_in_codigo
                                                                                                                          , par.par_in_codigo
                                                                                                                          , v_ent_dt_entrega
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
                                                     + NVL(mgcar.pck_car_fnc.fnc_car_total_taxasparcela( par.org_tab_in_codigo
                                                                                                       , par.org_pad_in_codigo
                                                                                                       , par.org_in_codigo
                                                                                                       , par.org_tau_st_codigo
                                                                                                       , par.cto_in_codigo
                                                                                                       , par.par_in_codigo), 0)), 2) vl_pago
                , ROUND(NVL(par.par_re_valorcorrecaobx, 0), 2) par_re_valorcorrecaobx
                , ROUND(NVL(par.par_re_valordesconto, 0), 2)   par_re_valordesconto
                , DECODE ( mgcar.pck_car_fnc.fnc_car_paiorigem( par.org_tab_in_codigo
                                                              , par.org_pad_in_codigo
                                                              , par.org_in_codigo
                                                              , par.org_tau_st_codigo
                                                              , par.cto_in_codigo
                                                              , par.par_in_codigo) , 'T', ROUND( NVL(par.par_re_valordesconto, 0) - ((NVL(par.par_re_jrotpncobrado, 0) + NVL(par.par_re_vmjrotpncob, 0))), 2)
                                                                                   , 'S', ROUND( NVL(par.par_re_valordesconto, 0) - ((NVL(par.par_re_jrotpncobrado, 0) + NVL(par.par_re_vmjrotpncob, 0))), 2)
                                                                                        , ROUND(( NVL(par.par_re_valordesconto, 0) + DECODE((SIGN(NVL(par.par_re_valorjurosren, 0))), -1, NVL((par.par_re_valorjurosren * -1), 0), 0)), 2)) par_re_descontosemantecip
                ,  DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem( par.org_tab_in_codigo
                                                              , par.org_pad_in_codigo
                                                              , par.org_in_codigo
                                                              , par.org_tau_st_codigo
                                                              , par.cto_in_codigo
                                                              , par.par_in_codigo) , 'T', DECODE(v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_jrotpncobrado, 0), 2)), ROUND(NVL(par.par_re_jrotpncobrado, 0), 2))
                                                                                   , 'S', DECODE(v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_jrotpncobrado, 0), 2)), ROUND(NVL(par.par_re_jrotpncobrado, 0), 2))
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

                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo) , 'S', ROUND(NVL((par.par_re_valororiginal - par.par_re_vlroriginalsac), 0), 2)
                                                                                  , 'T', ROUND(NVL(tpb.tpr_re_jurostp, 0), 2)
                                                                                       , 0) tpr_re_jurostp

                , DECODE( mgcar.pck_car_fnc.fnc_car_paiorigem( par.org_tab_in_codigo
                                                             , par.org_pad_in_codigo
                                                             , par.org_in_codigo
                                                             , par.org_tau_st_codigo
                                                             , par.cto_in_codigo
                                                             , par.par_in_codigo) , 'T', DECODE(v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_vmjrotpncob, 0), 2)), ROUND(NVL(par.par_re_vmjrotpncob, 0), 2))
                                                                                  , 'S', DECODE(v_fil_jrotpsaldovo, 'N', DECODE(NVL(tpb.tpr_re_jurostp, 0), 0, 0, ROUND(NVL(par.par_re_vmjrotpncob, 0), 2)), ROUND(NVL(par.par_re_vmjrotpncob, 0), 2))
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
                , DECODE(sign(trunc(SYSDATE) - NVL(par.par_dt_deposito, par.par_dt_baixa)), -1, 'F', DECODE(v_contrl_cheque, 'S', DECODE(par.par_ch_status, 'D', 'I'
                                                                                                                                                          , 'P', 'I'
                                                                                                                                                          , '1', 'I'
                                                                                                                                                          , 'U', 'I'
                                                                                                                                                          , 'N')
                                                                                                                            , 'N')) identificador

                , SUBSTR( mgcar.pck_car_fnc.fnc_car_origemparcela( par.org_tab_in_codigo
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
         FROM mgcar.car_contrato_planilha pla
         WHERE trunc(pla.pla_dt_movimento) <= v_dt_fim
         GROUP BY pla.org_tab_in_codigo
                , pla.org_pad_in_codigo
                , pla.org_in_codigo
                , pla.org_tau_st_codigo
                , pla.cto_in_codigo
                , pla.par_in_codigo )                          cpl
     , mgglo.glo_agentes                                       cta
     , mgcar.car_integra_movimento                             itm
     , mgcar.car_movimento_rateio                              mra
     , ( SELECT mvv.org_tab_in_codigo
              , mvv.org_pad_in_codigo
              , mvv.org_in_codigo
              , mvv.org_tau_st_codigo
              , mvv.cto_in_codigo
              , mvv.par_in_codigo
              , MIN(mvv.mov_in_codigo)	mov_in_codigo
              , MIN(mvv.mra_in_codigo)  mra_in_codigo
         FROM mgcar.car_movimento_parcela mvv
            , mgcar.car_movimento_rateio  maa
            , mgcar.car_integra_movimento imm
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
     , mgcar.car_tipo_termo                                    tpt
     , mgcar.car_contrato_termo                                ter
     , mgfin.fin_hbkcobranca                                   cob
     , mgcar.car_parcela_destino                               des
     , ( SELECT apa.org_tab_in_codigo
              , apa.org_pad_in_codigo
              , apa.org_in_codigo
              , apa.org_tau_st_codigo
              , apa.cto_in_codigo
              , apa.par_in_codigo
              , atp.ant_re_bonificacao
              , atp.ant_in_parcelas
              , atp.ant_re_jrotpncobrado
        FROM mgcar.car_antecipacao         atp
           , mgcar.car_antecipacao_parcela apa
        WHERE NVL(atp.ant_ch_status, 'A') = 'A'
          AND atp.org_tab_in_codigo       = apa.org_tab_in_codigo
          AND atp.org_pad_in_codigo       = apa.org_pad_in_codigo
          AND atp.org_in_codigo           = apa.org_in_codigo
          AND atp.org_tau_st_codigo       = apa.org_tau_st_codigo
          AND atp.cto_in_codigo           = apa.cto_in_codigo
          AND atp.ant_in_codigo           = apa.ant_in_codigo) ant
     , mgcar.car_caucao_parcela                                cau
     , mgcar.car_tabelaprice_baixa                             tpb
     , mgglo.glo_banco                                         ban
     , mgcar.car_parcela_observacao                            obs
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

         FROM mgglo.glo_contasfin            cta
            , mgfin.fin_hbkcontrato          hco
            , mgcar.car_movimento_financeiro mfi
            , mgcar.car_documento_financeiro dfi
            , mgglo.glo_banco                ban
            , mgcar.car_parcela_destino      pde
            , mgcar.car_parcela              par

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
     , mgcar.car_parcela                                 par

  WHERE par.org_tab_in_codigo = v_tab
    AND par.org_pad_in_codigo = v_pad
    AND par.org_in_codigo     = v_cod
    AND par.org_tau_st_codigo = v_tau
    AND par.cto_in_codigo     = v_cto

    AND DECODE( v_por_data_baixa, 'B', par.par_dt_baixa
                                , 'M', par.par_dt_movimento
                                , 'R', par.par_dt_realizacaobx
                                     , par.par_dt_vencimento) >= v_dt_ini

    AND DECODE( v_por_data_baixa, 'B', par.par_dt_baixa
                                , 'M', par.par_dt_movimento
                                , 'R', par.par_dt_realizacaobx
                                     , par.par_dt_vencimento) <= v_dt_fim

    AND ( (   v_cons_cessao = 'S' -- Considero as baixas separando-as por cliente (cessão de direito via botão + cliente atual)
            AND DECODE( v_por_data_baixa, 'B', par.par_dt_baixa
                                        , 'M', par.par_dt_movimento
                                        , 'R', par.par_dt_realizacaobx
                                             , par.par_dt_vencimento) >= v_cto_dt_ini

            AND DECODE( v_por_data_baixa, 'B', par.par_dt_baixa
                                        , 'M', par.par_dt_movimento
                                        , 'R', par.par_dt_realizacaobx
                                             , par.par_dt_vencimento) <= v_cto_dt_fim)
           OR v_cons_cessao = 'N') -- Não considero baixas separando-as por cliente de cessão de direito via botão. Considero todas as baixas para o cliente atual

    AND DECODE( v_por_data_baixa, 'V', NVL( par.par_dt_deposito, par.par_dt_baixa), v_dt_fim) <= v_dt_fim

    AND par.par_dt_baixa IS NOT NULL

    AND(( par.par_ch_status <> 'I') OR ( par.par_ch_status = 'I' AND par.par_dt_status > v_dt_fim) )

    AND par.par_ch_receita      IN ( v_tre_carteira, v_tre_finan, v_tre_fgts, v_tre_bens, v_tre_carta, v_tre_permuta, v_rec_subsidio, v_rec_fin_dir)
    AND par.par_ch_receitabaixa IN ( v_rec_cheque, v_rec_dinheiro, v_rec_bens, v_rec_deposito, v_rec_carta, v_rec_permuta, v_rec_boleto, v_rec_ted, v_rec_doc, v_rec_boleto_av, v_ch_rec_securit, v_rec_repasse)

    AND ((( v_confdivida = 'N') AND (( par.par_bo_confdivida IS NULL) OR ( par.par_bo_confdivida = 'N'))) OR ( v_confdivida = 'S'))

    AND ((( v_parc_caucao  = 'C') AND ( cau.ctc_in_codigo > 0)) OR (( v_parc_caucao  = 'N') AND ( cau.ctc_in_codigo IS NULL)) OR ( v_parc_caucao  = 'T'))

    AND ( ( v_termo LIKE '%C%' AND ( ( par.par_bo_contratual = 'S' AND ter.tte_in_codigo IS NULL)))
       OR ( v_termo LIKE '%F%' AND ( ( par.par_bo_contratual = 'N' AND ter.tte_in_codigo IS NULL)))
       OR ( v_termo LIKE '%T%' AND ter.tte_in_codigo IS NOT NULL
                               AND ( VConsT IS NULL OR VConsT LIKE '%-' || ter.tte_in_codigo || '-%')
                               AND ( VDescT IS NULL OR VDescT NOT LIKE '%-' || ter.tte_in_codigo || '-%')
                               AND ( ( v_termo LIKE '%E%' AND par.par_bo_contratual = 'S') OR ( v_termo LIKE '%N%' AND par.par_bo_contratual = 'N') ) ) )

    AND NVL( cta.agn_in_codigo, 0) = DECODE( v_cta_fin, 0, NVL( cta.agn_in_codigo, 0), v_cta_fin)

    AND (( v_ch_parcela = 'N' AND par.par_ch_parcela <> 'T') OR ( v_ch_parcela = 'S'))

    AND (   ( v_mostra_par_smov = 'N') -- Considera parcelas movimentadas e não movimentadas
         OR ( v_mostra_par_smov = 'S'  -- Considera somente parcelas movimentadas
             AND EXISTS (SELECT 1
                         FROM mgcar.car_movimento_parcela mvv
                            , mgcar.car_movimento_rateio  maa
                            , mgcar.car_integra_movimento imm
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

                           AND imm.imo_bo_integrado IN (v_mov_naointeg, v_mov_integrados)))) -- Opção pelo status do movimento financeiro

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

    AND tpt.tte_in_codigo (+)= ter.tte_in_codigo;'''

querySelect=''' VALUES ( cto.cto_in_codigo                                             -- 1
               , cto.fil_in_codigo                                             -- 6
               , cto.fil_st_nome                                               -- 7
               , cto.emp_in_codigo                                             -- 9
               , cto.emp_st_codigo                                             -- 10
               , cto.emp_st_nome                                               -- 11
               , cto.blo_in_codigo                                             -- 12
               , cto.blo_st_nome                                               -- 14
               , cto.und_st_codigo                                             -- 15
               , cto.ctoenv_ch_origem                                          -- 16
               --, cto.agn_st_codigoalt
               , cto.tipo                                                      -- 17
               , cto.agn_st_nome                                               -- 18
               , par.par_in_codigo                                             -- 19
               , par.par_dt_vencimento                                         -- 20
               , par.par_dt_movimento                                          -- 21
               , par.par_dt_realizacaobx                                       -- 22
               , ( NVL( par.par_re_valororiginal, 0) * v_perc_inv) / 100       -- 24
               , ( NVL( par.par_re_valorpago, 0) * v_perc_inv) / 100           -- 25
               , ( NVL( par.par_re_valortaxas, 0) * v_perc_inv) / 100          -- 26
               , ( NVL( par.par_re_valormulta, 0) * v_perc_inv) / 100          -- 27
               , ( NVL( par.par_re_valoratraso, 0) * v_perc_inv) / 100         -- 28
               , ( NVL( par.vl_corrigido, 0) * v_perc_inv) / 100               -- 32
               , ( NVL( par.vl_juro, 0) * v_perc_inv) / 100                    -- 34
               , ( NVL( par.par_re_valorjurosren, 0) * v_perc_inv) / 100       -- 35
               , ( NVL( par.vl_pago, 0) * v_perc_inv) / 100                    -- 36
               , ( NVL( par.par_re_descontosemantecip, 0) * v_perc_inv) / 100  -- 39
               , ( NVL( par.par_re_descontoantecip, 0) * v_perc_inv) / 100     -- 40
               , ( NVL( par.par_re_residuocobranca, 0) * v_perc_inv) / 100     -- 41
               , par.par_dt_baixa                                              -- 43
               , par.par_bo_confdivida                                         -- 46
               , par.par_st_agencia                                            -- 47
               , par.par_ch_receitabaixa                                       -- 48
               , par.ban_in_numero                                             -- 52
               , par.par_st_conta                                              -- 54
               , ( NVL(par.par_re_credito, 0) * v_perc_inv) / 100              -- 55
               , ( NVL( par.tpr_re_abatido, 0) * v_perc_inv) / 100             -- 57
               , ( NVL( par.tpr_re_jurostp, 0) * v_perc_inv) / 100             -- 58
               , ( NVL( par.ant_re_vmjrotpncob, 0) * v_perc_inv) / 100         -- 59
               , ( NVL( par.par_re_valorcorrecao_atr, 0) * v_perc_inv) / 100   -- 60
               , cto.pro_in_reduzido                                           -- 64
               , cto.pro_st_descricao                                          -- 67
               , par.par_st_observacao                                         -- 73
               , par.tte_in_codigo                                             -- 75
               , par.tte_st_descricao                                          -- 77
               , par.identificador                                             -- 81
               , par.pla_re_jrotp                                              -- 83
               , inv.inv_in_codigo                                             -- 84
               , inv.inv_st_nome                                               -- 85
               , cto.agn_in_codigo                                             -- 86
               );
               '''