PROCEDURE PRC_CAR_CALCULA_ABATIDOTPCTO( :antab number
                                         ,:anpad number
                                         ,:anorg number
                                         ,:astau varchar2
                                         ,:ancto number) is
  /***************************************************************************************************************
   Autor       : Valmir Silva
   Data criacao: 20/11/2008
   Objetivo    : Procedimento para execução do calculo de abatido e
                 juros das parcelas de um contrato
   Alteracoes  : Criado parâmetro nVlrAbatidoTmp para ser utilizada na chamada da prc PRC_DBM_CALCULA_ABATIDO
                 pois foi adicionado nessa prc dois novos parametros. PRP 07/04/2014 pm 77509
                 Incluído tratamento para parcelas securitizadas. Valmir 07/10/2014 PM 143286
                 Adicionado controle para que parcelas pais e filhas fiquem com a mesma posição. PRP 25/05/2016 PINC-3469
                 Inserido na UNION de parcelas renegociadas, para não considerar as parcelas securitizadas. AWR 30/06/2016 PINC-3600
  ****************************************************************************************************************/

    nPaiParcela     number;
    nAuxParcela     number;
    nExiste_BxTp    number;
    nPosicaoParc    number;
    nFilial         number;
    nParcAbertas    number;
    nDifMonths      number;
    nControle       number;
    nCodSerie       number:=0;
    nVlrAbatido     number(22,2):=0;
    nVlrJurosTP     number(22,2):=0;
    nVlrJurosProp   number(22,2):=0;
    nVlrOriginal    number(22,2):=0;
    nVlrOriginalPai number(22,2):=0;
    nVlrTotAbatido  number(22,2):=0;
    nVlrJroTPNCob   number(22,2):=0;
    nVlrAux1        number(22,2):=0;
    nVlrAux2        number(22,2):=0;
    nPercParcela    number(22,8):=0;
    sOrigem         string(1);
    sJroSaldoVO     string(1);
    sUltParcela     string(1);
    sDescProRata    string(1);
    dVencimento     date;
    dDescap         date;
    dAux1           date;
    dAux2           date;
    bDescapDtRef    boolean;


    cursor parcelas is
      select w.*
      from (select par.org_tab_in_codigo
                 , par.org_pad_in_codigo
                 , par.org_in_codigo
                 , par.org_tau_st_codigo
                 , par.cto_in_codigo
                 , par.par_in_codigo
                 , par.par_ch_origem
                 , par.par_dt_vencimento
                 , nvl(par.par_dt_baixa, par.par_dt_vencimento) par_dt_baixa
                 , par.cnd_in_codigo
                 , par.cndit_in_codigo
                 , par.par_re_jrotpncobrado
                 , par.par_re_diftaxa
                 , par.par_re_valororiginal
                 , nvl(par.par_re_valorcorrecao,0) + nvl(par.par_re_valorcorrecaobx,0) vlr_correcao
                 , nvl(par.par_re_valorjuros,0) + nvl(par.par_re_valorjurosbx,0) vlr_juros
                 , ite.cndit_in_parcela nro_parcelas
                 , ite.cndit_re_valorbase
                 , ite.cndit_re_difarredonda
                 , 'N' bo_reneg
                 , par.par_re_valorpago vlr_pago
                 , ite.cndit_re_txtabprice
                 , ite.cndit_dt_referenciatp
                 , decode(par.par_dt_baixa,null,'A','B') par_ch_situacao
                 , decode(par.par_dt_baixa,null,2,1) ordem
                 , par.pai_par_ch_origem Pai_Origem
                 , par.par_ch_status
            from mgcar.car_parcela@life par,
                 mgdbm.dbm_condicao_item@life ite
            where par.org_tab_in_codigo = :antab
              and par.org_pad_in_codigo = :anpad
              and par.org_in_codigo     = :anorg
              and par.org_tau_st_codigo = :astau
              and par.cto_in_codigo     = :ancto
              --and par.par_dt_baixa is not null
              and par.par_ch_status <> 'I'
              and par.par_bo_tabelaprice = 'S'
              and ite.org_tab_in_codigo = par.org_tab_in_codigo
              and ite.org_pad_in_codigo = par.org_pad_in_codigo
              and ite.org_in_codigo     = par.org_in_codigo
              and ite.org_tau_st_codigo = par.org_tau_st_codigo
              and ite.cnd_in_codigo     = par.cnd_in_codigo
              and ite.cndit_in_codigo   = par.cndit_in_codigo

              --Retorna somente parcelas não securitizadas
              and not exists ( select 1
                               from mgcar.car_contabiliza_securitizacao@life s
                               where s.cos_ch_tipooperacao = 'I'
                                 and s.org_tab_in_codigo = par.org_tab_in_codigo
                                 and s.org_pad_in_codigo = par.org_pad_in_codigo
                                 and s.org_in_codigo     = par.org_in_codigo
                                 and s.org_tau_st_codigo = par.org_tau_st_codigo
                                 and s.cto_in_codigo     = par.cto_in_codigo
                                 and s.par_in_codigo     = par.par_in_codigo)

            union all

            --Apenas renegociacao aprovada da parcela
            select par.org_tab_in_codigo
                 , par.org_pad_in_codigo
                 , par.org_in_codigo
                 , par.org_tau_st_codigo
                 , par.cto_in_codigo
                 , par.par_in_codigo
                 , par.par_ch_origem
                 , par.par_dt_vencimento
                 , ren.tre_dt_renegociacao par_dt_baixa
                 , par.cnd_in_codigo
                 , par.cndit_in_codigo
                 , par.par_re_jrotpncobrado
                 , par.par_re_diftaxa
                 , par.par_re_valororiginal
                 , nvl(par.par_re_valorcorrecao,0) + nvl(par.par_re_valorcorrecaobx,0) vlr_correcao
                 , nvl(par.par_re_valorjuros,0) + nvl(par.par_re_valorjurosbx,0) vlr_juros
                 , ite.cndit_in_parcela nro_parcelas
                 , ite.cndit_re_valorbase
                 , ite.cndit_re_difarredonda
                 , 'S' bo_reneg
                 , par.par_re_valororiginal + nvl(par.par_re_valorjuros, 0) + nvl(par.par_re_valorcorrecao, 0) vlr_pago
                 , ite.cndit_re_txtabprice
                 , ite.cndit_dt_referenciatp
                 , 'R' par_ch_situacao
                 , 1 ordem
                 , par.pai_par_ch_origem Pai_Origem
                 , par.par_ch_status
            from mgcar.car_parcela@life              par
               , mgcar.car_tabprice_ren_parcela@life tpr
               , mgcar.car_tabprice_ren@life         ren
               , mgdbm.dbm_condicao_item@life ite
            where par.org_tab_in_codigo   = :antab
              and par.org_pad_in_codigo   = :anpad
              and par.org_in_codigo       = :anorg
              and par.org_tau_st_codigo   = :astau
              and par.cto_in_codigo       = :ancto

              and par.par_bo_tabelaprice  = 'S'
              and ren.tre_bo_status       = 'A'

              and tpr.org_tab_in_codigo   = par.org_tab_in_codigo
              and tpr.org_pad_in_codigo   = par.org_pad_in_codigo
              and tpr.org_in_codigo       = par.org_in_codigo
              and tpr.org_tau_st_codigo   = par.org_tau_st_codigo
              and tpr.cto_in_codigo       = par.cto_in_codigo
              and tpr.par_in_codigo       = par.par_in_codigo

              and ren.org_tab_in_codigo   = tpr.org_tab_in_codigo
              and ren.org_pad_in_codigo   = tpr.org_pad_in_codigo
              and ren.org_in_codigo       = tpr.org_in_codigo
              and ren.org_tau_st_codigo   = tpr.org_tau_st_codigo
              and ren.cto_in_codigo       = tpr.cto_in_codigo
              and ren.cnd_in_codigo       = tpr.cnd_in_codigo
              and ren.cndit_in_codigo     = tpr.cndit_in_codigo
              and ren.tre_in_codigo       = tpr.tre_in_codigo

              and ite.org_tab_in_codigo = par.org_tab_in_codigo
              and ite.org_pad_in_codigo = par.org_pad_in_codigo
              and ite.org_in_codigo     = par.org_in_codigo
              and ite.org_tau_st_codigo = par.org_tau_st_codigo
              and ite.cnd_in_codigo     = par.cnd_in_codigo
              and ite.cndit_in_codigo   = par.cndit_in_codigo

              --Retorna somente parcelas não securitizadas. PINC-3600
              and not exists ( select 1
                               from mgcar.car_contabiliza_securitizacao@life s
                               where s.cos_ch_tipooperacao = 'I'
                                 and s.org_tab_in_codigo = par.org_tab_in_codigo
                                 and s.org_pad_in_codigo = par.org_pad_in_codigo
                                 and s.org_in_codigo     = par.org_in_codigo
                                 and s.org_tau_st_codigo = par.org_tau_st_codigo
                                 and s.cto_in_codigo     = par.cto_in_codigo
                                 and s.par_in_codigo     = par.par_in_codigo)

            union all
            -- Incluso select para retornar parcelas Securitizadas. Valmir 07/10/2014 PM 143286
            select par.org_tab_in_codigo
                 , par.org_pad_in_codigo
                 , par.org_in_codigo
                 , par.org_tau_st_codigo
                 , par.cto_in_codigo
                 , par.par_in_codigo
                 , par.par_ch_origem
                 , par.par_dt_vencimento
                 , cse.sec_dt_basecalc par_dt_baixa
                 , par.cnd_in_codigo
                 , par.cndit_in_codigo
                 , par.par_re_jrotpncobrado
                 , par.par_re_diftaxa
                 , par.par_re_valororiginal
                 , nvl(par.par_re_valorcorrecao,0) + nvl(par.par_re_valorcorrecaobx,0) vlr_correcao
                 , nvl(par.par_re_valorjuros,0) + nvl(par.par_re_valorjurosbx,0) vlr_juros
                 , ite.cndit_in_parcela nro_parcelas
                 , ite.cndit_re_valorbase
                 , ite.cndit_re_difarredonda
                 , 'N' bo_reneg
                 , par.par_re_valororiginal + nvl(par.par_re_valorjuros, 0) + nvl(par.par_re_valorcorrecao, 0) vlr_pago
                 , ite.cndit_re_txtabprice
                 , ite.cndit_dt_referenciatp
                 , 'S' par_ch_situacao
                 , 1 ordem
                 , par.pai_par_ch_origem Pai_Origem
                 , par.par_ch_status
            from mgcar.car_contabiliza_securitizacao@life cse
               , mgcar.car_parcela@life                   par
               , mgdbm.dbm_condicao_item@life             ite
            where par.org_tab_in_codigo  = :antab
              and par.org_pad_in_codigo  = :anpad
              and par.org_in_codigo      = :anorg
              and par.org_tau_st_codigo  = :astau
              and par.cto_in_codigo      = :ancto
              and par.par_bo_tabelaprice = 'S'

              and ite.org_tab_in_codigo = par.org_tab_in_codigo
              and ite.org_pad_in_codigo = par.org_pad_in_codigo
              and ite.org_in_codigo     = par.org_in_codigo
              and ite.org_tau_st_codigo = par.org_tau_st_codigo
              and ite.cnd_in_codigo     = par.cnd_in_codigo
              and ite.cndit_in_codigo   = par.cndit_in_codigo

              and cse.org_tab_in_codigo   = par.org_tab_in_codigo
              and cse.org_pad_in_codigo   = par.org_pad_in_codigo
              and cse.org_in_codigo       = par.org_in_codigo
              and cse.org_tau_st_codigo   = par.org_tau_st_codigo
              and cse.cto_in_codigo       = par.cto_in_codigo
              and cse.par_in_codigo       = par.par_in_codigo
              and cse.cos_in_codigo       = ( select min(c.cos_in_codigo)
                                              from mgcar.car_contabiliza_securitizacao@life c
                                              where c.org_tab_in_codigo = par.org_tab_in_codigo
                                                and c.org_pad_in_codigo = par.org_pad_in_codigo
                                                and c.org_in_codigo     = par.org_in_codigo
                                                and c.org_tau_st_codigo = par.org_tau_st_codigo
                                                and c.cto_in_codigo     = par.cto_in_codigo
                                                and c.par_in_codigo     = par.par_in_codigo
                                                and c.cos_ch_tipooperacao = 'I')) w
      order by cto_in_codigo, cndit_in_codigo, ordem, par_dt_baixa, par_dt_vencimento desc, par_ch_origem, par_in_codigo;

  begin

    delete mgcar.car_tabelaprice_baixa
    where org_tab_in_codigo = :antab
      and org_pad_in_codigo = :anpad
      and org_in_codigo     = :anorg
      and org_tau_st_codigo = :astau
      and cto_in_codigo     = :ancto;

    commit;

    --Seleciona a filial
    select cto.fil_in_codigo, nvl(cto.cto_bo_descprorata, 'N')
    into nFilial, sDescProRata
    from mgcar.car_contrato@life cto
    where cto.org_tab_in_codigo = :antab
      and cto.org_pad_in_codigo = :anpad
      and cto.org_in_codigo     = :anorg
      and cto.org_tau_st_codigo = :astau
      and cto.cto_in_codigo     = :ancto;

    begin
      --Seleciona os parametros referentes a juros de tp
      select nvl(prm.pco_bo_jrotpsaldovo, 'N')
      into sJroSaldoVO
      from mgdbm.dbm_parametro_contabilidade@life prm
      where prm.org_tab_in_codigo = :antab
        and prm.org_pad_in_codigo = :anpad
        and prm.org_in_codigo     = nFilial
        and prm.org_tau_st_codigo = :astau;

      exception
        when no_data_found then
          sJroSaldoVO := 'N';
    end;

    for npar in parcelas
    loop

      --Para cada série, verifica se existe registro na dbm_condicao_item_abat, caso não tenha chama a prc_dbm_calcula_abatido
      if nCodSerie <> npar.cndit_in_codigo then
        nCodSerie := npar.cndit_in_codigo;

        select count(a.cnd_in_codigo)
        into   nControle
        from   mgdbm.dbm_condicao_item_abat@life a
        where  a.org_tab_in_codigo = :antab
          and  a.org_pad_in_codigo = :anpad
          and  a.org_in_codigo     = :anorg
          and  a.org_tau_st_codigo = :astau
          and  a.cnd_in_codigo     = npar.cnd_in_codigo
          and  a.cndit_in_codigo   = npar.cndit_in_codigo;

        if nControle = 0 then
          --Adicionado aos parâmetros a variável nVlrAbatido para não dar erro, pois essa procedure foi alterada inserindo dois novos parâmetros. PRP 07/04/2014 77509
          mgdbm.prc_dbm_calcula_abatido@life(:antab,:anpad,:anorg,:astau,npar.cnd_in_codigo,npar.cndit_in_codigo,nVlrAbatido);
        end if;
      end if;

      --Somente se a parcela já estiver com a TP aplicada
      if mgcar.pck_car_fnc.fnc_car_paiorigem@life(anTab, anPad, anOrg, asTau, anCto, npar.par_in_codigo) = 'T'
      then

        --Calcula a posicao da parcela na série, conforme ordem de pagamento
        select nvl(max(t.tpr_in_posicaoparc),0) + 1
        into   nPosicaoParc
        from   mgcar.car_tabelaprice_baixa@life t
        where  t.org_tab_in_codigo = :antab
           and t.org_pad_in_codigo = :anpad
           and t.org_in_codigo     = :anorg
           and t.org_tau_st_codigo = :astau
           and t.cto_in_codigo     = :ancto
           and t.cnd_in_codigo     = npar.cnd_in_codigo
           and t.cndit_in_codigo   = npar.cndit_in_codigo;

        --Se parcela inativa tem filha na tabela car_tabelaprice_baixa pega posição da filha PRP 25/05/2016 PINC-3469
        if (npar.par_ch_status = 'I') then
          nControle := 0;
          select count(*)
            into nControle
            from mgcar.car_tabelaprice_baixa@life s
           where s.org_tab_in_codigo = :antab
             and s.org_pad_in_codigo = :anpad
             and s.org_in_codigo     = :anorg
             and s.org_tau_st_codigo = :astau
             and s.cto_in_codigo     = :ancto
             and s.pai_par_in_codigo = npar.par_in_codigo;

          if nControle > 0 then
            select t.tpr_in_posicaoparc
              into nPosicaoParc
              from mgcar.car_tabelaprice_baixa@life t
             where (t.org_tab_in_codigo
                  , t.org_pad_in_codigo
                  , t.org_in_codigo
                  , t.org_tau_st_codigo
                  , t.cto_in_codigo
                  , t.par_in_codigo) = (select s.org_tab_in_codigo
                                             , s.org_pad_in_codigo
                                             , s.org_in_codigo
                                             , s.org_tau_st_codigo
                                             , s.cto_in_codigo
                                             , s.par_in_codigo
                                          from mgcar.car_tabelaprice_baixa@life s
                                         where s.org_tab_in_codigo = :antab
                                           and s.org_pad_in_codigo = :anpad
                                           and s.org_in_codigo     = :anorg
                                           and s.org_tau_st_codigo = :astau
                                           and s.cto_in_codigo     = :ancto
                                           and s.pai_par_in_codigo = npar.par_in_codigo
                                           and rownum = 1);
           end if;
        end if;--Fim PINC-3469

        --Verifica se é a última parcela paga/renegociada da série
        sUltParcela := 'N';
        if nPosicaoParc = npar.nro_parcelas then
          sUltParcela := 'S';
        end if;

        nPaiParcela := null;
        sOrigem := npar.par_ch_origem;
        nAuxParcela := npar.par_in_codigo;
        nVlrOriginal := npar.par_re_valororiginal;
        nVlrJroTPNCob := npar.par_re_jrotpncobrado;
        nPercParcela := 1;

        --Se for parcela renegociada ou securitizada
        if (npar.bo_reneg = 'S') or (npar.par_ch_situacao = 'S') then
          nVlrJroTPNCob := 0;
          if npar.par_dt_vencimento > npar.par_dt_baixa then
            if npar.par_dt_baixa < npar.cndit_dt_referenciatp then
              dDescap := trunc(npar.cndit_dt_referenciatp);
              bDescapDtRef := true;
            else
              dDescap:= npar.par_dt_baixa;
            end if;

            if (sDescProRata = 'N') or (bDescapDtRef) then
              dAux1 := to_date(('01/' || to_char(dDescap, 'mm/yyyy')), 'dd/mm/yyyy');
              dAux2 := to_date(('01/' || to_char(npar.par_dt_vencimento,'mm/yyyy')),'dd/mm/yyyy');
              nDifMonths := mgdbm.fnc_dbm_difdias_comercial@life(dAux2, dAux1);
              nDifMonths := trunc(nDifMonths);
            else
              nDifMonths := mgdbm.fnc_dbm_difdias_comercial@life(npar.par_dt_vencimento, dDescap);
            end if;

            nVlrJroTPNCob := npar.par_re_valororiginal - (npar.par_re_valororiginal / (power(1 + (npar.cndit_re_txtabprice / 100), nDifMonths)));
          end if;
        end if;

        --Se for baixa parcial, verifica se já possui alguma parcela irmã calculada
        if sOrigem = 'B' then
          while sOrigem = 'B' loop
            select min(par.par_in_codigo)
                 , min(par.pai_par_in_codigo)
                 , trim(min(par.par_ch_origem))
                 , min(par.par_re_valororiginal)
            into nPaiParcela
               , nAuxParcela
               , sOrigem
               , nVlrOriginalPai
            from mgcar.car_parcela@life par
            where par.org_tab_in_codigo = :antab
              and par.org_pad_in_codigo = :anpad
              and par.org_in_codigo     = :anorg
              and par.org_tau_st_codigo = :astau
              and par.cto_in_codigo     = :ancto
              and par.par_in_codigo     = nAuxParcela;
          end loop;

          select count(tpb.par_in_codigo)
          into nExiste_BxTp
          from mgcar.car_tabelaprice_baixa@life tpb
          where tpb.org_tab_in_codigo = :antab
            and tpb.org_pad_in_codigo = :anpad
            and tpb.org_in_codigo     = :anorg
            and tpb.org_tau_st_codigo = :astau
            and tpb.cto_in_codigo     = :ancto
            and tpb.pai_par_in_codigo = nPaiParcela
            and tpb.par_in_codigo    <> npar.par_in_codigo;

          if nExiste_BxTp > 0 then
            select tpb.tpr_in_posicaoparc
            into nPosicaoParc
            from mgcar.car_tabelaprice_baixa@life tpb
            where tpb.org_tab_in_codigo = :antab
              and tpb.org_pad_in_codigo = :anpad
              and tpb.org_in_codigo     = :anorg
              and tpb.org_tau_st_codigo = :astau
              and tpb.cto_in_codigo     = :ancto
              and tpb.pai_par_in_codigo = nPaiParcela
              and tpb.par_dt_vencimento is not null
              and tpb.par_in_codigo    <> npar.par_in_codigo
              and rownum = 1 ;
          end if;

          nPercParcela := nVlrOriginal / nVlrOriginalPai;

          sUltParcela := 'N';
          if nPosicaoParc = npar.nro_parcelas then
            --Procura por parcelas irmãs que ainda não estejam na tabelaprice_baixa
            select count(p.par_in_codigo)
            into   nParcAbertas
            from   mgcar.car_parcela@life p
            where  p.org_tab_in_codigo = :antab
              and  p.org_pad_in_codigo = :anpad
              and  p.org_in_codigo     = :anorg
              and  p.org_tau_st_codigo = :astau
              and  p.cto_in_codigo     = :ancto
              and  p.cnd_in_codigo     = npar.cnd_in_codigo
              and  p.cndit_in_codigo   = npar.cndit_in_codigo
              and  p.par_in_codigo    <> npar.par_in_codigo
              and  trunc(p.par_dt_vencimento,'month') = trunc(npar.par_dt_vencimento,'month')
              and  not exists (select 1
                               from   mgcar.car_tabelaprice_baixa@life t
                               where  t.org_tab_in_codigo = p.org_tab_in_codigo
                                 and  t.org_pad_in_codigo = p.org_pad_in_codigo
                                 and  t.org_in_codigo     = p.org_in_codigo
                                 and  t.org_tau_st_codigo = p.org_tau_st_codigo
                                 and  t.cto_in_codigo     = p.cto_in_codigo
                                 and  t.par_in_codigo     = p.par_in_codigo)
              and  not exists (select 1
                               from   mgcar.car_parcela@life l
                               where  l.org_tab_in_codigo = p.org_tab_in_codigo
                                 and  l.org_pad_in_codigo = p.org_pad_in_codigo
                                 and  l.org_in_codigo     = p.org_in_codigo
                                 and  l.org_tau_st_codigo = p.org_tau_st_codigo
                                 and  l.cto_in_codigo     = p.cto_in_codigo
                                 and  l.pai_par_in_codigo = p.par_in_codigo);

              if nParcAbertas = 0 then
                sUltParcela := 'S';
              end if;
            end if;
        end if;

        select nvl(a.cndita_re_jurostp,0), nvl(a.cndita_re_jroantestp,0), a.cndita_dt_vencimento
        into   nVlrJurosTP, nVlrJurosProp, dVencimento
        from   mgdbm.dbm_condicao_item_abat@life a
        where  a.org_tab_in_codigo = :antab
          and  a.org_pad_in_codigo = :anpad
          and  a.org_in_codigo     = :anorg
          and  a.org_tau_st_codigo = :astau
          and  a.cnd_in_codigo     = npar.cnd_in_codigo
          and  a.cndit_in_codigo   = npar.cndit_in_codigo
          and  a.cndita_in_parcela = nPosicaoParc;

        if sJroSaldoVO = 'N' then
          nVlrJurosTP   := nVlrJurosTP * nPercParcela;
          nVlrAbatido   := NVL(nVlrOriginal, 0) - NVL(nVlrJurosTP, 0);
          nVlrJurosProp := nVlrJurosProp * nPercParcela;
        else
          nVlrJurosTP  := 0;
          nVlrAbatido  := nVlrOriginal;
          nPercParcela := 0;
        end if;

        if sUltParcela = 'S' AND sJroSaldoVO = 'N' then
          select nvl(sum(tpb.tpr_re_abatido),0)
          into nVlrTotAbatido
          from mgcar.car_tabelaprice_baixa@life tpb
          where tpb.org_tab_in_codigo = :antab
            and tpb.org_pad_in_codigo = :anpad
            and tpb.org_in_codigo     = :anorg
            and tpb.org_tau_st_codigo = :astau
            and tpb.cto_in_codigo     = :ancto
            and tpb.cnd_in_codigo     = npar.cnd_in_codigo
            and tpb.cndit_in_codigo   = npar.cndit_in_codigo
            and tpb.par_in_codigo    <> npar.par_in_codigo;

          if nvl(npar.cndit_re_valorbase, 0) <> nvl(nVlrTotAbatido, 0) + nvl(nVlrAbatido, 0) then
            nVlrAux1 := nvl(npar.cndit_re_valorbase, 0) - nvl(nVlrTotAbatido, 0);
            nVlrAux2 := nvl(nVlrAux1, 0) - nvl(nVlrAbatido, 0);
            if nVlrAux2 <= 3 and nVlrAux2 >= -3 then --Aumentei a  diferença para 3 e -3. PRP 07/05/2012 PM 336310
              nVlrAbatido := nVlrAbatido + nVlrAux2;
              nVlrJurosTP := nVlrJurosTP - nVlrAux2;
            else
              nVlrAbatido := nVlrAbatido + nvl(npar.cndit_re_difarredonda, 0);
            end if;
          end if;
        end if;

        insert into mgcar.car_tabelaprice_baixa
          (  org_tab_in_codigo   , org_pad_in_codigo   , org_in_codigo      , org_tau_st_codigo
           , cnd_in_codigo       , cndit_in_codigo     , cto_in_codigo      , par_in_codigo
           , tpr_dt_baixa        , tpr_re_correcao     , tpr_re_juros       , tpr_re_abatido
           , tpr_re_jurostp      , par_dt_vencimento   , tpr_re_diftaxa     , tpr_re_jrotpncobrado
           , tpr_re_jro_antestp  , tpr_re_difsaldo     , pai_par_in_codigo  , tpr_bo_ultparcela
           , tpr_re_valororiginal, tpr_re_tpincorporado, tpr_in_posicaoparc , tpr_ch_situacao)
        values
          (  :antab               , :anpad               , :anorg              , :astau
           , npar.cnd_in_codigo  , npar.cndit_in_codigo, :ancto              , npar.par_in_codigo
           , npar.par_dt_baixa   , npar.vlr_correcao   , npar.vlr_juros     , nVlrAbatido
           , nVlrJurosTP         , dVencimento         , npar.par_re_diftaxa, nVlrJroTPNCob
           , nVlrJurosProp       , 0                   , nvl(nPaiParcela, npar.par_in_codigo), sUltParcela
           , nVlrAbatido         , nVlrJurosTP         , nPosicaoParc       , npar.par_ch_situacao);

      end if;
    end loop;
  end PRC_CAR_CALCULA_ABATIDOTPCTO;