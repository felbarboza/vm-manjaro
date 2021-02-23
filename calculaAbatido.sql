CREATE OR REPLACE PROCEDURE MGDBM."PRC_DBM_CALCULA_ABATIDO" (aTab number
                                                        , aPad number
                                                        , aOrg number
                                                        , aTau string
                                                        , aCnd number
                                                        , aCndIt number
                                                        , aVlrAbatido OUT NUMBER
                                                        , aDtParVenc date default null) is

  iIntervalo      integer;
  iNroParcela     integer;
  iAux            integer;
  iAntesTP        integer;
  sDia            string(2);
  dRefTP          date;
  dVctoParc       date;
  dVencimento     date;
  nTxTP           number;
  nVlrAux         number;
  nVlrBase        number;
  nVlrFuturo      number;
  nDifArred       number;
  nSaldoSerie     number;
  nSaldoCalc      number;
  nJurosTP        number;
  nAbatido        number;
  nJurosAntesTot  number;
  nJurosAntesParc number;
  nJurosAntesPago number;
  
  nJurosNaoPago   number; --POPF-1093

begin

    select c.cndit_in_intervalo, c.cndit_in_parcela, c.cndit_re_valorbase, c.cndit_dt_vencimento,
           c.cndit_re_txtabprice, c.cndit_dt_referenciatp, c.cndit_re_valorfuturo, nvl(c.cndit_re_difarredonda,0)
    into   iIntervalo, iNroParcela, nVlrBase, dVencimento,
           nTxTP, dRefTP, nVlrFuturo, nDifArred
    from   mgdbm.dbm_condicao_item c
    where  c.org_tab_in_codigo = aTab and
           c.org_pad_in_codigo = aPad and
           c.org_in_codigo     = aOrg and
           c.org_tau_st_codigo = aTau and
           c.cnd_in_codigo     = aCnd and
           c.cndit_in_codigo   = aCndIt;

    --Só dá o delete se o processo NÃO for chamado pela pck_car_fnc.prc_car_busca_amortizacaotp. PRP 07/04/2014 pm 77509
    if aDtParVenc is null then
      delete from mgdbm.dbm_condicao_item_abat a
      where  a.org_tab_in_codigo = aTab and
             a.org_pad_in_codigo = aPad and
             a.org_in_codigo     = aOrg and
             a.org_tau_st_codigo = aTau and
             a.cnd_in_codigo     = aCnd and
             a.cndit_in_codigo   = aCndIt;
    end if;

    sDia := to_char(dVencimento,'DD');

    nSaldoSerie := nVlrBase;

    --Calcula o valor de juros antes da TP
    iAntesTP :=  trunc(months_between(trunc(dVencimento, 'MONTH'), trunc(dRefTP, 'MONTH'))) - iIntervalo;
    if (iAntesTP > 0) and (iNroParcela > 1) then
      nJurosAntesTot := round(NVL(nVlrBase, 0) * (((nTxTP / 100 + 1) ** iAntesTP) - 1), 2);
    else
      nJurosAntesTot := 0;
    end if;
    nJurosAntesParc := round(nJurosAntesTot/iNroParcela, 2);

    for iParc in 1 ..iNroParcela loop

      nJurosAntesPago := nJurosAntesParc * (iParc - 1);
      nSaldoCalc := nSaldoSerie + (nJurosAntesTot - nJurosAntesPago) + nvl(nJurosNaoPago,0);

      dVctoParc := mgdbm.fnc_dbm_datavalida(sDia || '/' || to_char(add_months(dVencimento, (iParc-1)*iIntervalo), 'MM/YYYY'));

      if (iParc = 1) or (iNroParcela = 1) THEN
        iAux := mgdbm.fnc_dbm_difdias_comercial(TRUNC(dRefTP, 'MONTH'), TRUNC(dVencimento, 'MONTH'));
        if (iAux < iIntervalo) OR (iNroParcela = 1) THEN
          nJurosTP := round((nSaldoCalc * (nTxTP/100 + 1) ** iAux) - nSaldoCalc, 2);
        else
          nJurosTP  := round((nSaldoCalc * (nTxTP/100 + 1) ** iIntervalo) - nSaldoCalc, 2);
        end if;
      else
        nJurosTP  := round((nSaldoCalc * (nTxTP/100 + 1) ** iIntervalo) - nSaldoCalc, 2);
      end if;

      nJurosTP := nJurosTP + nJurosAntesParc;
      
      nJurosNaoPago := 0;
      if nJurosTP > nVlrFuturo then
        nJurosNaoPago := nJurosTP - nVlrFuturo;
        nJurosTP := nVlrFuturo;
      end if;
      
      nAbatido := nVlrFuturo - nJurosTP;
      if nAbatido < 0 then
        nAbatido := 0;
      end if;

      --Se for a ultima parcela, faz alguns ajustes
      if iParc = iNroParcela then
        --nAbatido := nAbatido + nDifArred;
        nVlrAux  := nSaldoSerie - nAbatido;
        if (nVlrAux <= 2 and nVlrAux >= -2) or (nVlrAux <= nJurosAntesParc and nVlrAux >= (-1*nJurosAntesParc)) then
          nAbatido := nAbatido + nVlrAux;
          nJurosTP := nJurosTP - nVlrAux;
        end if;
      end if;
      nSaldoSerie := nSaldoSerie - nAbatido;

      --Caso tenha encontrado o vencimento da parcela enviado por parâmetro retorna o respectivo valor abatido. PRP 77509
      if aDtParVenc is NOT null then
        if trunc(aDtParVenc, 'MONTH') = trunc(dVctoParc, 'MONTH') then
           aVlrAbatido := nAbatido;
           return;
        end if;
      end if;

      --Só dá o insert se o processo NÃO for chamado pela pck_car_fnc.prc_car_busca_amortizacaotp. PRP 07/04/2014 pm 77509
      if aDtParVenc is null then
        insert into mgdbm.dbm_condicao_item_abat
         (org_tab_in_codigo, org_pad_in_codigo, org_in_codigo       , org_tau_st_codigo,
          cnd_in_codigo    , cndit_in_codigo  , cndita_in_parcela   , cndita_dt_vencimento,
          cndita_re_abatido, cndita_re_jurostp, cndita_re_jroantestp, cndita_re_saldodev)
        values
         (aTab             , aPad             , aOrg                , aTau,
          aCnd             , aCndIt           , iParc               , dVctoParc,
          nAbatido         , nJurosTP         , nJurosAntesParc     , nSaldoSerie);
      end if;
    end loop;

end PRC_DBM_CALCULA_ABATIDO;
