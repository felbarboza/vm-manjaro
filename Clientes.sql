SELECT  EST.ORG_TAB_IN_CODIGO 
      , EST.ORG_PAD_IN_CODIGO 
      , EST.ORG_IN_CODIGO 
      , EST.ORG_TAU_ST_CODIGO 
      , EST.EMP_IN_CODIGO 
      , EST.EMP_ST_CODIGO 
      , EST.EMP_ST_NOME 
      , EST.ETP_IN_CODIGO 
      , EST.ETP_ST_CODIGO 
      , EST.ETP_ST_NOME 
      , EST.BLO_IN_CODIGO 
      , EST.BLO_ST_CODIGO 
      , EST.BLO_ST_NOME 
      , EST.UND_IN_CODIGO 
      , EST.UND_ST_CODIGO 
      , EST.UND_ST_NOME 
      , NVL( EST.UND_RE_AREAPRIVATIVA, 0) UND_RE_AREAPRIVATIVA 
      , CTO.CTO_IN_CODIGO 
      , DECODE(:B28 , 'N', ROUND(NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO (    CTO.ORG_TAB_IN_CODIGO 
                                                                                  , CTO.ORG_PAD_IN_CODIGO 
                                                                                  , CTO.ORG_IN_CODIGO 
                                                                                  , CTO.ORG_TAU_ST_CODIGO 
                                                                                  , CTO.CTO_IN_CODIGO 
                                                                                  , 'D' 
                                                                                  , :B18 
                                                                                  , '' 
                                                                                  , '')
                                      , 0)
                                , 2) 
                        , ROUND( NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO (    CTO.ORG_TAB_IN_CODIGO 
                                                                                  , CTO.ORG_PAD_IN_CODIGO 
                                                                                  , CTO.ORG_IN_CODIGO 
                                                                                  , CTO.ORG_TAU_ST_CODIGO 
                                                                                  , CTO.CTO_IN_CODIGO 
                                                                                  , 'D' 
                                                                                  , :B18 
                                                                                  , '' 
                                                                                  , '')
                                  , 0) * ( NVL( MGREL.PCK_REL_FNC.FNC_CAR_BUSCA_PERC_PART_DIST( CTO.ORG_TAB_IN_CODIGO 
                                                                                              , CTO.ORG_PAD_IN_CODIGO 
                                                                                              , CTO.ORG_IN_CODIGO 
                                                                                              , CTO.ORG_TAU_ST_CODIGO 
                                                                                              , DECODE( NVL( EST.UND_BO_CONSINVESTIDOR, 'N')
                                                                                                        , 'S', EST.UND_IN_CODIGO
                                                                                                        , EST.BLO_IN_CODIGO) 
                                                                                              , 0  
                                                                                              , CTO.ORG_TAB_IN_CODIGO 
                                                                                              , CTO.ORG_PAD_IN_CODIGO 
                                                                                              , CTO.ORG_IN_CODIGO 
                                                                                              , CTO.ORG_TAU_ST_CODIGO 
                                                                                              , CTO.CTO_IN_CODIGO 
                                                                                              , NULL 
                                                                                              , 'C')
                                            , 0)/100)
                          , 2)) CTO_RE_VALORCONTRATO , 
      DECODE(:B28 , 'N', ROUND( NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO ( CTO.ORG_TAB_IN_CODIGO 
                                                                              , CTO.ORG_PAD_IN_CODIGO 
                                                                              , CTO.ORG_IN_CODIGO 
                                                                              , CTO.ORG_TAU_ST_CODIGO 
                                                                              , CTO.CTO_IN_CODIGO 
                                                                              , :B27 
                                                                              , :B18 
                                                                              , '' 
                                                                              , '')
                                , 0)
                          , 2) 
                    , ROUND( NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO (  CTO.ORG_TAB_IN_CODIGO 
                                                                            , CTO.ORG_PAD_IN_CODIGO 
                                                                            , CTO.ORG_IN_CODIGO 
                                                                            , CTO.ORG_TAU_ST_CODIGO 
                                                                            , CTO.CTO_IN_CODIGO 
                                                                            , :B27 
                                                                            , :B18 
                                                                            , '' 
                                                                            , '')
                                , 0)* ( NVL( MGREL.PCK_REL_FNC.FNC_CAR_BUSCA_PERC_PART_DIST(  CTO.ORG_TAB_IN_CODIGO 
                                                                                            , CTO.ORG_PAD_IN_CODIGO 
                                                                                            , CTO.ORG_IN_CODIGO 
                                                                                            , CTO.ORG_TAU_ST_CODIGO 
                                                                                            , DECODE( NVL( EST.UND_BO_CONSINVESTIDOR, 'N')
                                                                                                      , 'S', EST.UND_IN_CODIGO
                                                                                                      , EST.BLO_IN_CODIGO) 
                                                                                            , 0 
                                                                                            , CTO.ORG_TAB_IN_CODIGO 
                                                                                            , CTO.ORG_PAD_IN_CODIGO 
                                                                                            , CTO.ORG_IN_CODIGO 
                                                                                            , CTO.ORG_TAU_ST_CODIGO 
                                                                                            , CTO.CTO_IN_CODIGO 
                                                                                            , NULL 
                                                                                            , 'C')
                                        , 0)/100)
      , 2)) CTO_RE_VALORVENDA 
      , CTO.CTO_CH_TIPO 
      , CTO.CTO_DT_CADASTRO 
      , MGREL.PCK_REL_FNC.FNC_CAR_STATUS_CTO_DATA(  CTO.ORG_TAB_IN_CODIGO 
                                                  , CTO.ORG_PAD_IN_CODIGO 
                                                  , CTO.ORG_IN_CODIGO 
                                                  , CTO.ORG_TAU_ST_CODIGO 
                                                  , CTO.CTO_IN_CODIGO 
                                                  , :B18 
                                                  , 'N') CTO_CH_STATUS 
      , CTO.CTO_DT_STATUS 
      , CTO.CTO_ST_OBSERVACAO 
      , EST.CTOENV_CH_ORIGEM 
      , AGN.AGN_IN_CODIGO 
      , AGN.AGN_ST_NOME 
      , AGN.AGN_CH_TIPOPESSOAFJ 
      , AGN.AGN_ST_CGC 
      , AGN.UF_ST_SIGLA 
      , AGN.AGN_ST_MUNICIPIO 
      , AGN.TPL_ST_SIGLA 
      , AGN.AGN_ST_LOGRADOURO 
      , AGN.AGN_ST_NUMERO 
      , AGN.AGN_ST_BAIRRO 
      , AGN.AGN_ST_CEP 
      , AGN.AGN_ST_EMAIL 
      , MGREL.PCK_REL_GLO_FNC.FNC_GLO_CONCATENA_TELAGENTE(  AGN.AGN_TAB_IN_CODIGO 
                                                          , AGN.AGN_PAD_IN_CODIGO 
                                                          , AGN.AGN_IN_CODIGO 
                                                          , ' ' 
                                                          , 'S' 
                                                          , NULL 
                                                          , 99) TEL_AGENTE 
      , AGN.AGN_ST_COMPLEMENTO 
      , PEF.AGN_CH_TIPO 
      , PEF.AGN_ST_CPF 
      , PEF.AGN_ST_RG 
      , PEF.AGN_ST_CARGOPROFISS 
      , PEF.AGN_ST_NACIONALIDADE 
      , PEF.AGN_CH_ESTCIVIL 
      , PEF.AGN_CH_REGIMECASAMENTO 
      , PEF.AGN_DT_NASCIMENTO 
      , PEF.AGN_ST_CONJUGE NOME_CONJUGE 
      , PFC.AGN_ST_CPF CPF_CONJUGE 
      , PFC.AGN_ST_RG RG_CONJUGE 
      , PFC.AGN_ST_NACIONALIDADE NAC_CONJUGE 
      , PFC.AGN_ST_CARGOPROFISS PROF_CONJUGE 
      , CLA.CSF_IN_CODIGO 
      , CLA.CSF_ST_DESCRICAO 
      , TRUNC( MONTHS_BETWEEN( ( NVL( PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO)), CTO.CTO_DT_CADASTRO), 0) PRAZO_PAGAMENTO 
      , TPU.TPU_IN_CODIGO 
      , TPU.TPU_ST_ESTENDIDA 
      , TPU.TPU_ST_DESCRICAO 
      FROM  MGCAR.CAR_PARCELA PAR 
          , MGGLO.GLO_PESSOA_FISICA PFC 
          , MGGLO.GLO_PESSOA_FISICA PEF 
          , MGDBM.DBM_TIPOLOGIA_UNIDADE TPU 
          , MGDBM.DBM_UNIDADE UND 
          , MGGLO.GLO_AGENTES AGN 
          , MGGLO.GLO_AGENTES_ID AID 
          , MGDBM.DBM_CLASSIFICACAO CLA 
          , MGCAR.CAR_CONTRATO_CLIENTE CTC 
          , MGREL.VW_CAR_ESTRUTURA EST 
          , (SELECT   FIA.FIL_TAB_IN_CODIGO TAB_IN_CODIGO 
                    , FIA.FIL_PAD_IN_CODIGO PAD_IN_CODIGO 
                    , DECODE(AGN.AGN_BO_CONSOLIDADOR
                              , 'E', AGN.AGN_IN_CODIGO
                              , AGN.PAI_AGN_IN_CODIGO) ORG_IN_CODIGO 
                    , AGN.AGN_IN_CODIGO FIL_IN_CODIGO 
              FROM  MGGLO.GLO_FILIAL_ATIVA FIA 
                  , MGGLO.GLO_AGENTES AGN 
              WHERE 
                    FIA.USU_IN_CODIGO = :B2 
                AND FIA.COMP_ST_NOME = :B1
                AND AGN.AGN_TAB_IN_CODIGO = FIA.FIL_TAB_IN_CODIGO 
                AND AGN.AGN_PAD_IN_CODIGO = FIA.FIL_PAD_IN_CODIGO 
                AND AGN.AGN_IN_CODIGO = FIA.FIL_IN_CODIGO 
                AND AGN.AGN_BO_CONSOLIDADOR IN ('E', 'F')) ATI 
            , MGCAR.CAR_CONTRATO CTO 
            
      WHERE CTO.ORG_TAB_IN_CODIGO = :B26 
        AND CTO.ORG_PAD_IN_CODIGO = :B25 
        AND CTO.ORG_TAU_ST_CODIGO = :B24 
        AND NVL( EST.EMP_IN_CODIGO,0) = DECODE( :B23 , 0, NVL( EST.EMP_IN_CODIGO,0), :B23 ) 
        AND CTO.CSF_IN_CODIGO = DECODE( :B22 , 0, CTO.CSF_IN_CODIGO, :B22 ) 
        AND CTO.CTO_CH_TIPO IN ( :B21 , :B20 , :B19 ) 
        AND ( MGREL.PCK_REL_FNC.FNC_CAR_STATUS_CTO_DATA(  CTO.ORG_TAB_IN_CODIGO 
                                                        , CTO.ORG_PAD_IN_CODIGO 
                                                        , CTO.ORG_IN_CODIGO 
                                                        , CTO.ORG_TAU_ST_CODIGO 
                                                        , CTO.CTO_IN_CODIGO 
                                                        , :B18 
                                                        , 'N') IN (:B17 , :B16 , :B15 , :B14 , :B13 , :B12 )) 
        AND EST.CTOENV_CH_ORIGEM IN (:B11 , :B10 , :B9 )
        AND DECODE( :B7 , 'A', CTO.CTO_DT_ASSINATURA , 'C', CTO.CTO_DT_CADASTRO) >= :B8 
        AND DECODE( :B7 , 'A', CTO.CTO_DT_ASSINATURA , 'C', CTO.CTO_DT_CADASTRO) <= :B6 
        AND DECODE( :B5 , 'AVISTA', CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) = 0 
                                              THEN 1 ELSE 0 END , '1', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 1 AND 12 
                                              THEN 1 ELSE 0 END , '2', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 13 AND 24 
                                              THEN 1 ELSE 0 END , '3', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 25 AND 36 
                                              THEN 1 ELSE 0 END , '4', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 37 AND 48 
                                              THEN 1 ELSE 0 END , '5', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 49 AND 60 
                                              THEN 1 ELSE 0 END , '6', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 61 AND 72 
                                              THEN 1 ELSE 0 END , '7', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 73 AND 84 
                                              THEN 1 ELSE 0 END , '8', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 85 AND 96 
                                              THEN 1 ELSE 0 END , '9', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 97 AND 108 
                                              THEN 1 ELSE 0 END , '10', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) BETWEEN 109 AND 120
                                              THEN 1 ELSE 0 END , '11', 
                                    CASE WHEN TRUNC(MONTHS_BETWEEN(PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO), 0) > 120 
                                              THEN 1 ELSE 0 END , 'T', 1) = 1 
        AND NVL( MGREL.PCK_REL_GLO_FNC.FNC_GLO_EMPREENDIMENTO_SINC(   EST.ORG_TAB_IN_CODIGO 
                                                                      , EST.ORG_PAD_IN_CODIGO 
                                                                      , EST.ORG_IN_CODIGO 
                                                                      , EST.ORG_TAU_ST_CODIGO 
                                                                      , EST.EMP_IN_CODIGO 
                                                                      , :B4 ), 0) = NVL( EST.EMP_IN_CODIGO, 0) 
        AND NVL( TPU.TPU_IN_CODIGO, 0) = NVL( DECODE( :B3 , 0, TPU.TPU_IN_CODIGO, :B3 ), 0) 
        AND CTO.ORG_TAB_IN_CODIGO = ATI.TAB_IN_CODIGO 
        AND CTO.ORG_PAD_IN_CODIGO = ATI.PAD_IN_CODIGO
        AND CTO.ORG_IN_CODIGO = ATI.ORG_IN_CODIGO 
        AND CTO.FIL_IN_CODIGO = ATI.FIL_IN_CODIGO 
        AND EST.CTO_ORG_TAB_IN_CODIGO = CTO.ORG_TAB_IN_CODIGO 
        AND EST.CTO_ORG_PAD_IN_CODIGO = CTO.ORG_PAD_IN_CODIGO 
        AND EST.CTO_ORG_IN_CODIGO = CTO.ORG_IN_CODIGO 
        AND EST.CTO_ORG_TAU_ST_CODIGO = CTO.ORG_TAU_ST_CODIGO 
        AND EST.CTO_IN_CODIGO = CTO.CTO_IN_CODIGO 
        AND CTO.ORG_TAB_IN_CODIGO = CTC.ORG_TAB_IN_CODIGO 
        AND CTO.ORG_PAD_IN_CODIGO = CTC.ORG_PAD_IN_CODIGO 
        AND CTO.ORG_IN_CODIGO = CTC.ORG_IN_CODIGO 
        AND CTO.ORG_TAU_ST_CODIGO = CTC.ORG_TAU_ST_CODIGO 
        AND CTO.CTO_IN_CODIGO = CTC.CTO_IN_CODIGO 
        AND CTO.CSF_IN_CODIGO = CLA.CSF_IN_CODIGO 
        AND CTC.AGN_TAB_IN_CODIGO = AID.AGN_TAB_IN_CODIGO 
        AND CTC.AGN_PAD_IN_CODIGO = AID.AGN_PAD_IN_CODIGO 
        AND CTC.AGN_IN_CODIGO = AID.AGN_IN_CODIGO 
        AND CTC.AGN_TAU_ST_CODIGO = AID.AGN_TAU_ST_CODIGO 
        AND AID.AGN_TAB_IN_CODIGO = AGN.AGN_TAB_IN_CODIGO 
        AND AID.AGN_PAD_IN_CODIGO = AGN.AGN_PAD_IN_CODIGO 
        AND AID.AGN_IN_CODIGO = AGN.AGN_IN_CODIGO 
        AND EST.ORG_TAB_IN_CODIGO = UND.ORG_TAB_IN_CODIGO(+) 
        AND EST.ORG_PAD_IN_CODIGO = UND.ORG_PAD_IN_CODIGO(+) 
        AND EST.ORG_IN_CODIGO = UND.ORG_IN_CODIGO (+) 
        AND EST.ORG_TAU_ST_CODIGO = UND.ORG_TAU_ST_CODIGO(+) 
        AND EST.UND_IN_CODIGO = UND.EST_IN_CODIGO (+) 
        AND UND.TPU_IN_CODIGO = TPU.TPU_IN_CODIGO(+) 
        AND AGN.AGN_TAB_IN_CODIGO = PEF.AGN_TAB_IN_CODIGO(+) 
        AND AGN.AGN_PAD_IN_CODIGO = PEF.AGN_PAD_IN_CODIGO(+) 
        AND AGN.AGN_IN_CODIGO = PEF.AGN_IN_CODIGO (+) 
        AND PEF.AGN_CH_TIPO (+)= 'P' 
        AND AGN.AGN_TAB_IN_CODIGO = PFC.AGN_TAB_IN_CODIGO(+) 
        AND AGN.AGN_PAD_IN_CODIGO = PFC.AGN_PAD_IN_CODIGO(+) 
        AND AGN.AGN_IN_CODIGO = PFC.AGN_IN_CODIGO (+) 
        AND PFC.AGN_CH_TIPO (+)= 'C' 
        AND CTO.ORG_TAB_IN_CODIGO = PAR.ORG_TAB_IN_CODIGO(+) 
        AND CTO.ORG_PAD_IN_CODIGO = PAR.ORG_PAD_IN_CODIGO(+) 
        AND CTO.ORG_IN_CODIGO = PAR.ORG_IN_CODIGO (+) 
        AND CTO.ORG_TAU_ST_CODIGO = PAR.ORG_TAU_ST_CODIGO(+) 
        AND CTO.CTO_IN_CODIGO = PAR.CTO_IN_CODIGO (+) 
        AND (( PAR.PAR_IN_CODIGO IN ( SELECT MAX( PCL.PAR_IN_CODIGO) 
                                      FROM MGCAR.CAR_PARCELA PCL 
                                      WHERE PCL.ORG_TAB_IN_CODIGO = CTO.ORG_TAB_IN_CODIGO 
                                        AND PCL.ORG_PAD_IN_CODIGO = CTO.ORG_PAD_IN_CODIGO 
                                        AND PCL.ORG_IN_CODIGO = CTO.ORG_IN_CODIGO 
                                        AND PCL.ORG_TAU_ST_CODIGO = CTO.ORG_TAU_ST_CODIGO 
                                        AND PCL.CTO_IN_CODIGO = CTO.CTO_IN_CODIGO 
                                        AND PCL.PAR_DT_VENCIMENTO = ( SELECT MAX( PAC.PAR_DT_VENCIMENTO) 
                                                                        FROM MGCAR.CAR_PARCELA PAC 
                                                                        WHERE PAC.PAR_CH_STATUS <> 'I' 
                                                                          AND PAC.ORG_TAB_IN_CODIGO = CTO.ORG_TAB_IN_CODIGO 
                                                                          AND PAC.ORG_PAD_IN_CODIGO = CTO.ORG_PAD_IN_CODIGO 
                                                                          AND PAC.ORG_IN_CODIGO = CTO.ORG_IN_CODIGO 
                                                                          AND PAC.ORG_TAU_ST_CODIGO = CTO.ORG_TAU_ST_CODIGO 
                                                                          AND PAC.CTO_IN_CODIGO = CTO.CTO_IN_CODIGO))) 
              OR PAR.PAR_IN_CODIGO IS NULL)