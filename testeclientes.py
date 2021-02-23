import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt

def gera_clientes(id_emp, data_base, to_excel=False, path=''):
      """Gera relatório de clientes de um empreendimento

      Args:
          id_emp (str): empreendimento como chave do mongo, ex: 'viv'
          data_base (data): data base para o relatório dd-mm-yyyy 
          to_excel (bool, optional): Se quiser o df para excel. Defaults to False.
          path (str, optional): caminho para salvar o relatório. Defaults to ''.

      Returns:
            clientes: Dataframe no formato do relatório de clientes
      """

      cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
      con = cx_Oracle.connect("life", "lli8339e", "megacloud")
      cursor = con.cursor()

      fil_in_codigo = int(fcFuncoes.retorna_filial(id_emp))
      
      dados_empreendimentos = requisao_generica_banco_de_dados('dados_empreendimentos')

      codigos_empreendimentos=[]
      for linha in dados_empreendimentos.index:
      if(dados_empreendimentos.at[linha, "filial"]==fil_in_codigo):
            fases = dados_empreendimentos.at[linha,"fases"]
            for fase in fases:
            codigos_empreendimentos.append(int(dados_empreendimentos.at[linha,"fases"][fase]["codigo"]))

      query='''SELECT EST.ORG_IN_CODIGO 
            , EST.EMP_ST_CODIGO 
            , EST.EMP_ST_NOME 
            , EST.BLO_IN_CODIGO 
            , EST.BLO_ST_CODIGO 
            , EST.BLO_ST_NOME 
            , EST.UND_IN_CODIGO 
            , EST.UND_ST_CODIGO 
            , NVL( EST.UND_RE_AREAPRIVATIVA, 0) UND_RE_AREAPRIVATIVA 
            , CTO.CTO_IN_CODIGO 
            , DECODE(:B28 , 'N', ROUND(NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO@life (    CTO.ORG_TAB_IN_CODIGO 
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
                              , ROUND( NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO@life (    CTO.ORG_TAB_IN_CODIGO 
                                                                                    , CTO.ORG_PAD_IN_CODIGO 
                                                                                    , CTO.ORG_IN_CODIGO 
                                                                                    , CTO.ORG_TAU_ST_CODIGO 
                                                                                    , CTO.CTO_IN_CODIGO 
                                                                                    , 'D' 
                                                                                    , :B18 
                                                                                    , '' 
                                                                                    , '')
                                    , 0) * ( NVL( MGREL.PCK_REL_FNC.FNC_CAR_BUSCA_PERC_PART_DIST@life( CTO.ORG_TAB_IN_CODIGO 
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
            DECODE(:B28 , 'N', ROUND( NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO@life ( CTO.ORG_TAB_IN_CODIGO 
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
                        , ROUND( NVL( MGREL.PCK_REL_FNC.FNC_CAR_VALOR_CONTRATO@life (  CTO.ORG_TAB_IN_CODIGO 
                                                                              , CTO.ORG_PAD_IN_CODIGO 
                                                                              , CTO.ORG_IN_CODIGO 
                                                                              , CTO.ORG_TAU_ST_CODIGO 
                                                                              , CTO.CTO_IN_CODIGO 
                                                                              , :B27 
                                                                              , :B18 
                                                                              , '' 
                                                                              , '')
                                    , 0)* ( NVL( MGREL.PCK_REL_FNC.FNC_CAR_BUSCA_PERC_PART_DIST@life(  CTO.ORG_TAB_IN_CODIGO 
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
            , MGREL.PCK_REL_FNC.FNC_CAR_STATUS_CTO_DATA@life(  CTO.ORG_TAB_IN_CODIGO 
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
            , MGREL.PCK_REL_GLO_FNC.FNC_GLO_CONCATENA_TELAGENTE@life(  AGN.AGN_TAB_IN_CODIGO 
                                                            , AGN.AGN_PAD_IN_CODIGO 
                                                            , AGN.AGN_IN_CODIGO 
                                                            , ' ' 
                                                            , 'S' 
                                                            , NULL 
                                                            , 99) TEL_AGENTE 
            , AGN.AGN_ST_COMPLEMENTO 
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
            , TRUNC( MONTHS_BETWEEN( ( NVL( PAR.PAR_DT_VENCIMENTO, CTO.CTO_DT_CADASTRO)), CTO.CTO_DT_CADASTRO), 0) PRAZO_PAGAMENTO 
            , TPU.TPU_IN_CODIGO 
            , TPU.TPU_ST_DESCRICAO 
            , MGCAR.CAR_CONTRATO CTO 

            FROM  MGCAR.CAR_PARCELA@life PAR 
            , MGGLO.GLO_PESSOA_FISICA@life PFC 
            , MGGLO.GLO_PESSOA_FISICA@life PEF 
            , MGDBM.DBM_TIPOLOGIA_UNIDADE@life TPU 
            , MGDBM.DBM_UNIDADE@life UND 
            , MGGLO.GLO_AGENTES@life AGN 
            , MGREL.VW_CAR_ESTRUTURA@life EST 
            WHERE CTO.ORG_TAB_IN_CODIGO = :org_tab_in_codigo 
            AND CTO.ORG_PAD_IN_CODIGO = :org_pad_in_codigo 
            AND CTO.ORG_TAU_ST_CODIGO = :org_tau_st_codigo 
            AND NVL( EST.EMP_IN_CODIGO,0) = DECODE( :B23 , 0, NVL( EST.EMP_IN_CODIGO,0), :B23 ) 
            AND CTO.CSF_IN_CODIGO = DECODE( :B22 , 0, CTO.CSF_IN_CODIGO, :B22 ) 
            AND CTO.CTO_CH_TIPO IN ( :B21 , :B20 , :B19 ) 
            AND ( MGREL.PCK_REL_FNC.FNC_CAR_STATUS_CTO_DATA@life(  CTO.ORG_TAB_IN_CODIGO 
                                                            , CTO.ORG_PAD_IN_CODIGO 
                                                            , CTO.ORG_IN_CODIGO 
                                                            , CTO.ORG_TAU_ST_CODIGO 
                                                            , CTO.CTO_IN_CODIGO 
                                                            , :B18 
                                                            , 'N') IN (:ativo , :B16 , :distr , :B14 , :B13 , :quit )) 
            AND EST.CTOENV_CH_ORIGEM IN (:B11 , :B10 , :B9 )
            AND DECODE( :B7 , 'A', CTO.CTO_DT_ASSINATURA , 'C', CTO.CTO_DT_CADASTRO) >= to_date(01-01-2000, 'dd/mm/yyyy') 
            AND DECODE( :B7 , 'A', CTO.CTO_DT_ASSINATURA , 'C', CTO.CTO_DT_CADASTRO) <= to_date(01-01-2100, 'dd/mm/yyyy') 
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
            AND NVL( MGREL.PCK_REL_GLO_FNC.FNC_GLO_EMPREENDIMENTO_SINC@life(   EST.ORG_TAB_IN_CODIGO 
                                                                        , EST.ORG_PAD_IN_CODIGO 
                                                                        , EST.ORG_IN_CODIGO 
                                                                        , EST.ORG_TAU_ST_CODIGO 
                                                                        , EST.EMP_IN_CODIGO 
                                                                        , :B4 ), 0) = NVL( EST.EMP_IN_CODIGO, 0) 
            AND NVL( TPU.TPU_IN_CODIGO, 0) = NVL( DECODE( :B3 , 0, TPU.TPU_IN_CODIGO, :B3 ), 0) 
            AND EST.CTO_ORG_TAB_IN_CODIGO = CTO.ORG_TAB_IN_CODIGO 
            AND EST.CTO_ORG_PAD_IN_CODIGO = CTO.ORG_PAD_IN_CODIGO 
            AND EST.CTO_ORG_IN_CODIGO = CTO.ORG_IN_CODIGO 
            AND EST.CTO_ORG_TAU_ST_CODIGO = CTO.ORG_TAU_ST_CODIGO 
            AND EST.CTO_IN_CODIGO = CTO.CTO_IN_CODIGO 
            AND AGN.AGN_TAB_IN_CODIGO = PEF.AGN_TAB_IN_CODIGO(+) 
            AND AGN.AGN_PAD_IN_CODIGO = PEF.AGN_PAD_IN_CODIGO(+) 
            AND AGN.AGN_IN_CODIGO = PEF.AGN_IN_CODIGO (+) 
            AND EST.ORG_TAB_IN_CODIGO = UND.ORG_TAB_IN_CODIGO(+) 
            AND EST.ORG_PAD_IN_CODIGO = UND.ORG_PAD_IN_CODIGO(+) 
            AND EST.ORG_IN_CODIGO = UND.ORG_IN_CODIGO (+) 
            AND EST.ORG_TAU_ST_CODIGO = UND.ORG_TAU_ST_CODIGO(+) 
            AND EST.UND_IN_CODIGO = UND.EST_IN_CODIGO (+) 
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
      '''

      data_base_query = '''SELECT to_date(:v_dt_base, 'dd/mm/yyyy')
                        FROM dual'''

      data_base_arrumada = cursor.execute(data_base_query, v_dt_base=data_base).fetchall()[0][0]
      clientes = pd.DataFrame()
      for codigo_empreendimento in codigos_empreendimentos:
            clientes_params = { 'B10':'G',
                              'B11':'U',
                              'quit':'Q',
                              'B13':'X',
                              'B14':'X',
                              'distr':'D',
                              'B16':'U',
                              'ativo':'A',
                              'B18':data_base_arrumada, 
                              'B19':'N',
                              'B20':'P',
                              'B21':'V',
                              'B22':0,
                              'B22':0,
                              'B23':codigo_empreendimento,
                              'B27': 'O',
                              'B28': 'N',
                              'org_tau_st_codigo':'G',
                              'org_pad_in_codigo':1,
                              'org_tab_in_codigo':53,
                              'B3':0,
                              'B4':'N',
                              'B5':'T',
                              'B7':'C',
                              'B7':'C',
                              'B9':'X'}
            temp = pd.read_sql(query,con,params=clientes_params)

            if(len(clientes.index)==0):
                  clientes=temp
            else:
                  clientes = pd.concat([clientes, temp])

      if(to_excel):
            clientes.to_excel(path + '\\rel_clientes_'+ id_emp + '_' + data_base + '.xlsx', index=False)
      
      return clientes