import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt



def gera_extrato_completo(fil_in_codigo, emp_in_codigo, data_base, cto_in_codigo=0):
  """Gera extrato completo

  Args:
      fil_in_codigo (int): Código da filial
      emp_in_codigo (int): Código do empreendimento
      data_base (data): Data base no formato 'dd-mm-yyyy'
      cto_in_codigo (int): Código do contrato se não passar nada pega todos do empreendimento 

  Returns:
      extrato_completo: Dataframe no formato extrato completo

  """  
  print('--Extraindo Extrato Completo--')
  time_inicio = dt.datetime.today()
  
  #Estabelece a conexão com o banco de dados (precisa estar com o instant client instalado na máquina)
  #https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html baixar o 12.2
  #colocar as configurações da conexão na pasta e colocar as variáveis de ambiente do windows
  cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
  con = cx_Oracle.connect("life", "lli8339e", "megacloud")
  cursor = con.cursor()
  
  #transforma a data_base em datetime para realizar as queries
  data_base_query = '''SELECT to_date(:v_dt_base, 'dd/mm/yyyy')
                        FROM dual'''

  data_base_arrumada = cursor.execute(data_base_query, v_dt_base=data_base).fetchall()[0][0]

  #lê a query que gera o df com os contratos do empreendimento
  with open('cursor_extrato_completo_query.txt', 'r') as file:
    cursorQuery = file.read()

  #parametros para a query
  cursor_params = { 'v_dt_base':data_base, 
        'v_data_base':data_base_arrumada,
        'v_tab':53,
        'v_pad':1,
        'v_cod':fil_in_codigo-1,
        'v_tau':'G',
        'v_fil':fil_in_codigo,
        'v_emp':emp_in_codigo,
        'v_blc':0,
        'v_cto':cto_in_codigo,
        'v_agn_in_codigo':0,
        'v_ativo':'A',
        'v_quit':'Q',
        'v_inadim':'U',
        'v_distr':'D',
        'v_cessao':'X',
        'v_trans':'X'}
        
  #Dataframe com os contratos do empreendimento
  cursor_table=pd.read_sql(cursorQuery,con,params=cursor_params)

  #query para pegar uma variável utilizada na requisição do extrato completo (não sei pra que serve, mas precisa)
  cursor.execute('''
  SELECT NVL(pco.pco_ch_reajprojetado, 'B')
    FROM mgdbm.dbm_parametro_contabilidade@life pco
    WHERE pco.org_tab_in_codigo = :v_tab
      AND pco.org_pad_in_codigo = :v_pad
      AND pco.org_in_codigo     = :fil_in_codigo
      AND pco.org_tau_st_codigo = :v_tau
  ''', v_tab=53, v_pad=1, fil_in_codigo=fil_in_codigo, v_tau='G')

  vs_IndiceProjetado=cursor.fetchall()[0][0]

  #abre arquivo com a query que gera o extrato completo
  with open('extrato_completo_query.txt', 'r') as file:
    extrato_completo_query= file.read().replace('\n', '')

  print("Número de contratos no empreendimento: ", len(cursor_table.index))
  
  i=0
  extrato_completo=[]
  #passa de contrato em contrato e adiciona ao dataframe
  while i < len(cursor_table.index):
    print('-----   ', i+1, " de ", len(cursor_table.index), '   -----', end = '\r', flush=True)

    #variáveis necessárias retiradas dos dados do contrato
    cto_in_codigo = cursor_table['CTO_IN_CODIGO'][i].item()
    cto_re_valorcontrato=cursor_table['CTO_RE_VALORCONTRATO'][i].item()
    cto_dt_cadastro=cursor_table['CTO_DT_CADASTRO'][i].strftime("%d/%m/%Y")
    cto_dt_assinatura=cursor_table['CTO_DT_ASSINATURA'][i].strftime("%d/%m/%Y")
    agn_st_nome=cursor_table['AGN_ST_NOME'][i]
    cus_st_descricao=cursor_table['CUS_ST_DESCRICAO'][i]
    cus_in_reduzido=cursor_table['CUS_IN_REDUZIDO'][i].item()
    cto_ch_status=cursor_table['CTO_CH_STATUS'][i]
    agn_tab_in_codigo=cursor_table['AGN_TAB_IN_CODIGO'][i].item()
    agn_pad_in_codigo=cursor_table['AGN_PAD_IN_CODIGO'][i].item()
    agn_in_codigo=cursor_table['AGN_IN_CODIGO'][i].item()
    cto_re_vlroricontrato=cursor_table['CTO_RE_VLRORICONTRATO'][i].item()
    emp_st_nome=cursor_table['EMP_ST_NOME'][i]
    cto_ch_reajusteanual=cursor_table['CTO_CH_REAJUSTEANUAL'][i]
    cto_re_taxaant=cursor_table['CTO_RE_TAXAANT'][i].item()
    cto_re_taxaant_sac=cursor_table['CTO_RE_TAXAANT_SAC'][i].item()
    cto_re_taxaant_tp=cursor_table['CTO_RE_TAXAANT_TP'][i].item()
    query_params={  'codigo_contrato':cto_in_codigo,
                    'valorcontrato':cto_re_valorcontrato,
                    'data_cadastro':cto_dt_cadastro,
                    'data_assinatura':cto_dt_assinatura,
                    'nome':agn_st_nome,
                    'descricao':cus_st_descricao,
                    'custo_reduzido':cus_in_reduzido,
                    'status':cto_ch_status,
                    'agn_tabela_in_codigo':agn_tab_in_codigo,
                    'agente_pad_in_codigo':agn_pad_in_codigo,
                    'agente_in_codigo':agn_in_codigo,
                    'cto_re_valororicontrato':cto_re_vlroricontrato,
                    'empreendimento_st_nome':emp_st_nome,
                    'vs_indiceprojetado': vs_IndiceProjetado,
                    'cto_ch_reajusteanual': cto_ch_reajusteanual,
                    'cto_re_taxaant': cto_re_taxaant,
                    'cto_re_taxaant_sac': cto_re_taxaant_sac,
                    'cto_re_taxaant_tp': cto_re_taxaant_tp,
                    'cto_org_tab_in_codigo':53,
                    'cto_org_pad_in_codigo':1,
                    'cto_org_in_codigo':fil_in_codigo-1,
                    'cto_org_tau_st_codigo':'G',
                    'v_dt_base':data_base_arrumada,
                    'v_st_termo':'CFTETN',
                    'v_const':'',
                    'v_desct':'',
                    'v_tipoindice':'RP',
                    'v_descongela':'M',
                    'v_descap_tp':'S',
                    'v_descap_sac':'S',
                    'p_cons_taxa':'S',
                    'p_parc_caucao':'T', 
                    'p_vl_corrigido':'N',
                    'p_cons_confdivida':'S',
                    'p_par_secur':'T',
                    'p_cons_jr_tp_sac':'N'}

    #reconecta ao banco de dados em caso de erro de query ou caso a conexão caia
    try:
      temp= pd.read_sql(extrato_completo_query,con,params=query_params)  
    except cx_Oracle.Error as e:
      con = cx_Oracle.connect("life", "lli8339e", "megacloud")
      cursor = con.cursor()
      continue

    #adiciona as colunas de data da primeira e ultima parcela para cada contrato
    temp["DATA_PRIMEIRA_PARC"] = min(temp.loc[temp.CTO_IN_CODIGO==cto_in_codigo, "PAR_DT_VENCIMENTO"])
    temp["DATA_ULTIMA_PARC"] = max(temp.loc[temp.CTO_IN_CODIGO==cto_in_codigo, "PAR_DT_VENCIMENTO"])

    #organiza o dataframe no formato ideal do extrato completo
    temp=temp[['EMP_ST_NOME',
    'CTO_IN_CODIGO',
    'CTO_RE_VALORCONTRATO',
    'CTO_DT_CADASTRO',
    'CTO_DT_ASSINATURA',
    'CTO_CH_STATUS',
    'AGN_ST_NOME',
    'CUS_ST_DESCRICAO',
    'CUS_IN_REDUZIDO',
    'PAR_IN_CODIGO',
    'PAR_CH_PARCELA',
    'PAR_DT_VENCIMENTO',
    'PAR_RE_VALORORIGINAL',
    'PAR_RE_VALORCORRIGIDO',
    'PAR_ST_ORIGEM',
    'PAR_ST_OBSERVACAO',
    'PAR_DT_BAIXA',
    'PAR_RE_NUMERODIAS_ATRASO',
    'PAR_RE_VALORENCARGOS',
    'PAR_RE_VALORDESCONTO',
    'PAR_RE_RESIDUOCOBRANCA',
    'PAR_RE_VALORTAXAS',
    'PAR_RE_VALORPAGO',
    'PAR_CH_REAJUSTE',
    'PAR_ST_SIGLAINDICE',
    'PAR_ST_DEFASAGEM',
    'PAR_DT_VIGENCIAINDICE',
    'PAR_RE_VALORATUALIZADO',
    'PAR_RE_VALORRESIDUO',
    'PAR_RE_VALORRESIDUOCORRIGIDO',
    'PAR_IN_RESIDUO',
    'COBPAR_IN_CODIGO',
    'RESCOB_RE_VALORACOBRAR',
    'PAR_BO_CONFDIVIDA',
    'VALOR_QUITACAO',
    'DATA_ULT_REAJ_ANUAL',
    'DATA_PRIMEIRA_PARC',
    'DATA_ULTIMA_PARC',
    'VALOR_BAIXA_CCRED',
    'SLD_CCRED_CORRIGIDO',
    'CTO_RE_MORA',
    'CTO_RE_VLRORICONTRATO',
    'NOSSO_NUMERO']]

    #adiciona esse dataframe temporário ao dataframe geral
    if(i==0):
      extrato_completo = temp
    else:
      extrato_completo = pd.concat([extrato_completo, temp])
    i+=1
  return extrato_completo
    
  time_fim = dt.datetime.today()

  print("Temp decorrido: ",time_fim-time_inicio)

extrato_completo = gera_extrato_completo(17, 937, '10-02-2021', 434)

extrato_completo.to_excel('./extrato_completo.xlsx', index=False)
