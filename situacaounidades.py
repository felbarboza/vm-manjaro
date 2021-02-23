import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt


cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
con = cx_Oracle.connect("life", "lli8339e", "megacloud")

query ='''select
        u.org_tab_in_codigo,
        u.org_pad_in_codigo,
        u.org_in_codigo,
        u.org_tau_st_codigo,
        u.est_in_codigo,
        u.und_re_areatotal,
        u.und_re_peso,
        u.und_in_andar,
        u.und_st_matricula
      from mgdbm.dbm_unidade@life u'''

teste = pd.read_sql(query, con)
teste.to_excel('./qqehisso.xlsx')

try:
  sim=True
except Exception as e: