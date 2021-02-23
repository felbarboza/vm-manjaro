import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt


cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
con = cx_Oracle.connect("life", "lli8339e", "megacloud")

cursor = con.cursor()

data = pd.read_sql('''SELECT * FROM ALL_OBJECTS@life WHERE OBJECT_TYPE IN ('FUNCTION', 'PACKAGE', 'PROCEDURE', 'TABLE')''', con)

data.to_excel('./permissao.xlsx')