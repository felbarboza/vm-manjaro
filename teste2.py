import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt


cx_Oracle.init_oracle_client("C:\Oracle\instantclient_12_2")
con = cx_Oracle.connect("life", "lli8339e", "megacloud")
typeCursor = con.gettype('CURSOR')
cursor = con.cursor()
# l_cur = cursor.var(cx_Oracle.)
con.begin()
refCursor = cursor.callproc("mgcustom.PRC_CAR_EXTRATO_COMPLETO_EMP@life" ,[
      typeCursor,
      53,
      1,
      11,
      'G',
      12,
      'CFTETN',
      0,
      0,
      0,
      0,
      'RPS',
      '25-11-2020',
      708,
      0,
      0,
      0,
      'A',
      'N',
      'N',
      'S',
      'A',
      'Q',
      'U',
      'D',
      'X',
      'X',
      'X',
      'S',
      'S',
      'S',
      'T',
      'S',
      'N',
      'S',
      0,
      'S',
      'S'
])

for row in refCursor:
  print(row)