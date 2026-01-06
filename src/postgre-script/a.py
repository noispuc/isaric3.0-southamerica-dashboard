import os
import tempfile
import itertools
import pandas as pd
from dbfread import DBF
import datasus_dbc

INPUT_DBC = "datas/DENGBR18.dbc"
source_encoding = "latin-1"

with tempfile.TemporaryDirectory() as tmp:
    dbf_path = os.path.join(tmp, os.path.splitext(os.path.basename(INPUT_DBC))[0] + ".dbf")
    datasus_dbc.decompress(INPUT_DBC, dbf_path)

    table = DBF(dbf_path, encoding=source_encoding)
    df = pd.DataFrame(iter(table))

df.columns = df.columns.str.lower()

print("Colunas do DBC:")
for c in df.columns:
    print(c)

print("\nPrimeiras linhas:")
print(df.head())