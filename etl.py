# -*- coding: utf-8 -*-


import psycopg2
import pandas as pd
from sqlalchemy import create_engine
dictio = []
connection = psycopg2.connect(user = "postgresetl",
                              password = "secret",
                              host = "127.0.0.1",
                              port = "5432",
                              database = "odoo")

cursor = connection.cursor()
# Print PostgreSQL Connection properties

# Print PostgreSQL version
cursor.execute("SELECT * FROM etl_contacto")
records = cursor.fetchall()
print("Imprimiendo registros")

for row in records:
    if '-' in row[3]:
        rut = row[3].split("-")
        if int(rut[0]) > 70000000:
            print("Migrar:", row[3])
            dictio.append({"nombre":row[1],"apellido":row[2], "rut":row[3]})

    else:
        print("Dañado", row[3])
cursor.close()
#insertando en la tabla destino
df = pd.DataFrame(dictio)
df.shape
df.index
df.columns
df.count()
df.sum()
engine = create_engine('postgresql://postgresetl:secret@127.0.0.1:5432/odoo')
df.to_sql('vadatos',engine)
cur = connection.cursor()
for i in dictio:
    result = i['nombre'],i['apellido'],i['rut']
    sql = """insert into adatos values {}""".format(result)
    cur.execute(sql)
connection.commit()
connection.close()

print("PostgreSQL connection is closed")
