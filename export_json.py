import json
import psycopg2

username = 'khapusova_diana'
password = 'lask3w0984resh'
database = 'postgres'
host = 'localhost'
port = '5433'

OUTPUT_FILE_T = 'export_csv_json.json'

TABLES = [
    'country',
    'state',
    'reason',
    'place',
    'death',
    'time'
]

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

data = {}
with conn:
    cur = conn.cursor()

    for table in TABLES:
        cur.execute('SELECT * FROM ' + table)
        rows = []
        fields = [x[0] for x in cur.description]

        for row in cur:
            rows.append(dict(zip(fields, row)))

        data[table] = rows

with open(OUTPUT_FILE_T, 'w') as outf:
    json.dump(data, outf, default=str)