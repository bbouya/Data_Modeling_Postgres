import psycopg2 as ps

conn = ps.connect('host = 127.0.0.1 dbname = sparkifydb user = postgres password = 1234')

query = "SELECT * FROM SONGPLAYS WHERE ARTIST_ID IS NOT NULL LIMIT 5"

cur = conn.cursor()
cur.execute(query)

print(cur.fetchone())
