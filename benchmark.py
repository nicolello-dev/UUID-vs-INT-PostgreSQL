import psycopg2
import psycopg2.extras
import time
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

FETCHES = 500_000

# https://stackoverflow.com/questions/51105100/psycopg2-cant-adapt-type-uuid
psycopg2.extras.register_uuid()

conn = psycopg2.connect(
    dbname="UUID-test",
    user="postgres",
    password=os.environ.get("PGSQL_PASSWORD"),
    host="localhost",
    port="5432",
)

cur = conn.cursor()

start = time.time()

# Get random UUIDs and ints from the tables
cur.execute(
    "SELECT id FROM UUID ORDER BY RANDOM() LIMIT %s;",
    (FETCHES,),
)
RANDOM_UUIDS = cur.fetchall()
cur.execute("SELECT id FROM int ORDER BY RANDOM() LIMIT %s;", (FETCHES,))
RANDOM_INTS = cur.fetchall()

# Query both tables to see how long it takes

# Query the UUID table

UUID_START = time.time()
for i in range(FETCHES):
    cur.execute(
        "SELECT data FROM UUID WHERE id=%s::uuid;",
        (RANDOM_UUIDS[i][0],),
    )
    cur.fetchall()
UUID_END = time.time()

# Query the INT table
INT_START = time.time()
for i in range(FETCHES):
    cur.execute(
        "SELECT data FROM INT WHERE id=%s;",
        (RANDOM_INTS[i][0],),
    )
    cur.fetchall()
INT_END = time.time()

# Close the connection
cur.close()

print(f"UUID query time: {UUID_END - UUID_START}")
print(f"INT query time: {INT_END - INT_START}")
