import psycopg2
import psycopg2.extras
import time
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

INSERTS = 1_000_000

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

# Create the two tables,
# UUID for the one with UUIDs
# INT for the one with integers
cur.execute("DROP TABLE IF EXISTS UUID; DROP TABLE IF EXISTS INT;")
cur.execute(
    "CREATE TABLE UUID (id uuid PRIMARY KEY, data VARCHAR(100)); CREATE TABLE INT (id SERIAL PRIMARY KEY, data VARCHAR(100));"
)

start = time.time()

# Insert one million rows into each table
for i in range(INSERTS):
    print(
        f"Inserting row {i}/{INSERTS} (estimated time: {round((time.time() - start) / (i + 1) * (INSERTS - i), 2)}s)"
    )
    cur.execute(
        "INSERT INTO UUID (id, data) VALUES (%s::uuid, 'data'); INSERT INTO INT (data) VALUES ('data');",
        (uuid.uuid4(),),
    )
    cur.execute("COMMIT;")

# Close the connection
cur.close()
