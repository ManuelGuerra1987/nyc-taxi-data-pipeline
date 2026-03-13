import duckdb

# Postgres conection

POSTGRES_CONN = """
host=localhost
port=5433
dbname=ny_taxi
user=root2
password=root2
"""

con = duckdb.connect()
con.execute("INSTALL postgres")
con.execute("LOAD postgres")

print("Connecting to Postgres")

con.execute(f"""
ATTACH '{POSTGRES_CONN}'
AS postgres_db (TYPE POSTGRES)
""")

print("Creating destination table if not exists")

# Create Schema Raw

con.execute("""
CREATE SCHEMA IF NOT EXISTS postgres_db.raw
""")


# Create table

con.execute("""
CREATE TABLE IF NOT EXISTS postgres_db.raw.taxi_trips_raw AS
SELECT * FROM read_parquet(
'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet'
)
LIMIT 0
""")

# Insert taxi trips data

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet"

con.execute(f"""
    INSERT INTO postgres_db.raw.taxi_trips_raw
    SELECT *
    FROM read_parquet('{url}')
    """)

print("Taxi trips inserted")


# Taxi Zone Lookup Table 

zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

con.execute(f"""
CREATE TABLE IF NOT EXISTS postgres_db.raw.zone_lookup AS
SELECT *
FROM read_csv_auto('{zone_url}')
LIMIT 0
""")

# Insert zone data
con.execute(f"""
INSERT INTO postgres_db.raw.zone_lookup
SELECT *
FROM read_csv_auto('{zone_url}')
""")

print("Zone lookup inserted")