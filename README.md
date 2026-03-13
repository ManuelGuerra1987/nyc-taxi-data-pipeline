# nyc taxi data pipeline


### Table of contents

- [Data ingestion](#Data-ingestion)

# Data ingestion

### Setting up the database

In a new terminal run the following command:

```
docker compose up -d
```

This command starts and runs the services defined in the docker-compose.yml file:

```yaml

services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root2
      - POSTGRES_PASSWORD=root2
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5433:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    volumes:
      - "./data_pgadmin:/var/lib/pgadmin"
    ports:
      - "8080:80"
    depends_on:
      - pgdatabase  
``` 

pgdatabase service creates a PostgreSQL database container. 

pgadmin service: This creates a pgAdmin container, which is a web interface to manage PostgreSQL databases.

Postgres Port mapping:

5432 → PostgreSQL port inside the container
5433 → Port on your computer. So you can connect from your machine using localhost:5433

Pgadmin Port mapping:

8080:80 → This means you can open pgAdmin in your browser at: http://localhost:8080


### Register Server

Go to http://localhost:8080/

* PGADMIN_DEFAULT_EMAIL=admin@admin.com
* PGADMIN_DEFAULT_PASSWORD=root

Right-click on servers and register server

<br>

![1a](images/1a.jpg)
<br><br>

<br>

![1b](images/1b.jpg)
<br><br>

<br>

![1c](images/1c.jpg)
<br><br>



### Python script

```python

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

```




After running the command::

```
python ingest_data.py
```

This Python script uses DuckDB as an intermediary engine to download data from the internet and load it directly into a PostgreSQL database. First, the script defines a PostgreSQL connection string containing the necessary parameters to connect to the database, including the host, port, database name, user, and password. 

Next, the script installs and loads the Postgres extension for DuckDB using the commands INSTALL postgres and LOAD postgres. This extension allows DuckDB to connect to a PostgreSQL database and execute queries that read from or write to it. Then the script uses the ATTACH command to establish the connection to PostgreSQL using the previously defined connection string. Inside DuckDB, this attached database is referenced as postgres_db.

Once the connection is established, the script creates a schema called raw inside PostgreSQL. A raw schema is commonly used to store raw or unprocessed data, meaning the data is kept exactly as it was received from the original source before any transformations or cleaning are applied.

After that, the script creates a table called taxi_trips_raw inside the raw schema. To define the structure of this table, the script uses DuckDB’s read_parquet function to read a remote Parquet file containing New York green taxi trip data. The query includes LIMIT 0, which means the table is created with the same column structure as the file but without inserting any rows yet.

Once the table structure is created, the script proceeds to insert the actual trip data from the same Parquet file. This is done with an INSERT INTO ... SELECT * FROM read_parquet(url) statement. DuckDB reads the Parquet file directly from the URL and sends the resulting rows to the destination table in PostgreSQL.

Finally, the script performs a similar process for another table called zone_lookup. In this case, the data comes from a CSV file that contains information about taxi zones in New York City. 

<br>

![2](images/2.jpg)
<br><br>


<br>

![4](images/4.jpg)
<br><br>
