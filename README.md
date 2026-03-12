# 1 Data ingestion

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

PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=root

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

# Create table

con.execute("""
CREATE TABLE IF NOT EXISTS postgres_db.public.taxi_trips_raw AS
SELECT * FROM read_parquet(
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
)
LIMIT 0
""")

# Insert data

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

con.execute(f"""
    INSERT INTO postgres_db.public.taxi_trips_raw
    SELECT *
    FROM read_parquet('{url}')
    """)

print("Pipeline finished")

```

This Python script creates a simple data pipeline that loads data from a Parquet file into a PostgreSQL database using DuckDB.

Creates a DuckDB connection which allows DuckDB to communicate directly with a PostgreSQL database.

Creates a table in PostgreSQL if it does not already exist. The table structure is generated automatically based on the schema of a remote Parquet dataset.

Reads data from a remote Parquet file (NYC taxi trip data) and inserts it into the PostgreSQL table.


After running the command::

```
python ingest_data.py
```

the script starts executing the data pipeline. It connects to the PostgreSQL database and begins reading the Parquet file from the remote URL.

Because the dataset is relatively large, the process can take a few minutes. During this time, DuckDB reads the data from the Parquet file and sends it to PostgreSQL.

After a few minutes, the data will be inserted into the taxi_trips_raw table in the PostgreSQL database. Once the process finishes, the table will contain all the records from the dataset.

<br>

![2](images/2.jpg)
<br><br>


<br>

![2](images/2.jpg)
<br><br>
