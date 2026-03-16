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


# Create Schema Staging

con.execute("""
CREATE SCHEMA IF NOT EXISTS postgres_db.staging
""")


# Create table: staging.trips

print("Creating staging.trips")

con.execute("""
CREATE OR REPLACE TABLE postgres_db.staging.trips AS
SELECT
    "VendorID",
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    "PULocationID",
    "DOLocationID",
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM postgres_db.raw.taxi_trips_raw
WHERE trip_distance > 0
AND lpep_pickup_datetime >= '2019-01-01'
AND lpep_pickup_datetime < '2019-02-01';
""")


# Create Schema dwh

con.execute("""
CREATE SCHEMA IF NOT EXISTS postgres_db.dwh
""")


# Create table: dwh.dim_date

print("Creating dwh.dim_date")

con.execute("""
CREATE OR REPLACE TABLE postgres_db.dwh.dim_date AS
            
SELECT DISTINCT
    CAST(lpep_pickup_datetime AS DATE) AS date,
    EXTRACT(YEAR FROM lpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM lpep_pickup_datetime) AS month,
    EXTRACT(DAY FROM lpep_pickup_datetime) AS day,
    EXTRACT(DOW FROM lpep_pickup_datetime) AS weekday
FROM postgres_db.staging.trips;
""")

# Create table: dwh.dim_location

print("Creating dwh.dim_location")

con.execute("""
CREATE OR REPLACE TABLE postgres_db.dwh.dim_location AS

WITH cte AS 

	(SELECT DISTINCT
	    "PULocationID" AS location_id
	FROM postgres_db.staging.trips
	UNION
	SELECT DISTINCT
	    "DOLocationID"
	FROM postgres_db.staging.trips)

select 
	cte.location_id, 
	zone_lookup."Borough",
	zone_lookup."Zone",
	zone_lookup."service_zone"


FROM cte LEFT JOIN postgres_db.raw.zone_lookup
ON cte.location_id = zone_lookup."LocationID";
""")

# Create table: dwh.dim_payment

print("Creating dwh.dim_payment")

con.execute("""
CREATE OR REPLACE TABLE postgres_db.dwh.dim_payment AS

SELECT DISTINCT
    payment_type,
    CASE 
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
        ELSE 'Other'
    END AS description
FROM postgres_db.staging.trips;
""")

# Create table: dwh.fact_trips

print("Creating dwh.fact_trips")

con.execute("""
CREATE OR REPLACE TABLE postgres_db.dwh.fact_trips AS
            
SELECT
    CAST(lpep_pickup_datetime AS DATE) AS date,
    "PULocationID" AS pickup_location_id,
    "DOLocationID" AS dropoff_location_id,
    payment_type,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount
FROM postgres_db.staging.trips;
""")