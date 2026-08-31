-- Spark SQL / Databricks example: external raw table to managed analytics table.
-- Replace the placeholder path with a cloud-storage or Databricks Volume location.

CREATE DATABASE IF NOT EXISTS portfolio_flights;

DROP TABLE IF EXISTS portfolio_flights.flights_external;
CREATE TABLE portfolio_flights.flights_external
USING CSV
OPTIONS (
  path '/Volumes/your_catalog/your_schema/flights/*.csv',
  header 'true',
  inferSchema 'true'
);

CREATE OR REPLACE TABLE portfolio_flights.flights_managed
USING PARQUET
AS
SELECT *
FROM portfolio_flights.flights_external;

CREATE OR REPLACE TABLE portfolio_flights.route_metrics
USING PARQUET
AS
SELECT
  ORIGIN_AIRPORT_ID,
  ORIGIN,
  DEST_AIRPORT_ID,
  DEST,
  DISTANCE,
  COUNT(*) AS num_flights
FROM portfolio_flights.flights_managed
GROUP BY
  ORIGIN_AIRPORT_ID,
  ORIGIN,
  DEST_AIRPORT_ID,
  DEST,
  DISTANCE;

SELECT *
FROM portfolio_flights.route_metrics
ORDER BY num_flights DESC
LIMIT 20;
