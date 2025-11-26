-- Creation du warehouse
CREATE OR REPLACE WAREHOUSE Warehouse_dst
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
-- Activation du warehouse
USE WAREHOUSE Warehouse_dst;


-- Creation base de données et schema
CREATE OR REPLACE DATABASE flight_delay_db;
CREATE OR REPLACE SCHEMA flight_delay_sch;
-- Activation base de données et schema
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;

-- Creation du file format (instructions de lecture des csv)
CREATE OR REPLACE FILE FORMAT flight_delay_fileformat
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL','null','','-','--','__')
  EMPTY_FIELD_AS_NULL = TRUE;

-- Creation stage
CREATE OR REPLACE STAGE FLIGHT_DELAY_SCH
    -- INFORMATIONS COMPTES AWS A COMPLETER
    URL='s3://XXXXXXXXX/'
    CREDENTIALS = (AWS_KEY_ID='XXXXXXXXX' AWS_SECRET_KEY='XXXXXXXXX')
    FILE_FORMAT = flight_delay_fileformat;




-- Creation tables
CREATE OR REPLACE TABLE dataset_table (
    ds_flight_code VARCHAR,
    ds_airline_code VARCHAR(2), 
    ds_airline_rating NUMBER(3,1),
    ds_flight_date VARCHAR,
    ds_flight_aircraft VARCHAR,
    ds_departure_airport VARCHAR,
    ds_arrival_airport VARCHAR,
    ds_departure_airport_code VARCHAR(3),
    ds_arrival_airport_code VARCHAR(3),
    ds_flight_duration VARCHAR,
    ds_departure_plan VARCHAR,
    ds_departure_real VARCHAR,
    ds_arrival_plan VARCHAR,
    ds_arrival_real VARCHAR,
    ds_departure_airport_rating NUMBER(3,1),
    ds_arrival_airport_rating NUMBER(3,1),
    ds_departure_airport_lat NUMBER(9,6),
    ds_departure_airport_long NUMBER(9,6),
    ds_arrival_airport_lat NUMBER(9,6),
    ds_arrival_airport_long NUMBER(9,6),
    ds_departure_airport_temp_cel NUMBER(5,2),
    ds_departure_airport_rain_mmHour NUMBER(5,2),
    ds_departure_airport_wind_kmh NUMBER(5,2),
    ds_departure_airport_vis_km NUMBER(5,2),
    ds_arrival_airport_temp_cel NUMBER(5,2),
    ds_arrival_airport_rain_mmHour NUMBER(5,2),
    ds_arrival_airport_wind_kmh NUMBER(5,2),
    ds_arrival_airport_vis_km NUMBER(5,2),
    ds_flight_status VARCHAR,
    ds_prev_delay_min NUMBER(6,2),
    ds_final_delay_min NUMBER(6,2),
     -- Clés étrangères
    FOREIGN KEY (ds_departure_airport_code) REFERENCES airport_rating_table(IATA_airport_code),
    FOREIGN KEY (ds_arrival_airport_code) REFERENCES airport_rating_table(IATA_airport_code),
    FOREIGN KEY (ds_airline_code) REFERENCES airline_rating_table(IATA_airline_code)
    
);


CREATE OR REPLACE TABLE airport_rating_table (
    airport VARCHAR,
    location_city VARCHAR,
    location_country VARCHAR,
    IATA_airport_code VARCHAR(3) PRIMARY KEY,
    airHelp_score VARCHAR, -- Conversion en float apres
    ponctuality_rating VARCHAR -- Conversion en float apres
);


CREATE OR REPLACE TABLE airline_rating_table (
    airline VARCHAR,
    IATA_airline_code VARCHAR(2)PRIMARY KEY,
    airHelp_score VARCHAR, -- Conversion en float apres
    ponctuality_rating VARCHAR -- Conversion en float apres
);


--Ingestion des données
COPY INTO dataset_table
FROM @FLIGHT_DELAY_SCH/Flight-delay_dataset.csv
FILE_FORMAT = flight_delay_fileformat
ON_ERROR = 'CONTINUE';


COPY INTO airline_rating_table
FROM @FLIGHT_DELAY_SCH/Flight-delay_airlines-ratings.csv
FILE_FORMAT = flight_delay_fileformat
ON_ERROR = 'CONTINUE';


COPY INTO airport_rating_table
FROM @FLIGHT_DELAY_SCH/Flight-delay_airports-ratings.csv
FILE_FORMAT = flight_delay_fileformat
ON_ERROR = 'CONTINUE';
