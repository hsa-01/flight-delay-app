-- LISTE DE COMMANDES POUR ANALYSE DU DATASET POST-TRAITEMENT (via dashabord Snowflake)



-- VU D'ENSEMBLE DONNÉES RETARD FINAL (in min)
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;

SELECT 
    AVG(ds_final_delay_min) AS moyenne,
    MEDIAN(ds_final_delay_min) AS mediane,
    MIN(ds_final_delay_min) AS valeur_min,
    MAX(ds_final_delay_min) AS valeur_max
FROM dataset_table;



-- VU D'ENSEMBLE DONNÉES RETARD PRÉCÉDENT (in min)
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;

SELECT 
    AVG(ds_prev_delay_min) AS moyenne,
    MEDIAN(ds_prev_delay_min) AS mediane,
    MIN(ds_prev_delay_min) AS valeur_min,
    MAX(ds_prev_delay_min) AS valeur_max
FROM dataset_table;



-- VU D'ENSEMBLE DONNÉES DURÉE DE VOL (in min)
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;

SELECT 
    AVG(ds_flight_duration_min) AS moyenne,
    MEDIAN(ds_flight_duration_min) AS mediane,
    MIN(ds_flight_duration_min) AS valeur_min,
    MAX(ds_flight_duration_min) AS valeur_max
FROM dataset_table;



-- NOMBRE DE VOL LISTÉ
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;


SELECT COUNT(*) AS nombre_de_lignes
FROM dataset_table;


-- NOMBRE D'AEROPORT UNIQUE LISTÉ
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;


SELECT COUNT(DISTINCT airport) AS nb_aeroports_uniques
FROM (
    SELECT ds_departure_airport_code AS airport FROM dataset_table
    UNION
    SELECT ds_arrival_airport_code AS airport FROM dataset_table
) AS all_airports;



-- NOMBRE DE COMPAGNIE UNIQUE LISTÉ
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;


SELECT
    COUNT(DISTINCT ds_airline_code) AS nb_compagnie,
FROM dataset_table;



-- NOMBRE DE ROUTE UNIQUE LISTÉ
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;


SELECT COUNT(DISTINCT ds_airline_code || '-' || ds_departure_airport_code || '-' || ds_arrival_airport_code) AS nb_routes_uniques
FROM dataset_table;



-- TOP 3 DES COMPAGNIES LES PLUS REPRÉSENTÉS
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;


SELECT 
    ds_airline_code,
    COUNT(*) AS nb_vols
FROM dataset_table
GROUP BY ds_airline_code
ORDER BY nb_vols DESC
LIMIT 3;


-- TOP 3 DES AÉROPORTS LES PLUS REPRÉSENTÉS
------------------------------------------------------------------
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;


SELECT airport_code, COUNT(*) AS nb_vols
FROM (
    SELECT ds_departure_airport_code AS airport_code
    FROM dataset_table
    UNION ALL
    SELECT ds_arrival_airport_code AS airport_code
    FROM dataset_table
) AS all_airports
GROUP BY airport_code
ORDER BY nb_vols DESC
LIMIT 3;
