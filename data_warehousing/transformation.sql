-- Activation du warehouse
USE WAREHOUSE Warehouse_dst;
-- Activation base de données et schema
USE DATABASE flight_delay_db;
USE SCHEMA flight_delay_sch;


-------------------------------------------------------------------------
-- AIRLINE_RATING_TABLE
-------------------------------------------------------------------------

-- Conversion des notes str->float : Etape 1 / Creation de nouvelles colonnes
ALTER TABLE airline_rating_table ADD COLUMN airHelp_score_num NUMBER(3,1);
ALTER TABLE airline_rating_table ADD COLUMN ponctuality_rating_num NUMBER(3,1);

-- Conversion des notes str->float : Etape 2 / Copie des données + conversion dans les nouvelles colonnes
UPDATE airline_rating_table
SET 
    airHelp_score_num = TO_NUMBER(REPLACE(airHelp_score, ',', '.'), 10, 1),
    ponctuality_rating_num = TO_NUMBER(REPLACE(ponctuality_rating, ',', '.'), 10, 1);


-- Conversion des notes str->float : Etape 3 / Suppression des anciennes colonnes + renomage des nouvelles 
ALTER TABLE airline_rating_table DROP COLUMN airHelp_score;
ALTER TABLE airline_rating_table DROP COLUMN ponctuality_rating;

ALTER TABLE airline_rating_table RENAME COLUMN airHelp_score_num TO airHelp_score;
ALTER TABLE airline_rating_table RENAME COLUMN ponctuality_rating_num TO ponctuality_rating;

-- Conversion des notes str->float : Etape 4 / Verification finale
SELECT airline, IATA_airline_code, airHelp_score, ponctuality_rating
FROM airline_rating_table
LIMIT 5;


-------------------------------------------------------------------------
-- AIRPORT_RATING_TABLE
-------------------------------------------------------------------------

-- Conversion des notes str->float : Etape 1 / Creation de nouvelles colonnes
ALTER TABLE airport_rating_table ADD COLUMN airHelp_score_num_airport NUMBER(3,1);
ALTER TABLE airport_rating_table ADD COLUMN ponctuality_rating_num_airport NUMBER(3,1);

-- Conversion des notes str->float : Etape 2 / Copie des données + conversion dans les nouvelles colonnes
UPDATE airport_rating_table
SET 
    airHelp_score_num_airport = TO_NUMBER(REPLACE(airHelp_score, ',', '.'), 10, 1),
    ponctuality_rating_num_airport = TO_NUMBER(REPLACE(ponctuality_rating, ',', '.'), 10, 1);


-- Conversion des notes str->float : Etape 3 / Suppression des anciennes colonnes + renomage des nouvelles 
ALTER TABLE airport_rating_table DROP COLUMN airHelp_score;
ALTER TABLE airport_rating_table DROP COLUMN ponctuality_rating;

ALTER TABLE airport_rating_table RENAME COLUMN airHelp_score_num_airport TO airHelp_score;
ALTER TABLE airport_rating_table RENAME COLUMN ponctuality_rating_num_airport TO ponctuality_rating;

-- Conversion des notes str->float : Etape 4 / Verification finale
SELECT airport, location_city,location_country,IATA_airport_code,airHelp_score, ponctuality_rating
FROM airport_rating_table
LIMIT 5;



-------------------------------------------------------------------------
-- DATASET_TABLE
-------------------------------------------------------------------------
-- Copie backup du dataset
CREATE OR REPLACE TABLE dataset_table_backup AS SELECT * FROM dataset_table;



-- ETAPE 1 : JOINTURES BDDR
-- Suppression des données des colonnes ds_airline_rating, ds_departure_airport_rating et ds_arrival_airport_rating
--(Jointures realisés intialement avec Python, finalement repris ici en SQL avec BDDR) 
UPDATE dataset_table SET ds_airline_rating = NULL;
UPDATE dataset_table SET ds_departure_airport_rating = NULL;
UPDATE dataset_table SET ds_arrival_airport_rating = NULL;
-- Jointure note de ponctualité compagnie (airline_rating_table -> dataset_table)
UPDATE dataset_table
SET ds_airline_rating = airline_rating_table.ponctuality_rating
FROM airline_rating_table
WHERE dataset_table.ds_airline_code = airline_rating_table.iata_airline_code;
-- Jointure note de ponctualité aéroport de depart (airport_rating_table -> dataset_table)
UPDATE dataset_table
SET ds_departure_airport_rating = airport_rating_table.ponctuality_rating
FROM airport_rating_table
WHERE dataset_table.ds_departure_airport_code = airport_rating_table.iata_airport_code;
-- Jointure note de poncutalité aéroport d'arrivée (aiport_rating_table -> dataset_table)
UPDATE dataset_table
SET ds_arrival_airport_rating = airport_rating_table.ponctuality_rating
FROM airport_rating_table
WHERE dataset_table.ds_arrival_airport_code = airport_rating_table.iata_airport_code;


-- ETAPE 2 : Suppression des vols dont le statut est different de "Landed"
-- a)Identification des modes de la colonne "STATUS" 
-- b)Mode unique : "Landed", pas de vols ayant comme mode "Diverted" ou autres -> Pas de filtre necessaire à ce niveau
SELECT DISTINCT ds_flight_status FROM dataset_table; 

-- ETAPE 3 : Suppression des colonnes non numeriques (sauf codes aéroport départ/arrivée et airline code)
ALTER TABLE dataset_table DROP COLUMN ds_flight_code;
ALTER TABLE dataset_table DROP COLUMN ds_flight_date;
ALTER TABLE dataset_table DROP COLUMN ds_flight_aircraft;
ALTER TABLE dataset_table DROP COLUMN ds_departure_airport;

ALTER TABLE dataset_table DROP COLUMN ds_arrival_airport;
ALTER TABLE dataset_table DROP COLUMN ds_departure_plan;
ALTER TABLE dataset_table DROP COLUMN ds_departure_real;
ALTER TABLE dataset_table DROP COLUMN ds_arrival_plan;
ALTER TABLE dataset_table DROP COLUMN ds_arrival_real;

ALTER TABLE dataset_table DROP COLUMN ds_departure_airport_lat;
ALTER TABLE dataset_table DROP COLUMN ds_departure_airport_long;

ALTER TABLE dataset_table DROP COLUMN ds_arrival_airport_lat;
ALTER TABLE dataset_table DROP COLUMN ds_arrival_airport_long;

ALTER TABLE dataset_table DROP COLUMN ds_flight_status;

-- ETAPE 4 : Suppression des lignes vides
-- a) Colonne ds_prev_delay_min -> 4917 lignes supp
SELECT COUNT(*) AS nb_vides
FROM dataset_table
WHERE ds_prev_delay_min IS NULL
   OR TRIM(ds_prev_delay_min) = '';

DELETE FROM dataset_table
WHERE ds_prev_delay_min IS NULL
   OR TRIM(ds_prev_delay_min) = '';
   
-- b) Colonne ds_final_delay_min -> 2 lignes supp
DELETE FROM dataset_table
WHERE ds_final_delay_min IS NULL
   OR TRIM(ds_final_delay_min) = '';
-- c) Colonne ds_arrival_airport_vis_km -> 762 lignes supp
DELETE FROM dataset_table
WHERE ds_arrival_airport_vis_km IS NULL
   OR TRIM(ds_arrival_airport_vis_km) = '';
-- d) Colonne ds_arrival_airport_wind_kmh -> 32 lignes supp
DELETE FROM dataset_table
WHERE ds_arrival_airport_wind_kmh IS NULL
   OR TRIM(ds_arrival_airport_wind_kmh) = '';
-- e) Colonne ds_arrival_airport_rain_mmhour -> 29 lignes supp
DELETE FROM dataset_table
WHERE ds_arrival_airport_rain_mmhour IS NULL
   OR TRIM(ds_arrival_airport_rain_mmhour) = '';
-- f) Colonne ds_arrival_airport_temp_cel -> 28 lignes supp
DELETE FROM dataset_table
WHERE ds_arrival_airport_temp_cel IS NULL
   OR TRIM(ds_arrival_airport_temp_cel) = '';
-- g) Colonne ds_departure_airport_vis_km  -> 46 lignes supp
DELETE FROM dataset_table
WHERE ds_departure_airport_vis_km IS NULL
   OR TRIM(ds_departure_airport_vis_km) = '';
-- h) Colonne ds_departure_airport_wind_kmh  -> 34 lignes supp
DELETE FROM dataset_table
WHERE ds_departure_airport_wind_kmh IS NULL
   OR TRIM(ds_departure_airport_wind_kmh) = '';
-- i) Colonne ds_departure_airport_rain_mmhour  -> 37 lignes supp
DELETE FROM dataset_table
WHERE ds_departure_airport_rain_mmhour IS NULL
   OR TRIM(ds_departure_airport_rain_mmhour) = '';
-- j) Colonne ds_departure_airport_temp_cel  -> 36 lignes supp
DELETE FROM dataset_table
WHERE ds_departure_airport_temp_cel IS NULL
   OR TRIM(ds_departure_airport_temp_cel) = '';
-- k) Colonne ds_flight_duration  -> 36 lignes supp
DELETE FROM dataset_table
WHERE ds_flight_duration IS NULL
   OR TRIM(ds_flight_duration) = '';



-- ETAPE 5 : Conversion temps de vol "hh:min" -> en min 
-- a) Ajouter la colonne
ALTER TABLE dataset_table ADD COLUMN ds_flight_duration_min NUMBER;
-- b) la remplir avec la durée convertie
UPDATE dataset_table
SET ds_flight_duration_min =
    (CAST(SPLIT_PART(ds_flight_duration, ':', 1) AS INT) * 60
     + CAST(SPLIT_PART(ds_flight_duration, ':', 2) AS INT));
--c) Suppression colonne initiale en "hh:mm"
ALTER TABLE dataset_table DROP COLUMN ds_flight_duration;


-- ETAPE 6 : Imputation champs vides notes poncutalité compagnies/aeroports par mediane
-- a) Mediane note de compagnie -> 884 lignes traitées
UPDATE dataset_table
SET ds_airline_rating = (
    SELECT MEDIAN(ponctuality_rating) 
    FROM airline_rating_table
    WHERE ponctuality_rating IS NOT NULL
)
WHERE ds_airline_rating IS NULL;

-- b) Mediane note aeroport de depart -> 3730 lignes traités
UPDATE dataset_table
SET ds_departure_airport_rating = (
    SELECT MEDIAN(ponctuality_rating) 
    FROM airport_rating_table
    WHERE ponctuality_rating IS NOT NULL
)
WHERE ds_departure_airport_rating IS NULL;

-- c) Mediane note aeroport d'arrivée -> 3759 lignes traités
UPDATE dataset_table
SET ds_arrival_airport_rating = (
    SELECT MEDIAN(ponctuality_rating) 
    FROM airport_rating_table
    WHERE ponctuality_rating IS NOT NULL
)
WHERE ds_arrival_airport_rating IS NULL;


-- ETAPE 7 : Suppression valeurs aberrantes retard
-- a) Retard final --> 0 lignes supprimées sur +550 / 441 lignes supprimées sur -550
SELECT COUNT(*) AS nb_lignes
FROM dataset_table
WHERE ds_final_delay_min > 550;

DELETE FROM dataset_table
WHERE ds_final_delay_min > 550;

DELETE FROM dataset_table
WHERE ds_final_delay_min < -550;


-- b) Retard vol precedent --> 2 lignes supprimées sur +550 / 260 lignes supprimées
SELECT COUNT(*) AS nb_lignes
FROM dataset_table
WHERE ds_prev_delay_min > 550;

DELETE FROM dataset_table 
WHERE ds_prev_delay_min > 550;

DELETE FROM dataset_table
WHERE ds_prev_delay_min < -550;


-- ETAPE 7 : Normalisation
-- a) Ajout de nouvelles colonnes 
ALTER TABLE dataset_table ADD COLUMN ds_airline_rating_norm FLOAT;
ALTER TABLE dataset_table ADD COLUMN ds_departure_airport_rating_norm FLOAT;
ALTER TABLE dataset_table ADD COLUMN ds_arrival_airport_rating_norm FLOAT;
-- b) Calcul et stockage min/max dans les variables
SET min_val_airline_rating = (SELECT MIN(ds_airline_rating) FROM dataset_table);
SET max_val_airline_rating = (SELECT MAX(ds_airline_rating) FROM dataset_table);
SET min_val_departure_rating = (SELECT MIN(ds_departure_airport_rating) FROM dataset_table);
SET max_val_departure_rating = (SELECT MAX(ds_departure_airport_rating) FROM dataset_table);
SET min_val_arrival_rating = (SELECT MIN(ds_arrival_airport_rating) FROM dataset_table);
SET max_val_arrival_rating = (SELECT MAX(ds_arrival_airport_rating) FROM dataset_table);
-- c) Utiliser les variables dans ton UPDATE
UPDATE dataset_table
SET ds_airline_rating_norm = 
    (ds_airline_rating - $min_val_airline_rating) / NULLIF($max_val_airline_rating - $min_val_airline_rating, 0);
UPDATE dataset_table
SET ds_departure_airport_rating_norm = 
    (ds_departure_airport_rating - $min_val_departure_rating) / NULLIF($max_val_departure_rating - $min_val_departure_rating, 0);
UPDATE dataset_table
SET ds_arrival_airport_rating_norm = 
    (ds_arrival_airport_rating - $min_val_arrival_rating) / NULLIF($max_val_arrival_rating - $min_val_arrival_rating, 0);     
-- d) Suppression des colonnes initales
ALTER TABLE dataset_table DROP COLUMN ds_airline_rating;
ALTER TABLE dataset_table DROP COLUMN ds_departure_airport_rating;
ALTER TABLE dataset_table DROP COLUMN ds_arrival_airport_rating;





-- ETAP8 : Verification finale

SELECT
    COUNT(CASE WHEN ds_airline_code IS NULL OR ds_airline_code = '' THEN 1 END) AS nb_vides_ds_airline_code,
    COUNT(CASE WHEN ds_airline_rating IS NULL THEN 1 END) AS nb_vides_ds_airline_rating,
    COUNT(CASE WHEN ds_departure_airport_code IS NULL OR ds_departure_airport_code = '' THEN 1 END) AS nb_vides_ds_departure_airport_code,
    COUNT(CASE WHEN ds_arrival_airport_code IS NULL OR ds_arrival_airport_code = '' THEN 1 END) AS nb_vides_ds_airrival_airport_code,
    COUNT(CASE WHEN ds_departure_airport_rating IS NULL THEN 1 END) AS nb_vides_ds_departure_airport_rating,
    COUNT(CASE WHEN ds_arrival_airport_rating IS NULL THEN 1 END) AS nb_vides_ds_arrival_airport_rating,
    COUNT(CASE WHEN ds_departure_airport_temp_cel IS NULL THEN 1 END) AS nb_vides_ds_departure_airport_temp_cel,
    COUNT(CASE WHEN ds_departure_airport_rain_mmhour IS NULL THEN 1 END) AS nb_vides_ds_departure_airport_rain_mmhour,
    COUNT(CASE WHEN ds_departure_airport_wind_kmh IS NULL THEN 1 END) AS nb_vides_ds_departure_airport_wind_kmh,
    COUNT(CASE WHEN ds_departure_airport_vis_km IS NULL THEN 1 END) AS nb_vides_ds_departure_airport_vis_km,
    COUNT(CASE WHEN ds_arrival_airport_temp_cel IS NULL THEN 1 END) AS nb_vides_ds_arrival_airport_temp_cel,
    COUNT(CASE WHEN ds_arrival_airport_rain_mmhour IS NULL THEN 1 END) AS nb_vides_ds_arrival_airport_rain_mmhour,
    COUNT(CASE WHEN ds_arrival_airport_wind_kmh IS NULL THEN 1 END) AS nb_vides_ds_arrival_airport_wind_kmh,
    COUNT(CASE WHEN ds_arrival_airport_vis_km IS NULL THEN 1 END) AS nb_vides_ds_arrival_airport_vis_km,
    COUNT(CASE WHEN ds_prev_delay_min IS NULL THEN 1 END) AS nb_vides_ds_prev_delay_min,
    COUNT(CASE WHEN ds_final_delay_min IS NULL THEN 1 END) AS nb_vides_ds_final_delay_min,
    COUNT(CASE WHEN ds_flight_duration_min IS NULL THEN 1 END) AS nb_vides_ds_flight_duration_min
FROM dataset_table;

    


SELECT * FROM dataset_table;

