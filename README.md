# flight-delay-app

**✈️ A PROPOS**
-------------------------------------------------------------------------------------------------------
-Objectif : Application de prédiction de retard de vol
-Réalisation : 
-Choix des données :
-Résulat : Ecart moyen +/- 13min 
-Démo : https://flight-delay-app-q3zgpvmvvvrmvdwbyd3fyw.streamlit.app/


**🛠️ TECHNOLOGIES**
-------------------------------------------------------------------------------------------------------
-Pipeline ETL : Python
  >Scraping : BeautifulSoup / Selenium
  >Requests : API
-Cloud : AWS
  > Déploiement pipeline ETL sur EC2
  > Stockage resultats et autres données sur S3
-Data Warehousing : Snowflake (SQL)
-Machine Learning : Scikit-learn (Random Forest)
-API : FastAPI
-Déploiement : Streamlit et render (API + .joblib)


**🧱 ARCHITECTURE**
-------------------------------------------------------------------------------------------------------

Listes de numéro vols
v
+-----------------------------+
| PIPELINE ETL (sur AWS EC2)  |   -> Flightradar24 : Données de vol general (scraping)
|                             |   -> OpenMeteo : Données de météo (API)
|                             |   -> Ourairports : Données GPS des aeroprts (csv)                             
+-----------------------------+
v
Dataset brut (sur AWS S3)      
v     
+-----------------------------+
| DATA WAREHOUSE              |   <- AirHelp : Note de ponctualité des aeroports (csv)
|                             |   <- AirHelp : Note de ponctualité des compagnies (csv)
|                             |
+-----------------------------+
v
Dataset nettoyé
v
+-----------------------------+
| MACHINE LEARNING            |
+-----------------------------+
v
Fichier .joblib
v
+-----------------------------+   
| API                         | 
+-----------------------------+
v
+-----------------------------+
| DÉPLOIEMENT                 |
+-----------------------------+
                 

**🖥️ DEVELOPPEMENT**
-------------------------------------------------------------------------------------------------------

1️⃣ PIPELINE ETL :
-Objectif : Constituer un dataset complet de vols à partir d’une liste de numéros de vol
-Réalisations : 
  > 🔍 EXTRACT
    Source	Données collectées	Méthode
    *Flightradar24	horaires prévus / réels, aéroports, compagnie, immatriculation avion	Scraping
    *OurAirports	coordonnées GPS des aéroports (pour météo)	CSV
    *OpenMeteo	vent, visibilité, pluie, température	API
    *AirHelp	notes de ponctualité compagnies et aéroports	CSV
  > 🔧 TRANSFORM
    *Filtrer : ne garder que les vols terminés
    *Standardisation : renaming, gestion des types (dates, numéros de vol…)
    *Calculs : Retard finalN retard du vol précédent de l'appareil (feature importante)
  > 💾 LOAD : 
  Sauvegarde du jeu de données en local (mode append)

2️⃣ DATA WAREHOUSING :
-Objectif : Nettoyer, stocker et exploiter les données dans un environnement Data Warehouse
-Réalisations :
  > Paramétrage : Création BDD, tables, pipelines Snowflake (Import des données depuis AWS S3)
  > Ingestion : Chargement du dataset brut
  > Processing : Suppression champs vides et valeurs aberrantes, normalisation


3️⃣ MACHINE LEARNING :
-Objectif : Entrainer le dataset nettoyé
-Réalisations : 
  > Définition de la variable cible : Retard final en minutes
  > Définition des variables explicatives : Horraires, météo, notes poncutalité aeroports et compagnies etc
  > Encodage des variables catégorielles (non numériques)
  > Split train/test


4️⃣ API :
-Objectif : Developper une API permettant d'estimer un retard grace .joblib (modele entrainé)
-Réalisations : 
  > Saisie : Récuperation des input 
  > Pipeline interne : Récuperation des données du vol selectionné en input. Même données et mêmes méthodes utilisées par le pipeline ETL du dataset.   
  > Endpoints : Les principaux endpoints sont GET /health (Statut API) et GET /predict-flight (Estimation retard en min)

5️⃣ DÉPLOIEMENT :
-Objectif : Déployer une démo en ligne
-Réalisations : 
  > API : Déploiement sur Render
  > Interface : Dévelppement front-end (barre de saisie, indicateurs),connexion API et déploiement sur Streamlit





