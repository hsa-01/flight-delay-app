# flight-delay-app



**✈️ A PROPOS**
-------------------------------------------------------------------------------------------------------
- Objectif : Application de prédiction de retard de vol
- Réalisation : Pipeline ETL (dataset), Data Warehousing (traitement dataset), Machine Learning (entrainement), API, Interface et Déploiement
- Source de données : Flightradar24, OpenMeteo, AirHelp, OurAirports
- Résulat : Ecart moyen +/- 13min 
- Démo : https://flight-delay-app-q3zgpvmvvvrmvdwbyd3fyw.streamlit.app/


**🛠️ TECHNOLOGIES**
-------------------------------------------------------------------------------------------------------
- Pipeline ETL : Python
  > Scraping : BeautifulSoup / Selenium
  > Requests : API
- Cloud : AWS
  > Déploiement pipeline ETL sur EC2
  > Stockage résultats et autres données sur S3
- Data Warehousing : Snowflake (SQL)
- Machine Learning : Scikit-learn (Random Forest)
- API : FastAPI
- Déploiement : Streamlit et render (API + .joblib)


**🧱 ARCHITECTURE**
-------------------------------------------------------------------------------------------------------
<pre><code>
  
Listes de numéros de vols
v
+-----------------------------+
| PIPELINE ETL (sur AWS EC2)  |   -> Flightradar24 : Données de vol générales (scraping)
|                             |   -> OpenMeteo : Données de météo (API)
|                             |   -> OurAirports : Données GPS des aéroports (csv)                             
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
+-----------------------------+       +-----------------------------+
| API                         |   >   |  DÉPLOIEMENT                |
+-----------------------------+       |                             |
v                                     |                             |
+-----------------------------+       |                             |
| INTERFACE                   |   >   |                             |
+-----------------------------+       +-----------------------------+
                 
</code></pre>

**🖥️ DEVELOPPEMENT**
-------------------------------------------------------------------------------------------------------

1️⃣ PIPELINE ETL :
- Objectif : Constituer un dataset complet de vols à partir d’une liste de numéros de vol
- Réalisations : 
  > 🔍 EXTRACT
    * Flightradar24 (Scraping) :	Horaires prévus/réels, aéroports de départ/arrivée, compagnie, immatriculation avion
    * OurAirports	(csv) : Coordonnées GPS des aéroports pour extraction des données météo
    * OpenMeteo	(API) : Vent, visibilité, pluie, température des aéroports 
  > 🔧 TRANSFORM
    * Filtre : Conservation des vols terminés uniquement
    * Standardisation : Renommage des colonnes, répartition des données dans les bonnes colonnes, adaptation formats (date)
    * Calculs : Retard final retard du vol précédent de l'appareil (feature importante)
  > 💾 LOAD : 
    * Sauvegarde du jeu de données en local (mode append)

2️⃣ DATA WAREHOUSING :
- Objectif : Nettoyer, stocker et exploiter les données dans un environnement Data Warehouse
- Réalisations :
  > Paramétrage : Création de la BDD, tables et stage (Import des données depuis AWS S3)
  > Ingestion : Chargement du dataset brut
  > Processing : Suppression champs vides et valeurs aberrantes, normalisation
  > Jointures : Enrichissement du dataset avec les notes de ponctualité des compagnies et aéroports de départ/arrivée (Source : AirHelp / Type : csv)


3️⃣ MACHINE LEARNING :
- Objectif : Entrainer le dataset nettoyé
- Réalisations : 
  > Définition de la variable cible : Retard final en minutes
  > Définition des variables explicatives : Horaires, météo, notes poncutalité aéroports et compagnies etc
  > Encodage des variables catégorielles (non numériques)
  > Split train/test


4️⃣ API :
- Objectif : Developper une API permettant d'estimer un retard grace .joblib (modele entrainé)
- Réalisations : 
  > Saisie : Récuperation des input 
  > Pipeline interne : Récuperation des données du vol selectionné en input. Même données et mêmes méthodes utilisées par le pipeline ETL du dataset.   
  > Endpoints : Les principaux endpoints sont GET /health (Statut API) et GET /predict-flight (Estimation retard en min)


5️⃣  INTERFACE :
- Objectif : Developper une interface permettant recuperer les input utilisateurs et de retourner le résultat (connecté à l'API)
- Réalisation : 
  > Interface : Developpement du front-end de l'application pour déploiement sur Streamlit


6️⃣ DÉPLOIEMENT :
- Objectif : Déployer l'application en ligne
- Réalisations : 
  > API : Déploiement sur Render
  > Interface : Dévelppement front-end (barre de saisie, indicateurs),connexion à l'API et déploiement sur Streamlit







