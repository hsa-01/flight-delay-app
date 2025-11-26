import streamlit as st

st.set_page_config(page_title="Fonctionnement", page_icon="🔧", layout="centered")

# ==============================================================
# CSS GLOBAL 
# ==============================================================
st.markdown(
    """
    <style>
    /* Marges identiques à la page principale */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 950px;
    }

    /* Bordure horizontale personnalisée (identique à "---" de Streamlit) */
    hr {
        border: 0;
        height: 1px;
        background: #e6e6e6;
        margin: 2rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ==============================================================
# TITRE  
# ==============================================================
st.title("🔧 DESCRIPTION")

st.markdown("---")

# ==============================================================
# FONCTIONNEMENT 
# ==============================================================
st.markdown("<h3 style='text-align:center;'>ℹ️ A propos</h3>", unsafe_allow_html=True)

st.markdown(
    """
    L’objectif de cette application est de prédire le retard d’un vol à partir des informations renseignées par l’utilisateur (numéro de vol et date du jour).
    Pour cela, l’application utilise un modèle entraîné (Machine Learning) accessible via une API.
    Le jeu de données constitué en amont de l’entraînement grâce à un pipeline ETL est traité dans un Data Warehouse.<br><br>
    Le détail de l’architecture et des technologies utilisées est disponible dans le repo GitHub : 
        <a href="https://github.com/hsa-01/flight-delay-app" target="_blank">hsa-01</a>
    """,
    unsafe_allow_html=True
)


st.markdown("---")

# ==============================================================
# MODE D'EMPLOI 
# ==============================================================
st.markdown("<h3 style='text-align:center;'>🖥️ Mode d'emploi</h3>", unsafe_allow_html=True)
st.markdown("Pour trouver un numéro de vol valide, suivez les instructions ci-dessous :", unsafe_allow_html=True)


st.markdown(
    """
    1. Aller sur : "https://www.flightradar24.com/"
    2. Sélectionner un aéroport (icone bleu)
    3. Cliquer sur la section 'On ground'
    4. Selectionner un des appareils listé (puis cliquer sur 'Aircraft info', cela ouvre une nouvelle page)
    5. Sur la nouvelle page, copier le numero de vol (colonne 'Flight') correspondant au prochain départ de l'avion
    """
)


st.markdown(
    """
    <div style="font-size:0.80rem; color:#000000; background-color:#E0F7FA; padding:10px; border-radius:5px;">
        💡 <strong>Note : </strong><br>
        - Date : L’estimation ne fonctionne qu’avec les vols du jour uniquement<br>
        - Format output : Le délai est indiqué en minutes, avec des valeurs positives pour les retards et négatives pour les vols en avance<br>
        - Résultat : Les prédictions sont des estimations basées sur des données historiques. Les retards réels peuvent varier en fonction de facteurs imprévus
    </div>
    """,
    unsafe_allow_html=True
)



st.markdown("---")

# ==============================================================
# SOURCE DE DONNÉES 
# ==============================================================
st.markdown("<h3 style='text-align:center;'>🔢 Sources de données</h3>", unsafe_allow_html=True)
st.markdown("Les sources de données qui ont permis de constituer le dataset (d’entraînement) sont les suivantes :", unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("##### Données de vol")
    st.markdown(
        """
        - Source : Flightradar24
        - Description : Tracking de vols direct/historique
        - Type : Scraping
        - Données :
            * Aéroport de départ/arrivée
            * Horaires prévus/réels
            * Compagnies
            * Immatriculation avion
        """
    )
    
    st.markdown("##### Données météorologiques")
    st.markdown(
        """
        - Source : OpenMétéo
        - Description : Données météo prévision/historique 
        - Type : API
        - Données :
            * Température
            * Vent
            * Visibilité
            * Pluie
        """
    )

with col2:
    st.markdown("##### Coordonnées aéroports")
    st.markdown(
        """
        - Source : OurAirports
        - Description : Blog
        - Type : Fichier csv
        - Données : 
            * Longitude aéroports départ/arrivée
            * Latitude aéroports départ/arrivée
        - Note : Input de l'API OpenMétéo
        """
    )
    
    st.markdown("##### Notes ponctualité")
    st.markdown(
        """
        - Source : AirHelp
        - Description : Entreprise de service
        - Type : Fichier csv
        - Données : 
            * Note de ponctualité compagnies
            * Note de ponctualité aéroports 
        """
    )

st.markdown("---")



# ==============================================================
# DONNÉES CLÉS 
# ==============================================================

st.markdown("<h3 style='text-align:center;'>📊 Données clés</h3>", unsafe_allow_html=True)

col_a, col_b, col_c = st.columns(3)


with col_a:
    st.markdown(
        "<div style='text-align:center;'>"
        "<h5>Précision moyenne</h5>"
        "<p style='font-size:30px;'>± 13,43min</p>"
        "</div>", 
        unsafe_allow_html=True
    )

with col_b:
    st.markdown(
        "<div style='text-align:center;'>"
        "<h5>Vols analysés</h5>"
        "<p style='font-size:30px;'>18,9K+</p>"
        "</div>", 
        unsafe_allow_html=True
    )

with col_c:
    st.markdown(
        "<div style='text-align:center;'>"
        "<h5>Facteur n°1 des retards</h5>"
        "<p style='font-size:30px;'>Retard précédent</p>"
        "</div>", 
        unsafe_allow_html=True
    )

st.markdown("---")

# ==============================================================
# CONTACT 
# ==============================================================

st.markdown(
    """
    <div style="font-size:0.80rem; color:#000000; background-color:#e6e6e5; padding:10px; border-radius:5px;">
        👤<strong>Contact Linkedin: </strong>
        <a href="https://www.linkedin.com/in/hafed-sassi-a48b9b125/" target="_blank" style="text-decoration:none;">
            Hafed S.
        </a>
    </div>
    """,
    unsafe_allow_html=True
)
