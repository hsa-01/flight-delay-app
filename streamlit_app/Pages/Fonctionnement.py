import streamlit as st

st.set_page_config(page_title="Fonctionnement", page_icon="🔧", layout="centered")

# CSS personnalisé pour réduire les marges
st.markdown(
    """
    <style>
    .block-container {
        padding-left: 2rem;
        padding-right: 2rem;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# === TITRE ===
st.title("🔧 Fonctionnement")

st.markdown("---")

# === DESCRIPTION GÉNÉRALE ===
st.markdown("<h3 style='text-align:center;'>Comment fonctionne la prédiction ?</h3>", unsafe_allow_html=True)

st.markdown(
    """
    Cette application utilise un **modèle d'intelligence artificielle** pour prédire le retard potentiel d'un vol.
    Elle combine plusieurs sources de données pour fournir une estimation précise.
    """
)

st.markdown("---")

# === SOURCES DE DONNÉES ===
st.markdown("<h3 style='text-align:center;'>📊 Sources de données</h3>", unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### ✈️ Données de vol")
    st.markdown(
        """
        - Numéro de vol
        - Compagnie aérienne
        - Aéroports de départ/arrivée
        - Horaires prévus
        - Historique du vol
        """
    )
    
    st.markdown("### 🌤️ Données météorologiques")
    st.markdown(
        """
        - Conditions météo au départ
        - Conditions météo à l'arrivée
        - Prévisions en temps réel
        - Visibilité et vents
        """
    )

with col2:
    st.markdown("### 📈 Historique des retards")
    st.markdown(
        """
        - Retards précédents du vol
        - Performance de la compagnie
        - Tendances saisonnières
        - Patterns horaires
        """
    )
    
    st.markdown("### 🤖 Modèle IA")
    st.markdown(
        """
        - Algorithme de machine learning
        - Entraîné sur données historiques
        - Mise à jour continue
        - Précision optimisée
        """
    )

st.markdown("---")

# === PROCESSUS ===
st.markdown("<h3 style='text-align:center;'>⚙️ Processus de prédiction</h3>", unsafe_allow_html=True)

st.markdown(
    """
    1. **Scrapping des données** : Récupération des informations du vol en temps réel
    2. **Analyse météorologique** : Collecte des conditions météo aux aéroports concernés
    3. **Calcul des retards précédents** : Analyse de l'historique du vol
    4. **Agrégation des données** : Préparation des features pour le modèle
    5. **Prédiction finale** : Le modèle IA calcule le retard estimé
    """
)

st.markdown("---")

# === PRÉCISION ===
st.markdown("<h3 style='text-align:center;'>🎯 Précision du modèle</h3>", unsafe_allow_html=True)

col_a, col_b, col_c = st.columns(3)

with col_a:
    st.metric(label="Précision moyenne", value="85%", delta="↑ 5%")

with col_b:
    st.metric(label="Vols analysés", value="10K+", delta="Mensuel")

with col_c:
    st.metric(label="Mise à jour", value="Temps réel", delta="Continue")

st.markdown("---")

st.info("💡 **Note** : Les prédictions sont des estimations basées sur des données historiques et actuelles. Les retards réels peuvent varier en fonction de facteurs imprévus.")