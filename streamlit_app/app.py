import streamlit as st
import requests
import json
import time

# ==============================================================
# API CONFIGURATION 
# ==============================================================
API_URL = "https://flight-delay-app.onrender.com"

st.set_page_config(page_title="Prédiction Retard Vol", page_icon="✈️", layout="centered")


# ==============================================================
# CSS GLOBAL 
# ==============================================================
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 950px;
    }

    /* === BOUTONS === */
    div.stButton > button[kind="primary"] {
        background: linear-gradient(90deg, #4A90E2, #007BFF);
        color: white !important;
        border: none;
        height: 3rem;
        font-size: 1rem;
        font-weight: 600;
        border-radius: 8px;
        width: 100%;
        cursor: pointer;
        transition: all 0.2s ease-in-out;
    }
    div.stButton > button[kind="primary"]:hover {
        background: linear-gradient(90deg, #007BFF, #0056b3);
        transform: translateY(-2px);
    }

    div.stButton > button[kind="secondary"] {
        background: linear-gradient(90deg, #E74C3C, #C0392B);
        color: white !important;
        border: none;
        height: 3rem;
        font-size: 1rem;
        font-weight: 600;
        border-radius: 8px;
        width: 100%;
        cursor: pointer;
        transition: all 0.2s ease-in-out;
    }
    div.stButton > button[kind="secondary"]:hover {
        background: linear-gradient(90deg, #C0392B, #A93226);
        transform: translateY(-2px);
    }

    /* === CHAMPS INPUT === */
    .stTextInput > div > div > input {
        height: 3rem !important;
        font-size: 1rem;
        border-radius: 8px;
        padding: 0 1rem !important;
        text-align: left;
        align-items: center;
    }


    /* === TEXTES DE STATUT (style uniforme avec labels Streamlit) === */
    .status-text, .status-text div, .status-text p {
        font-size: 0.8rem !important;   /* même taille que les labels d'input */
        color: #262730 !important;     /* même couleur que les labels Streamlit */
        font-weight: 400 !important;
        text-align: left;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# === TITRE PRINCIPAL ===
st.title("✈️ Prédiction du retard d'un vol")
st.markdown("Saisis un numéro de vol et une date pour obtenir une estimation du retard.")
st.markdown("---")

# ==============================================================
# SESSION VARIBALES 
# ==============================================================
if "show_results" not in st.session_state:
    st.session_state.show_results = False
if "departure_code" not in st.session_state:
    st.session_state.departure_code = "Not found"
if "arrival_code" not in st.session_state:
    st.session_state.arrival_code = "Not found"

# ==============================================================
# SEARCH BAR 
# ==============================================================
st.markdown("<h4 style='text-align:left; margin-bottom:1rem;'>🔍 Recherche</h4>", unsafe_allow_html=True)
with st.container():
    col1, col2, col3, col4 = st.columns([4, 4, 4, 2])
    with col1:
        flight_number = st.text_input("Numéro de vol (Ex : AF7338)")
    with col2:
        flight_date = st.text_input("Date du vol (JJ/MM/AA)")
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)  # alignement vertical
        clicked = st.button("PREDICTION", type="primary")
    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        reset_clicked = st.button("RESET", type="secondary")

st.markdown("---")

# ==============================================================
# BUTTON 
# ==============================================================
if clicked:
    st.session_state.show_results = True

if reset_clicked:
    # 🔄 Réinitialisation complète (inputs + résultats)
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()


# ==============================================================
# FLIGHT DATA DISPLAY 
# ==============================================================
if st.session_state.show_results:
    # === SECTION : Données de vol ===
    st.markdown("<h4 style='text-align:left; margin-bottom:1rem;'>📊 Données de vol</h4>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([2, 1, 2])

    with c1:
        dep_placeholder = st.empty()
        dep_placeholder.markdown(
            f"<div style='text-align:center; font-weight:700; font-size:3.2rem; color:black;'>{st.session_state.departure_code}</div>",
            unsafe_allow_html=True
        )
    with c2:
        st.markdown("<div style='text-align:center; font-size:3.8rem;'>→</div>", unsafe_allow_html=True)
    with c3:
        arr_placeholder = st.empty()
        arr_placeholder.markdown(
            f"<div style='text-align:center; font-weight:700; font-size:3.2rem; color:black;'>{st.session_state.arrival_code}</div>",
            unsafe_allow_html=True
        )

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        dep_name_placeholder = st.empty()
        dep_name_placeholder.markdown("**Départ :** —")
    with c2:
        arr_name_placeholder = st.empty()
        arr_name_placeholder.markdown("**Arrivée :** —")
    with c3:
        airline_placeholder = st.empty()
        airline_placeholder.markdown("**Compagnie :** —")
    with c4:
        duration_placeholder = st.empty()
        duration_placeholder.markdown("**Durée estimée :** —")


    st.markdown("---")

    # ==============================================================
    # PREDICTION STEP 
    # ==============================================================
    st.markdown("<h4 style='text-align:left; margin-bottom:2rem;'>🧭 Étapes de prédiction</h4>", unsafe_allow_html=True)

    col_init, col_data, col_api = st.columns(3)

    with col_init:
        st.markdown("<div style='font-weight:600; text-align:center;'>⚙️ Initialisation</div>", unsafe_allow_html=True)
        init_connexion_api_status = st.empty()
        init_connexion_api_status.markdown("<p class='status-text'>🕓 Connexion à l’API",unsafe_allow_html=True)

    with col_data:
        st.markdown("<div style='font-weight:600; text-align:center;'>📥 Récupération des données</div>", unsafe_allow_html=True)
        data_scrap_status = st.empty()
        data_extract_gps_status = st.empty()
        data_extract_airports_status = st.empty()
        data_extract_airline_status = st.empty()
        data_extract_meteodep_status = st.empty()
        data_extract_meteoarr_status = st.empty()
        data_extract_flighttime_status = st.empty()
        data_extract_prevdelay_status = st.empty()
        data_prep_status = st.empty()

        data_scrap_status.markdown("<p class='status-text'>🕓 Scraping des données de vol (FR24)", unsafe_allow_html=True)
        data_extract_gps_status.markdown("<p class='status-text'>🕓 Extraction coordonnées GPS (OurAirports)",unsafe_allow_html=True)
        data_extract_airports_status.markdown("<p class='status-text'>🕓 Extraction notes aéroports (AirHelp)",unsafe_allow_html=True)
        data_extract_airline_status.markdown("<p class='status-text'>🕓 Extraction note compagnie (AirHelp)",unsafe_allow_html=True)
        data_extract_meteodep_status.markdown("<p class='status-text'>🕓 Météo départ (Open-Meteo)",unsafe_allow_html=True)
        data_extract_meteoarr_status.markdown("<p class='status-text'>🕓 Météo arrivée (Open-Meteo)",unsafe_allow_html=True)
        data_extract_flighttime_status.markdown("<p class='status-text'>🕓 Calcul temps de vol",unsafe_allow_html=True)
        data_extract_prevdelay_status.markdown("<p class='status-text'>🕓 Retard vol précédent",unsafe_allow_html=True)
        data_prep_status.markdown("<p class='status-text'>🕓 Préparation des données",unsafe_allow_html=True)

    with col_api:
        st.markdown("<div style='font-weight:600; text-align:center;'>🖥️ API de prédiction</div>",unsafe_allow_html=True)
        api_prep_status = st.empty()
        api_prediction_status = st.empty()

        api_prep_status.markdown("<p class='status-text'>🕓 Préparation finale des données",unsafe_allow_html=True)
        api_prediction_status.markdown("<p class='status-text'>🕓 Prédiction",unsafe_allow_html=True)

    progress_bar = st.progress(0)

    # ==============================================================
    # CALL API 
    # ==============================================================
    with st.spinner("Prédiction en cours..."):
        try:
            with requests.post(
                API_URL,
                json={"flight_number": flight_number, "flight_date": flight_date},
                stream=True
            ) as response:

                if response.status_code == 200:
                    steps_done = 0
                    total_steps = 11  # ajuste selon ton API

                    for line in response.iter_lines():
                        if not line:
                            continue
                        try:
                            data = json.loads(line.decode("utf-8"))
                        except json.JSONDecodeError:
                            continue

                        step = data.get("step")

                        if step == "connexion_api":
                            init_connexion_api_status.markdown("<p class='status-text'>✅ Connexion à l’API",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "scraping_fr24":
                            data_scrap_status.markdown("<p class='status-text'>✅ Scraping des données de vol (FR24)",unsafe_allow_html=True)
                            steps_done += 1

                            dep_code = data.get("DS_DEPARTURE_AIRPORT_CODE")
                            arr_code = data.get("DS_ARRIVAL_AIRPORT_CODE")
                            airline_code = data.get("DS_AIRLINE_CODE")
                            dep_name = data.get("DS_DEPARTURE_AIRPORT")
                            arr_name = data.get("DS_ARRIVAL_AIRPORT")

                            if dep_code:
                                st.session_state.departure_code = dep_code
                            if arr_code:
                                st.session_state.arrival_code = arr_code

                            if dep_name:
                                dep_name_placeholder.markdown(f"**Départ :** {dep_name} ({dep_code})")
                            if arr_name:
                                arr_name_placeholder.markdown(f"**Arrivée :** {arr_name} ({arr_code})")
                            if airline_code:
                                airline_placeholder.markdown(f"**Compagnie :** {airline_code}")

                            # Mise à jour directe
                            dep_placeholder.markdown(
                                f"<div style='text-align:center; font-weight:700; font-size:3.2rem; color:black;'>{st.session_state.departure_code}</div>",
                                unsafe_allow_html=True
                            )
                            arr_placeholder.markdown(
                                f"<div style='text-align:center; font-weight:700; font-size:3.2rem; color:black;'>{st.session_state.arrival_code}</div>",
                                unsafe_allow_html=True
                            )

                        elif step == "extract_gps":
                            data_extract_gps_status.markdown("<p class='status-text'>✅ Extraction coordonnées GPS",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "extract_airports":
                            data_extract_airports_status.markdown("<p class='status-text'>✅ Notes aéroports importées",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "extract_airline":
                            data_extract_airline_status.markdown("<p class='status-text'>✅ Notes compagnie importées",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "meteo_dep":
                            data_extract_meteodep_status.markdown("<p class='status-text'>✅ Météo départ récupérée",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "meteo_arr":
                            data_extract_meteoarr_status.markdown("<p class='status-text'>✅ Météo arrivée récupérée",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "calc_flighttime":
                            data_extract_flighttime_status.markdown("<p class='status-text'>✅ Temps de vol calculé",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "calc_prevdelay":
                            data_extract_prevdelay_status.markdown("<p class='status-text'>✅ Retard précédent calculé",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "data_prep":
                            data_prep_status.markdown("<p class='status-text'>✅ Données préparées",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "api_preparation":
                            api_prep_status.markdown("<p class='status-text'>✅ Données prêtes pour la prédiction",unsafe_allow_html=True)
                            steps_done += 1

                        elif step == "prediction":
                            api_prediction_status.markdown("<p class='status-text'>✅ Prédiction effectuée",unsafe_allow_html=True)
                            steps_done += 1
                            delay = data.get("predicted_delay_min", None)
                            if delay is not None:
                                st.success(f"🕒 Retard estimé : **{delay:.1f} minutes** pour le vol {flight_number}")

                        # 🔄 Progression
                        progress_bar.progress(min(int((steps_done / total_steps) * 100), 100))
                        time.sleep(0.2)

                else:
                    st.error(f"Erreur API ({response.status_code}) : {response.text}")

        except Exception as e:
            st.error(f"Erreur de connexion à l'API : {e}")
