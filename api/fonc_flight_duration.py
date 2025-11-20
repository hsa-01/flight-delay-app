import math

def flight_duration(lat1, lon1, lat2, lon2):
    """
    PURPOSE : 
        Calculation to estimate a flight duration.
        When the user select a flight, the flight duration data is not available (because the plane did not took-off yet). 
        This fonction ensures to give an estimation of the flight duration to complete the input data necessary for the API to give its prediction.
        For this, the distance before departure and arrival is considered, with the following assumptions : 
        - < 800 km  → 500 km/h average speed
        - 800–2000 km → 600 km/h average speed
        - 2000–4000 km → 700 km/h average speed
        - > 4000 km → 800 km/h average speed
    ARGS:
        lat1 (float) : lattitude of departure airport
        lon1 (float) : longitude of departure airport
        lat2 (float) : lattitude of arrival airport
        lon2 (float) : longitude of arrival airport
    RETURNS:
        float: Flight duration in minutes
    """

    # DISTANCE : Calculation distance (Haversine formula)
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance_km = R * c

    # SPEED SELECTION
    if distance_km < 800:
        speed_kmh = 500
    elif distance_km < 2000:
        speed_kmh = 750
    elif distance_km < 3000:
        speed_kmh = 800
    else:
        speed_kmh = 850

    # FINAL CALCULATION
    duration_hr = distance_km / speed_kmh
    duration_min = duration_hr * 60

    return duration_min


