import openmeteo_requests
import pandas as pd
from datetime import datetime
import numpy as np



#=============================
# PART 1 : WEATHER DATA of DEPARTURE AIRPORT
#=============================

def weather_dep_temp(ds_flight_date, ds_departure_plan, ds_departure_airport_lat, ds_departure_airport_long):
    """
    Extract temperature in °C from Open-Meteo API
    
    Args:
        ds_flight_date (str): Date au format dd/mm/yy
        ds_departure_plan (str): Heure au format hh:mm
        ds_departure_airport_lat (float): Latitude
        ds_departure_long (float): Longitude
    
    Returns:
        float: Température en °C ou None si erreur
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_departure_plan) or pd.isna(ds_departure_airport_lat) or pd.isna(ds_departure_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_departure_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical 
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["temperature_2m"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "hourly": ["temperature_2m"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "temperature": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            return round(target.iloc[0]['temperature'], 1)
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_departure_plan}: {e}")
        return None


def weather_dep_vis(ds_flight_date, ds_departure_plan, ds_departure_airport_lat, ds_departure_airport_long):
    """
    Extract visibility in km from Open-meteo API
    
    Args:
        ds_flight_date (str): Date au format dd/mm/yy
        ds_departure_plan (str): Heure au format hh:mm
        ds_departure_airport_lat (float): Latitude
        ds_departure_long (float): Longitude
    
    Returns:
        float: Visibilité en km ou None si erreur
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_departure_plan) or pd.isna(ds_departure_airport_lat) or pd.isna(ds_departure_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_departure_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["visibility"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "hourly": ["visibility"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "visibility": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            visibility_m = target.iloc[0]['visibility']
            if pd.notna(visibility_m) and visibility_m > 0:
                return round(visibility_m / 1000, 1)  # Conversion mètres -> kilomètres
            else:
                return None
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_departure_plan}: {e}")
        return None


def weather_dep_wind(ds_flight_date, ds_departure_plan, ds_departure_airport_lat, ds_departure_airport_long):
    """
    Extract wind in km/h from Open-meteo API
    
    Args:
        ds_flight_date (str): Date au format dd/mm/yy
        ds_departure_plan (str): Heure au format hh:mm
        ds_departure_airport_lat (float): Latitude
        ds_departure_long (float): Longitude
    
    Returns:
        float: Vitesse du vent en km/h ou None si erreur
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_departure_plan) or pd.isna(ds_departure_airport_lat) or pd.isna(ds_departure_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_departure_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["wind_speed_10m"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "hourly": ["wind_speed_10m"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "wind_speed": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            return round(target.iloc[0]['wind_speed'], 1)
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_departure_plan}: {e}")
        return None


def weather_dep_rain(ds_flight_date, ds_departure_plan, ds_departure_airport_lat, ds_departure_airport_long):
    """
    Extract rain level in mm/hour from Open-meteo API
    
    Args:
        ds_flight_date (str): Date au format dd/mm/yy
        ds_departure_plan (str): Heure au format hh:mm
        ds_departure_airport_lat (float): Latitude
        ds_departure_long (float): Longitude
    
    Returns:
        float: Niveau de précipitations en mm ou None si erreur
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_departure_plan) or pd.isna(ds_departure_airport_lat) or pd.isna(ds_departure_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_departure_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["precipitation"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_departure_airport_lat),
                "longitude": float(ds_departure_airport_long),
                "hourly": ["precipitation"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "precipitation": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            return round(target.iloc[0]['precipitation'], 1)
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_departure_plan}: {e}")
        return None



#=============================
# PART 2 : WEATHER DATA of ARRIVAL AIRPORT
#=============================

def weather_arr_temp(ds_flight_date, ds_arrival_plan, ds_arrival_airport_lat, ds_arrival_airport_long):
    """
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_arrival_plan) or pd.isna(ds_arrival_airport_lat) or pd.isna(ds_arrival_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_arrival_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["temperature_2m"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "hourly": ["temperature_2m"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "temperature": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            return round(target.iloc[0]['temperature'], 1)
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_arrival_plan}: {e}")
        return None


def weather_arr_vis(ds_flight_date, ds_arrival_plan, ds_arrival_airport_lat, ds_arrival_airport_long):
    """
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_arrival_plan) or pd.isna(ds_arrival_airport_lat) or pd.isna(ds_arrival_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_arrival_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["visibility"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "hourly": ["visibility"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "visibility": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            visibility_m = target.iloc[0]['visibility']
            if pd.notna(visibility_m) and visibility_m > 0:
                return round(visibility_m / 1000, 1)  # Conversion mètres -> kilomètres
            else:
                return None
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_arrival_plan}: {e}")
        return None


def weather_arr_wind(ds_flight_date, ds_arrival_plan, ds_arrival_airport_lat, ds_arrival_airport_long):
    """
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_arrival_plan) or pd.isna(ds_arrival_airport_lat) or pd.isna(ds_arrival_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_arrival_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["wind_speed_10m"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "hourly": ["wind_speed_10m"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "wind_speed": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            return round(target.iloc[0]['wind_speed'], 1)
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_arrival_plan}: {e}")
        return None


def weather_arr_rain(ds_flight_date, ds_arrival_plan, ds_arrival_airport_lat, ds_arrival_airport_long):
    """
    """
    try:
        # VALIDATION : Of input data
        if pd.isna(ds_flight_date) or pd.isna(ds_arrival_plan) or pd.isna(ds_arrival_airport_lat) or pd.isna(ds_arrival_airport_long):
            return None
        
        # CONVERSION :  Of date format (dd/mm/yy to yyyy-mm-dd)
        date_obj = datetime.strptime(f"{ds_flight_date} {ds_arrival_plan}", "%d/%m/%y %H:%M")
        date_only = date_obj.strftime("%Y-%m-%d")
        hour = date_obj.hour
        
        # CLIENT API
        openmeteo = openmeteo_requests.Client()
        
        # URL : Definition of the URL depending of the historic date
        days_diff = (datetime.now() - date_obj).days
        
        if days_diff > 92:
            # DATA : Historical
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "start_date": date_only,
                "end_date": date_only,
                "hourly": ["precipitation"],
                "timezone": "auto"
            }
        else:
            # DATA : Historical
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": float(ds_arrival_airport_lat),
                "longitude": float(ds_arrival_airport_long),
                "hourly": ["precipitation"],
                "past_days": min(days_diff + 1, 92),
                "timezone": "auto"
            }
        
        # API REQUEST
        response = openmeteo.weather_api(url, params=params)[0]
        hourly = response.Hourly()
        
        # DATAFRAME : Creation of temporary dataset
        df_temp = pd.DataFrame({
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "precipitation": hourly.Variables(0).ValuesAsNumpy()
        })
        
        # FILTERING : Per exact hour
        target = df_temp[
            (df_temp['date'].dt.hour == hour) & 
            (df_temp['date'].dt.date == date_obj.date())
        ]
        
        if not target.empty:
            return round(target.iloc[0]['precipitation'], 1)
        else:
            return None
            
    except Exception as e:
        print(f"Erreur pour {ds_flight_date} {ds_arrival_plan}: {e}")
        return None

    