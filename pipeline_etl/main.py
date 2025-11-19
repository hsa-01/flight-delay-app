import requests
import pandas as pd
import time
import random
import json
from urllib.parse import urljoin
import logging
import numpy as np
import os
from bs4 import BeautifulSoup
from datetime import datetime

from fonc_delay import delay 
from fonc_airport_coordinate import airport_coordinate
from fonc_airport_rating import airport_rating
from fonc_airline_rating import airline_rating
from fonc_weather import weather_dep_temp, weather_dep_vis, weather_dep_wind, weather_dep_rain
from fonc_weather import weather_arr_temp, weather_arr_vis, weather_arr_wind, weather_arr_rain
from fonc_prev_delay import prev_delay



#=====================================================================
# HEADER COMMENT BLOCK
#=====================================================================
'''
This file is main.py of the ETL pipeline. Its purpose is to collect and consolidate flight data from multiple sources into a unified dataset.

-Extract : 
    * Main flight data => From external source (scraping) : Flightradar24
    * Airports coordinates => From internal source (csv) : OurAirports
    * Airports ponctuality ratings => From internal source (csv): AirHelp
    * Airline ponctuality rating => From internal source (csv): AirHelp
    * Airports weather data => From external source (API) : OpenMeteo
-Transform :
    * Data filtering
    * Columns renaming
    * Colums creation
-Load :
    * Local saving
'''

#=====================================================================
# CONFIGURATION SCRAPPING
#=====================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#=====================================================================
# SCRAPPING FUNCTIONS
#=====================================================================

class SimpleFlightScraper:
    def __init__(self):
        self.session = requests.Session()
        # Headers to look like a browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    
    def scrape_flight_data(self, url):
        """
        Try to scrap flight data with requests/BeautifulSoup
        """
        try:
            logger.info(f"Récupération de la page: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Search for tabs
            tables = soup.find_all('table')
            if tables:
                logger.info(f"Trouvé {len(tables)} tableau(x)")
                return self._extract_from_tables(tables)
            
            # Search for JSON data
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and ('flight' in script.string.lower() or 'af01' in script.string.lower()):
                    json_data = self._extract_json_from_script(script.string)
                    if json_data:
                        return json_data
            
            # Search for elements with classes linked to flight
            flight_elements = soup.find_all(['div', 'tr', 'li'], class_=lambda x: x and 'flight' in str(x).lower())
            if flight_elements:
                logger.info(f"Trouvé {len(flight_elements)} éléments de vol")
                return self._extract_from_elements(flight_elements)
            
            logger.warning("Aucune donnée trouvée avec BeautifulSoup")
            return None
            
        except Exception as e:
            logger.error(f"Erreur lors du scraping simple: {e}")
            return None
    
    def _extract_from_tables(self, tables):
        """
        Extract data from HTML tabs
        """
        all_data = []
        
        for i, table in enumerate(tables):
            logger.info(f"Traitement du tableau {i+1}")
            
            # Search for heads 
            headers = []
            header_row = table.find('thead') or table.find('tr')
            if header_row:
                th_elements = header_row.find_all(['th', 'td'])
                headers = [th.get_text(strip=True) for th in th_elements]
            
            # Data extraction
            rows = table.find_all('tr')[1:] if headers else table.find_all('tr')
            
            table_data = []
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if cells:
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    if headers and len(row_data) == len(headers):
                        table_data.append(dict(zip(headers, row_data)))
                    elif row_data:
                        table_data.append({f'Colonne_{j+1}': val for j, val in enumerate(row_data)})
            
            if table_data:
                all_data.extend(table_data)
        
        return all_data if all_data else None
    
    def _extract_from_elements(self, elements):
        """
        Extract data from HTML generic
        """
        data = []
        
        for element in elements:
            element_data = {
                'text': element.get_text(strip=True),
                'class': ' '.join(element.get('class', [])),
                'tag': element.name
            }
            
            # Recherche d'attributs data-*
            for attr, value in element.attrs.items():
                if attr.startswith('data-'):
                    element_data[attr] = value
            
            data.append(element_data)
        
        return data
    
    def _extract_json_from_script(self, script_content):
        """
        Try to extract JSON data from scripts
        """
        try:
            # Recherche de patterns JSON courants
            import re
            
            # Pattern pour les objets JSON
            json_patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                r'window\.flightData\s*=\s*({.*?});',
                r'data\s*:\s*({.*?})',
                r'flights\s*:\s*(\[.*?\])'
            ]
            
            for pattern in json_patterns:
                matches = re.search(pattern, script_content, re.DOTALL)
                if matches:
                    try:
                        json_str = matches.group(1)
                        json_data = json.loads(json_str)
                        logger.info("Données JSON trouvées dans le script")
                        return self._flatten_json_data(json_data)
                    except json.JSONDecodeError:
                        continue
            
        except Exception as e:
            logger.debug(f"Erreur lors de l'extraction JSON: {e}")
        
        return None
    
    def _flatten_json_data(self, json_data):
        """
        Flatten JSON data to create a tab
        """
        if isinstance(json_data, list):
            return json_data
        elif isinstance(json_data, dict):
            # Recherche de listes dans le dictionnaire
            for key, value in json_data.items():
                if isinstance(value, list) and value:
                    return value
            # Si pas de liste, retourner le dict comme une seule ligne
            return [json_data]
        return None
    
#=====================================================================
# MAIN (Pipeline ETL execution)
#=====================================================================


def main():
    """
    Main to test different methods
    """

    
    # CSV LOADING : Flight code list 
    df_flight_codes_list=pd.read_csv('Data/Flight-delay_flight-code.csv', sep=";")

    # FLIGHT CODE COLUMN EXTRACT
    df_flight_code = df_flight_codes_list["flight_code"].dropna().tolist() # Without NAN

    # MAIN LOOP : To inject flight code to access to flight data on fligthradar24 website
    scraper_log = SimpleFlightScraper()
    

    for code in df_flight_code:

        try:

            #--------------------
            # EXTRACT 1 : Initialisation of the data scraping, and extraction of main data flight * (for each new loop)
            # Main data flight * : Departure/arrival airports,hours scheduled/real, airline code, aircraft registration code
            #--------------------
            
            # SCRAPING SOURCE : Injection of the flight code in the following url for data extraction
            url = f"https://www.flightradar24.com/data/flights/{code}"
        
            print("=== Tentative avec l'approche simple (requests/BeautifulSoup) ===")
            simple_scraper = SimpleFlightScraper()
            simple_data = simple_scraper.scrape_flight_data(url)
            time.sleep(random.uniform(3, 7)) # Break to avoid blocking or error 429



            if simple_data:
                #--------------------
                # TRANSFORM 1 : Dataframe creation
                #--------------------
                df_data_prov = pd.DataFrame(simple_data)


                #--------------------
                # TRANSFORM 2 : Dataframe main transformation and formatting
                #--------------------

                # TRANSFORM 2a: Useless columns removing 
                columns_to_remove = ['FLIGHTS HISTORY','', 'Colonne_1', 'Colonne_2','Colonne_3'] 
                df_data_prov = df_data_prov.drop(columns=columns_to_remove, errors='ignore')

                # TRANSFORM 2b : Useless rows removing (Scheduled flight wituout data)
                df_data_prov = df_data_prov[df_data_prov['STATUS'].str.contains('Landed|Diverted', na=False)]
                df_data_prov = df_data_prov[df_data_prov['FLIGHT TIME'] != '—']

                # TRANSFORM 2c : Existing columns renaming
                df_data_prov = df_data_prov.rename(columns={'FROM': 'ds_departure_airport'})
                df_data_prov = df_data_prov.rename(columns={'TO': 'ds_arrival_airport'})
                df_data_prov = df_data_prov.rename(columns={'STD': 'ds_departure_plan'})
                df_data_prov = df_data_prov.rename(columns={'STA': 'ds_arrival_plan'})
                df_data_prov = df_data_prov.rename(columns={'ATD': 'ds_departure_real'})
                df_data_prov = df_data_prov.rename(columns={'STATUS': 'ds_flight_status'})
                df_data_prov = df_data_prov.rename(columns={'FLIGHT TIME': 'ds_flight_duration'})
                df_data_prov = df_data_prov.rename(columns={'DATE': 'ds_flight_date'})
                df_data_prov = df_data_prov.rename(columns={'AIRCRAFT': 'ds_flight_aircraft'})

                # TRANSFORM 2d : Spliting names and codes of departure & arrivals airports
                # Columns creation for airport codes (copy to then split)
                df_data_prov['ds_departure_airport_code']=df_data_prov["ds_departure_airport"]
                df_data_prov['ds_arrival_airport_code']=df_data_prov["ds_arrival_airport"]
                # Extraction of airports codes aeroport
                df_data_prov['ds_departure_airport_code'] = df_data_prov['ds_departure_airport_code'].str.strip()
                df_data_prov['ds_departure_airport_code'] = df_data_prov['ds_departure_airport_code'].str.extract(r'\((.*?)\)')
                df_data_prov['ds_arrival_airport_code'] = df_data_prov['ds_arrival_airport_code'].str.strip()
                df_data_prov['ds_arrival_airport_code'] = df_data_prov['ds_arrival_airport_code'].str.extract(r'\((.*?)\)')
                # Extraction of airports cities names 
                df_data_prov['ds_departure_airport'] = df_data_prov['ds_departure_airport'].str.strip()
                df_data_prov['ds_departure_airport'] = df_data_prov['ds_departure_airport'].str.split('(').str[0] 
                df_data_prov['ds_arrival_airport'] = df_data_prov['ds_arrival_airport'].str.strip()
                df_data_prov['ds_arrival_airport'] = df_data_prov['ds_arrival_airport'].str.split('(').str[0] 

                # TRANSOFORM 2e : Splitting of plane type and plane registration data
                df_data_prov['ds_flight_aircraft'] = df_data_prov['ds_flight_aircraft'].str.strip()
                df_data_prov['ds_flight_aircraft'] = df_data_prov['ds_flight_aircraft'].str.extract(r'\((.*?)\)')

                # TRANSFORM 2f : Splitting status and langing time data
                df_data_prov['ds_arrival_real'] = df_data_prov['ds_flight_status']
                df_data_prov['ds_arrival_real'] = df_data_prov['ds_arrival_real'].str.strip() # Removing spaces before and after
                df_data_prov['ds_arrival_real'] = df_data_prov['ds_arrival_real'].str.extract(r'(\d{2}:\d{2})') # Using regex expression reguliere
                df_data_prov['ds_flight_status'] = df_data_prov['ds_flight_status'].str.strip()
                df_data_prov['ds_flight_status'] = df_data_prov['ds_flight_status'].str.split(' ').str[0] # Keep first word
            
                # TRANSFORM 2g: Date format processing (conversion type str)
                df_data_prov["ds_flight_date"] = pd.to_datetime(df_data_prov["ds_flight_date"], format="%d %b %Y")
                df_data_prov["ds_flight_date"] = df_data_prov["ds_flight_date"].dt.strftime("%d/%m/%y") # Data format update en dd/mm/yy
                df_data_prov['ds_flight_date'] = df_data_prov['ds_flight_date'].astype("string")

                # TRANSOFM 2h : Creation of other new columns for next data extractions
                df_data_prov['ds_airline_rating']=np.nan
                df_data_prov['ds_final_delay_min']=np.nan
                df_data_prov['ds_arrival_airport_lat']=np.nan
                df_data_prov['ds_arrival_airport_long']=np.nan
                df_data_prov['ds_departure_airport_lat']=np.nan
                df_data_prov['ds_departure_airport_long']=np.nan
                df_data_prov['ds_departure_airport_rating']=np.nan
                df_data_prov['ds_arrival_airport_rating']=np.nan
                df_data_prov['ds_departure_airport_temp_cel']=np.nan
                df_data_prov['ds_departure_airport_rain_mmHour']=np.nan
                df_data_prov['ds_departure_airport_wind_kmh']=np.nan
                df_data_prov['ds_departure_airport_vis_km']=np.nan
                df_data_prov['ds_arrival_airport_temp_cel']=np.nan
                df_data_prov['ds_arrival_airport_rain_mmHour']=np.nan
                df_data_prov['ds_arrival_airport_wind_kmh']=np.nan
                df_data_prov['ds_arrival_airport_vis_km']=np.nan
                df_data_prov['ds_prev_delay_min']=np.nan

                # TRANSOFM 2i : Upper letters
                df_data_prov['ds_flight_code']=code.upper()


                #--------------------
                # TRANSFORM 3 : Final flight delay calculation (for each flight listed)
                #--------------------     
                df_data_prov['ds_final_delay_min'] = df_data_prov.apply(lambda row: delay(row['ds_flight_date'],
                                                                                row['ds_departure_plan'],
                                                                                row['ds_departure_real'],
                                                                                row['ds_flight_duration'],
                                                                                row['ds_arrival_plan']),axis=1)
                

                #--------------------
                # TRANSFORM 4 : Previous flight delay calculation (for each flight listed)
                #--------------------   
                time.sleep(random.uniform(6, 9)) # Break to avoid blocking or error 429
                df_data_prov['ds_prev_delay_min'] = df_data_prov.apply(lambda row: prev_delay(row['ds_flight_aircraft'],
                                                                                row['ds_flight_date'],
                                                                                row['ds_departure_airport_code'],
                                                                                row['ds_flight_code'],
                                                                                row['ds_flight_duration']),axis=1)
                

                #--------------------
                # EXTRACT 2 : Extraction of airports coordinates (from csv)
                #--------------------     
                # CSV LOADING :  Airports coordinates
                airport_coord_csv = pd.read_csv("Data/Flight-delay_airports-general-data.csv") 
                # DATASET ENRICHMENT : For arrival and departure airports 
                df_data_prov = airport_coordinate(df_data_prov, airport_coord_csv)  


                #--------------------
                # EXTRACT 3 : Extraction of airports poncutality rating (from csv)
                #--------------------    
                airport_rating_csv = pd.read_csv("Data/Flight-delay_airports-ratings.csv", encoding="latin-1", sep=";" ) 
                df_data_prov = airport_rating(df_data_prov, airport_rating_csv)


                #--------------------
                # EXTRACT 4 : Extraction of airlines poncutality rating (from csv)
                #--------------------    
                airline_rating_csv = pd.read_csv("Data/Flight-delay_airlines-ratings.csv", encoding="latin-1", sep=";" ) 
                df_data_prov = airline_rating(df_data_prov, airline_rating_csv)


                #--------------------
                # EXTRACT 5 : Extraction of airports weather data (from api) ==> Linked to latitude & longitude airports extraction (TRANSFORM 5)
                #--------------------  
                time.sleep(random.uniform(3, 7)) # Break to avoid blocking or error 429
                df_data_prov['ds_departure_airport_temp_cel'] = df_data_prov.apply(lambda row: weather_dep_temp(row['ds_flight_date'],
                                                                                row['ds_departure_plan'],
                                                                                row['ds_departure_airport_lat'],
                                                                                row['ds_departure_airport_long']),axis=1)
                
                time.sleep(random.uniform(3, 7)) # Break to avoid blocking or error 429
                df_data_prov['ds_departure_airport_vis_km'] = df_data_prov.apply(lambda row: weather_dep_vis(row['ds_flight_date'],
                                                                                row['ds_departure_plan'],
                                                                                row['ds_departure_airport_lat'],
                                                                                row['ds_departure_airport_long']),axis=1)
            
                time.sleep(random.uniform(3, 7)) # Break to avoid blocking or error 429
                df_data_prov['ds_departure_airport_wind_kmh'] = df_data_prov.apply(lambda row: weather_dep_wind(row['ds_flight_date'],
                                                                                row['ds_departure_plan'],
                                                                                row['ds_departure_airport_lat'],
                                                                                row['ds_departure_airport_long']),axis=1)
            
                time.sleep(random.uniform(3, 7)) 
                df_data_prov['ds_departure_airport_rain_mmHour'] = df_data_prov.apply(lambda row: weather_dep_rain(row['ds_flight_date'],
                                                                                row['ds_departure_plan'],
                                                                                row['ds_departure_airport_lat'],
                                                                                row['ds_departure_airport_long']),axis=1)
            
                time.sleep(random.uniform(3, 7)) # Break to avoid blocking or error 429
                df_data_prov['ds_arrival_airport_temp_cel'] = df_data_prov.apply(lambda row: weather_arr_temp(row['ds_flight_date'],
                                                                                row['ds_arrival_plan'],
                                                                                row['ds_arrival_airport_lat'],
                                                                                row['ds_arrival_airport_long']),axis=1)
            
                time.sleep(random.uniform(3, 7))  # Break to avoid blocking or error 429
                df_data_prov['ds_arrival_airport_vis_km'] = df_data_prov.apply(lambda row: weather_arr_vis(row['ds_flight_date'],
                                                                                row['ds_arrival_plan'],
                                                                                row['ds_arrival_airport_lat'],
                                                                                row['ds_arrival_airport_long']),axis=1)
            
                time.sleep(random.uniform(3, 7)) # Break to avoid blocking or error 429
                df_data_prov['ds_arrival_airport_wind_kmh'] = df_data_prov.apply(lambda row: weather_arr_wind(row['ds_flight_date'],
                                                                                row['ds_arrival_plan'],
                                                                                row['ds_arrival_airport_lat'],
                                                                                row['ds_arrival_airport_long']),axis=1)
            
                time.sleep(random.uniform(3, 7)) # Break to avoid blocking or error 429
                df_data_prov['ds_arrival_airport_rain_mmHour'] = df_data_prov.apply(lambda row: weather_arr_rain(row['ds_flight_date'],
                                                                                row['ds_arrival_plan'],
                                                                                row['ds_arrival_airport_lat'],
                                                                                row['ds_arrival_airport_long']),axis=1)
                

                #--------------------
                # LOAD : Local save
                #-------------------- 

                # COLUMNS ORDER DEFINITION
                columns_order = ["ds_flight_code","ds_airline_code","ds_airline_rating","ds_flight_date","ds_flight_aircraft","ds_departure_airport","ds_arrival_airport","ds_departure_airport_code",
                                "ds_arrival_airport_code","ds_flight_duration","ds_departure_plan","ds_departure_real","ds_arrival_plan","ds_arrival_real",
                                "ds_departure_airport_rating","ds_arrival_airport_rating","ds_departure_airport_lat","ds_departure_airport_long",
                                "ds_arrival_airport_lat","ds_arrival_airport_long","ds_departure_airport_temp_cel","ds_departure_airport_rain_mmHour",
                                "ds_departure_airport_wind_kmh","ds_departure_airport_vis_km","ds_arrival_airport_temp_cel","ds_arrival_airport_rain_mmHour",
                                "ds_arrival_airport_wind_kmh","ds_arrival_airport_vis_km","ds_flight_status","ds_prev_delay_min","ds_final_delay_min"]
            
                # COLUMNS ORDER APPLICATION  
                df_data_prov = df_data_prov[columns_order]

                # COPY : From df_data_prov_save
                df_data_prov_save = df_data_prov.copy()

                # SAVING : Final dataset conversion to csv (using append to not write on existing data, without touching columns titles)
                df_data_prov_save.to_csv("Flight-delay_dataset-save.csv", mode='a', index=False, header=False, sep=';')


        

                #--------------------
                # DATA DISPLAY
                #--------------------  

                print(f"Données extraites avec l'approche simple: {len(df_data_prov)} lignes")
                print(df_data_prov.head())


                # LOG : Row counter
                if os.path.exists("Flight-delay_dataset-save.csv"):
                    df_existing = pd.read_csv("Flight-delay_dataset-save.csv", sep=';')
                    num_rows = len(df_existing)
                else:
                    num_rows = 0
                
                print(f"🌐 Nombre de lignes collectés dans le CSV : {num_rows}")

            else:
                print("L'approche simple n'a pas fonctionné. Le site utilise probablement JavaScript.")
                print("Utilisez le script Selenium principal pour des résultats fiables.")
            
        except Exception as e:
            print(f"Erreur dans la boucle for du def main(): {e}")
            continue
    



if __name__ == "__main__":
    print("Dépendances requises:")
    print("pip install requests beautifulsoup4 pandas lxml")
    print("\n" + "="*60)
    main()
