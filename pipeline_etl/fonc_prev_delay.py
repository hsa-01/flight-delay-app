import pandas as pd
from bs4 import BeautifulSoup
import time
import random
import json
from urllib.parse import urljoin
import logging
import numpy as np
from datetime import datetime, timedelta
import requests


def prev_delay(ds_flight_aircraft,ds_flight_date,ds_departure_airport_code,ds_flight_code,ds_flight_duration):
    '''
    PURPOSE : 
        This file is a function called by the main.py file. Its purpose is to calculate the delay in minutes of the previous flight (than the one selected).
        It is composed by a pipeline block to collect data of the airfract which operated the selected flight, to reach its history whose the previous flight.
        Features overview : 
        -Extract : 
        * Main flight data => From external source (scraping) : Flightradar24
        -Transform :
        * Data filtering
        * Columns renaming
        * Colums creation
        - Calculation :
        * Mask : Application of filter to select the previous flight, than the one targeted one
        * Calculation : Application of a function to calculate the delay of the previous flight
    ARGS:
        ds_flight_aircraft (str) : Airfract registration code
        ds_flight_date (str) : Target flight date
        ds_departure_airport_code (str) : IATA departure airport code 
        ds_flight_code (str) : IATA flight code
        ds_flight_duration (str) : Target flight duration (hh:mm)
    RETURNS:
        float: Previous flight delay in minutes     
    '''

    try:
     
    
        #=====================================================================
        # CONFIGURATION SCRAPING
        #=====================================================================


        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)


        #=====================================================================
        # SCRAPING FUNCTIONS
        #=====================================================================


        class SimpleFlightScraper:
            def __init__(self):
                self.session = requests.Session()
                # Headers pour ressembler à un navigateur normal
                self.session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                })



            def scrape_flight_data(self, url):
                """Try to scrap flight data with requests/BeautifulSoup"""
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
                        if script.string and ('flight' in script.string.lower() or 'tu628' in script.string.lower()):
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
                """Extract data from HTML tabs"""
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
                """Extract data from HTML generic"""
                data = []
        
                for element in elements:
                    element_data = {
                        'text': element.get_text(strip=True),
                        'class': ' '.join(element.get('class', [])),
                        'tag': element.name
                    }
            
                    # Search for attribute data-*
                    for attr, value in element.attrs.items():
                        if attr.startswith('data-'):
                            element_data[attr] = value
            
                    data.append(element_data)
        
                return data


            def _extract_json_from_script(self, script_content):
                """Try to extract JSON data from scripts"""
                try:
                    # Search for JSON pattern
                    import re
            
                    # JSON pattern
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
                """Flatten JSON data to create a tab"""
                if isinstance(json_data, list):
                    return json_data
                elif isinstance(json_data, dict):
                    # Search for list in dict
                    for key, value in json_data.items():
                        if isinstance(value, list) and value:
                            return value
                    # If no list, return dict as one line
                    return [json_data]
                return None


                

        #=====================================================================
        # MAIN (Pipeline ETL execution)
        #=====================================================================



        def main():
            """Main to test different methods"""

            # SCRAPING SOURCE : Injection of the flight code in the following url for data extraction
            simple_scraper = SimpleFlightScraper()
            time.sleep(random.uniform(1, 2))  # Break to avoid blocking or error 429

            url = f"https://www.flightradar24.com/data/aircraft/{ds_flight_aircraft}"
            print(f"=== Scraping des données du vol {ds_flight_aircraft} ===")
        
            simple_data = simple_scraper.scrape_flight_data(url)  
            time.sleep(random.uniform(3, 7))  # Break to avoid blocking or error 429


            if simple_data:
        


                    #--------------------
                    # TRANSFORM 1 : Dataframe creation
                    #--------------------
                    df_prev_delay = pd.DataFrame(simple_data)


                    #--------------------
                    # TRANSFORM 2 : Dataframe main transformation and formatting
                    #--------------------

                    # TRANSFORM 2a: Useless columns removing 
                    columns_to_remove = ['FLIGHTS HISTORY','', 'Colonne_1', 'Colonne_2','Colonne_3'] 
                    df_prev_delay = df_prev_delay.drop(columns=columns_to_remove, errors='ignore')

                    # TRANSFORM 2b : Useless rows removing (Scheduled flight wituout data)
                    #df_simple = df_simple[df_simple['STATUS'] != 'Scheduled']
                    df_prev_delay = df_prev_delay[df_prev_delay['STATUS'].str.contains('Landed|Diverted', na=False)]
                    df_prev_delay = df_prev_delay[df_prev_delay['FLIGHT TIME'] != '—']

                    # TRANSFORM 2c : Existing columns renaming
                    df_prev_delay = df_prev_delay.rename(columns={'FROM': 'dx_departure_airport'})
                    df_prev_delay = df_prev_delay.rename(columns={'TO': 'dx_arrival_airport'})
                    df_prev_delay = df_prev_delay.rename(columns={'STD': 'dx_departure_plan'})
                    df_prev_delay = df_prev_delay.rename(columns={'STA': 'dx_arrival_plan'})
                    df_prev_delay = df_prev_delay.rename(columns={'ATD': 'dx_departure_real'})
                    df_prev_delay = df_prev_delay.rename(columns={'STATUS': 'dx_flight_status'})
                    df_prev_delay = df_prev_delay.rename(columns={'FLIGHT TIME': 'dx_flight_duration'})
                    df_prev_delay = df_prev_delay.rename(columns={'DATE': 'dx_flight_date'})
                    df_prev_delay = df_prev_delay.rename(columns={'FLIGHT': 'dx_flight_code'})

                    # TRANSFORM 2d : Spliting names and codes of departure & arrivals airports
                    # Columns creation for airport codes
                    df_prev_delay['dx_departure_airport_code']=df_prev_delay["dx_departure_airport"]
                    df_prev_delay['dx_arrival_airport_code']=df_prev_delay["dx_arrival_airport"] 
                    # Extraction of airports codes aeroport
                    df_prev_delay['dx_departure_airport_code'] = df_prev_delay['dx_departure_airport_code'].str.strip()
                    df_prev_delay['dx_departure_airport_code'] = df_prev_delay['dx_departure_airport_code'].str.extract(r'\((.*?)\)')
                    df_prev_delay['dx_arrival_airport_code'] = df_prev_delay['dx_arrival_airport_code'].str.strip()
                    df_prev_delay['dx_arrival_airport_code'] = df_prev_delay['dx_arrival_airport_code'].str.extract(r'\((.*?)\)')
                    # Extraction of airports cities names 
                    df_prev_delay['dx_departure_airport'] = df_prev_delay['dx_departure_airport'].str.strip()
                    df_prev_delay['dx_departure_airport'] = df_prev_delay['dx_departure_airport'].str.split('(').str[0] 
                    df_prev_delay['dx_arrival_airport'] = df_prev_delay['dx_arrival_airport'].str.strip()
                    df_prev_delay['dx_arrival_airport'] = df_prev_delay['dx_arrival_airport'].str.split('(').str[0] 

                    # TRANSFORM 2e : Splitting status and langing time data
                    df_prev_delay['dx_arrival_real'] = df_prev_delay['dx_flight_status']
                    df_prev_delay['dx_arrival_real'] = df_prev_delay['dx_arrival_real'].str.strip() # Removing space before and after 
                    df_prev_delay['dx_arrival_real'] = df_prev_delay['dx_arrival_real'].str.extract(r'(\d{2}:\d{2})') # Using regex
                    df_prev_delay['dx_flight_status'] = df_prev_delay['dx_flight_status'].str.strip()
                    df_prev_delay['dx_flight_status'] = df_prev_delay['dx_flight_status'].str.split(' ').str[0] # Keep first word
                
                    # TRANSFORM 2f: Date format processing (conversion type str)
                    df_prev_delay["dx_flight_date"] = pd.to_datetime(df_prev_delay["dx_flight_date"], format="%d %b %Y") # Conversion in datetime object
                    date_reference_dtime = pd.to_datetime(ds_flight_date, format="%d/%m/%y")
                    df_prev_delay['dx_departure_airport_code'] = df_prev_delay['dx_departure_airport_code'].astype(str).str.strip()
                    df_prev_delay['dx_flight_code'] = df_prev_delay['dx_flight_code'].astype(str).str.strip()



                    #--------------------
                    # SUB FUNCTION : Calculation of the previous flight delay
                    #--------------------


                    def prev_delay_sub(dx_flight_date, dx_departure_plan, dx_departure_real, dx_flight_duration, dx_arrival_plan):
                        # CHECK : Checking of missing data
                        if pd.isna(dx_flight_date) or pd.isna(dx_departure_plan) or pd.isna(dx_departure_real) \
                            or pd.isna(dx_flight_duration) or pd.isna(dx_arrival_plan):
                            return None

                        # CLEANING : Chain cleaning
                        dx_departure_plan = dx_departure_plan.strip()
                        dx_departure_real = dx_departure_real.strip()
                        dx_arrival_plan = dx_arrival_plan.strip()

                        # CONVERSION : Date format conversion
                        if isinstance(dx_flight_date, pd.Timestamp) or isinstance(dx_flight_date, datetime):
                            flight_date_obj = dx_flight_date.date()
                        else:
                            flight_date_obj = pd.to_datetime(dx_flight_date).date()

                        # CONVERSION : Hours conversion to datetime.time object
                        departure_plan_obj = datetime.strptime(dx_departure_plan, "%H:%M").time()
                        departure_real_obj = datetime.strptime(dx_departure_real, "%H:%M").time()
                        arrival_plan_obj = datetime.strptime(dx_arrival_plan, "%H:%M").time()

                        # CONVERSION : Flight duration conversion in minutes
                        hh_duration, mm_duration = map(int, dx_flight_duration.split(":"))
                        flight_duration = timedelta(hours=hh_duration, minutes=mm_duration)

                        # VARIBALE SET : Constitution of complete datetime
                        datetime_depart_plan = datetime.combine(flight_date_obj, departure_plan_obj)
                        datetime_depart_real = datetime.combine(flight_date_obj, departure_real_obj)
                        datetime_arrival_plan = datetime.combine(flight_date_obj, arrival_plan_obj)
                        datetime_arrival_real = datetime_depart_real + flight_duration

                        # CONDITION : If arrival scheduled is before real departure
                        if datetime_arrival_plan < datetime_depart_real:
                            datetime_arrival_plan += timedelta(days=1)

                        # CALCULATION : Delay of previous flight in minutes
                        delay_prev_in_min = (datetime_arrival_real - datetime_arrival_plan).total_seconds() / 60

                        return delay_prev_in_min


                    #--------------------
                    # TRANSFORM 3 : Search for current flight index in df and the previous flight ones
                    #--------------------
                    try:
                        # MASK DEFINITION: To target the right index of the target flight within the dataframe 
                        mask = ((df_prev_delay['dx_flight_date'] == date_reference_dtime) &
                                (df_prev_delay['dx_flight_code'].str.strip().str.upper() == ds_flight_code.strip().upper()) &
                                (df_prev_delay['dx_flight_duration']== ds_flight_duration))
                        matching_index = df_prev_delay.index[mask]


                    # MASK APPLICATION
                        if len(matching_index) > 0:
                            selected_index = matching_index[0] + 1
                            if selected_index < len(df_prev_delay):
                            
                            # SELECT PREVIOUS FLIGHT : Select the previous line, corresponds to the previous flight
                                df_prev_delay = df_prev_delay.loc[[selected_index]]
                    
                                # PREVIOUS FLIGHT DELAY : Apply calcultion to estimate delay of the previous flight
                                row_df = df_prev_delay.iloc[0] #initialement ILOC
                                delay_result = prev_delay_sub(row_df['dx_flight_date'],
                                                        row_df['dx_departure_plan'],
                                                        row_df['dx_departure_real'],
                                                        row_df['dx_flight_duration'],
                                                        row_df['dx_arrival_plan'])

                                return delay_result
                            else:
                                print("Pas de vol précédent trouvé (index hors limite)")
                                return None
                        else:
                            print("Aucune ligne ne correspond aux critères")
                            return None


                    except Exception as e:
                        print(f"Erreur inattendue dans prev_delay: {e}")
                        return None

        return main()
     
    except Exception as e:
        print(f"Erreur dans prev_delay: {e}")
        return np.nan


