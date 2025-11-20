import numpy as np

def airport_rating(df_data_prov, airport_rating_csv):
    '''
    PURPOSE : 
        This file is a function called by the fonc_get_flight_data.py file. Its purpose is to enrich dataset with coordinates of the departure and arrival airports from internal source
        (csv from OurAirports)
    ARGS:
        df_data_prov (df) : Data frame temporary of the pipeline ETL
        airport_coord_csv (csv) : Internal source 
    RETURNS:
        float: Coodinates (LONG,LAT) of the departure and arrival airports     
    '''

    try:

        #=============================
        # PART 1 : RATING of DEPARTURE AIRPORT
        #=============================
        # MAPPING DEFINITION : Creation of a mapping dict for a direct approach
        rating_mapping = airport_rating_csv.set_index('IATA_airport_code')['ponctuality_rating'].to_dict()
        
        # MAPPING APPLICATION : Apply the mapping only on NaN values 
        mask = df_data_prov['ds_departure_airport_rating'].isna()
        df_data_prov.loc[mask, 'ds_departure_airport_rating'] = df_data_prov.loc[mask, 'ds_departure_airport_code'].map(rating_mapping)


        df_data_prov['ds_departure_airport_rating'] = (
        df_data_prov['ds_departure_airport_rating']
        .astype(str)                     # Ensuring that is text
        .str.replace(',', '.', regex=True)  
        .astype(float)                   # Conversion in float
    )


        #=============================
        # PART 2 : RATING of ARRIVAL AIRPORT
        #=============================
        # MAPPING DEFINITION : Creation of a mapping dict for a direct approach
        rating_mapping = airport_rating_csv.set_index('IATA_airport_code')['ponctuality_rating'].to_dict()
        
        # MAPPING APPLICATION : Apply the mapping only on NaN values 
        mask = df_data_prov['ds_arrival_airport_rating'].isna()
        df_data_prov.loc[mask, 'ds_arrival_airport_rating'] = df_data_prov.loc[mask, 'ds_arrival_airport_code'].map(rating_mapping)


        df_data_prov['ds_arrival_airport_rating'] = (
        df_data_prov['ds_arrival_airport_rating']
        .astype(str)                     # Ensuring that is text
        .str.replace(',', '.', regex=True)  
        .astype(float)                   # Conversion in float
    )

        return df_data_prov

    except Exception as e:
        print(f"Erreur dans aiport_rating: {e}")
        return np.nan
