import numpy as np

def airline_rating(df_data_prov, airline_rating_csv):
    '''
    PURPOSE : 
        This file is a function called by the fonc_get_flight_data.py file. Its purpose is to enrich dataset with ponctuality ratings of the airline from internal source
        (csv from AirHelp)
    ARGS:
        df_data_prov(df) : Data frame temporary of the pipeline ETL
        airport_rating_csv (csv) : Internal source 
    RETURNS:
        float: Ponctuality ratings of the airline 
    '''

    try:

        #=============================
        # PART 1 : RATING of DEPARTURE AIRPORT
        #=============================
        # MAPPING DEFINITION : Creation of a mapping dict for a direct approach
        df_data_prov['ds_airline_code']=df_data_prov['ds_flight_code'].str[:2]

        rating_mapping = airline_rating_csv.set_index('IATA_airline_code')['ponctuality_rating'].to_dict()
        
        # MAPPING APPLICATION : Apply the mapping only on NaN values 
        mask = df_data_prov['ds_airline_rating'].isna()
        df_data_prov.loc[mask, 'ds_airline_rating'] = df_data_prov.loc[mask, 'ds_airline_code'].map(rating_mapping)


        df_data_prov['ds_airline_rating'] = (
        df_data_prov['ds_airline_rating']
        .astype(str)                     # Ensuring that is text
        .str.replace(',', '.', regex=True)  
        .astype(float)                   # Conversion in  float
    )

        return df_data_prov
    
    except Exception as e:
        print(f"Erreur dans prev_delay: {e}")
        return np.nan

    
