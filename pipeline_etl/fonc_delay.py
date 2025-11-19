from datetime import datetime, timedelta
import pandas as pd
import numpy as np


def delay(dx_flight_date, dx_departure_plan, dx_departure_real, dx_flight_duration, dx_arrival_plan):
    '''
    PURPOSE : 
      This file is a function called by the main.py file. Its purpose is to calculate the delay in minutes of the targeted flight.
    ARGS:
        dx_flight_date (str) : Target flight date
        dx_departure_plan (str) : Target flight departure scheduled (hh:mm)
        dx_departure_real (str) : Target flight departure real (hh:mm)
        dx_flight_duration (str) : Target flight duration (hh:mm)
        dx_arrival_plan (str) : Target flight arrival scheduled (hh:mm)      
    RETURNS:
        float: Target flight delay in minutes     
    '''
    try :
         
        # CHECK : Checking of missing data
        if pd.isna(dx_flight_date) or pd.isna(dx_departure_plan) or pd.isna(dx_departure_real) \
            or pd.isna(dx_flight_duration) or pd.isna(dx_arrival_plan):
                return None

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
        delay_in_min = (datetime_arrival_real - datetime_arrival_plan).total_seconds() / 60

        return delay_in_min

    except Exception as e:
        print(f"Erreur dans delay: {e}")
        return np.nan





