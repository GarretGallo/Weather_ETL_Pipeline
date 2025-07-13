import pandas as pd
from pipeline.aggregation import (
    get_daily_temp, get_weekly_temp, get_monthly_temp,
    get_daily_wind_and_air, get_solar_daily, get_conditions,
    get_countrys)

def transformation(**kwargs):
    ti = kwargs['ti']
    parquet_path = ti.xcom_pull(task_ids='extract')

    if not  parquet_path:
        raise ValueError("Transformation got no data from extract")
    
    df = pd.read_parquet(parquet_path)

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    return {
        'daily_temp_avg': get_daily_temp(df).to_dict(orient='records'),
        'weekly_temp_avg': get_weekly_temp(df).to_dict(orient='records'),
        'monthly_temp_avg': get_monthly_temp(df).to_dict(orient='records'),
        'daily_wind_and_air': get_daily_wind_and_air(df).to_dict(orient='records'),
        'solar_daily': get_solar_daily(df).to_dict(orient='records'),
        'conditions_pct': get_conditions(df).to_dict(orient='records'),
        'countries_df': get_countrys(df).to_dict(orient='records')
    }
