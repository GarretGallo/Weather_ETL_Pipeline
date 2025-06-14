import pandas as pd
from pipeline.aggregation import (get_daily_averages, get_monthly_temperature, get_conditions)

def transform(**kwargs) -> dict:
    ti  = kwargs['ti']
    raw = ti.xcom_pull(task_ids='extract')  
    if not raw:
        raise ValueError("Transform got no data from extract")

    
    df = pd.DataFrame(raw)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # perform your aggregations
    return {
        'daily_avg':      get_daily_averages(df).to_dict(orient='records'),
        'monthly_temp':   get_monthly_temperature(df).to_dict(orient='records'),
        'conditions_pct': get_conditions(df).to_dict(orient='records'),
    }