import pandas as pd
import calendar

def get_daily_averages(df: pd.DataFrame) -> pd.DataFrame:
    avg = df[['temp']].resample('D').mean().round(2)
    avg.reset_index(inplace=True)
    avg['Date'] = avg['date'].dt.strftime('%Y-%m-%d')
    avg['Average'] = avg['temp']
    avg['Country'] = df['country'].iloc[0] if 'country' in df.columns else None
    avg.drop(columns=['date', 'temp'], inplace=True)
    return avg[['Date', 'Country', 'Average']]


def get_monthly_temperature(df: pd.DataFrame) -> pd.DataFrame:
    avg = df[['temp']].resample('M').mean().round(2)
    avg.reset_index(inplace=True)
    avg['Date'] = avg['date'].dt.strftime('%Y-%m')
    avg['Average'] = avg['temp']
    avg['Country'] = df['country'].iloc[0] if 'country' in df.columns else None
    avg.drop(columns=['date', 'temp'], inplace=True)
    return avg[['Date', 'Country', 'Average']]


def get_conditions(df: pd.DataFrame) -> pd.DataFrame:
    dummies = pd.get_dummies(df['conditions'])
    pct = (dummies.groupby(df.index.month).mean() * 100).round(2)
    pct.index = [calendar.month_name[m] for m in pct.index]
    pct = pct.reset_index().rename(columns={'index': 'Month'})
    return pct.melt(id_vars=['Month'], var_name='Condition', value_name='Percentage')