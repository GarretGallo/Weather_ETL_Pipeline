import pandas as pd
import calendar

def get_daily_temp(df):
    countries =[]
    for (country, city), grp in df.groupby(['country','city']):
        avg = grp[['temp']].resample('D').mean().round(2)
        avg.reset_index(inplace=True)
        avg['date'] = avg['date'].dt.strftime('%m-%d-%Y')
        avg['averageTempF'] = avg['temp']
        avg['country'] = country
        avg['city'] = city
        
        daily_max = grp['temp'].resample('D').max().round(2).reset_index(drop=True)
        daily_min = grp['temp'].resample('D').min().round(2).reset_index(drop=True)
        daily_feels = grp['feelslike'].resample('D').mean().round(2).reset_index(drop=True)
        daily_hum = grp['humidity'].resample('D').mean().round(2).reset_index(drop=True)

        avg['tempMax'] = daily_max
        avg['tempMin'] = daily_min
        avg['feelsLikeTemp'] = daily_feels
        avg['humidity'] = daily_hum

        countries.append(avg[['date','country','city', 'averageTempF','feelsLikeTemp',
                              'tempMax','tempMin','humidity']])

    return pd.concat(countries, ignore_index=True)

def get_weekly_temp(df):
    countries =[]
    for (country, city), grp in df.groupby(['country','city']):
        avg = grp[['temp']].resample('W').mean().round(2)
        avg.reset_index(inplace=True)
        avg['date'] = avg['date'].dt.strftime('%Y-%m-%d')
        avg['averageTempF'] = avg['temp']
        avg['country'] = country
        avg['city'] = city

        weekly_max = grp['temp'].resample('W').max().round(2).reset_index(drop=True)
        weekly_min = grp['temp'].resample('W').min().round(2).reset_index(drop=True)
        weekly_feels = grp['feelslike'].resample('W').mean().round(2).reset_index(drop=True)
        weekly_hum = grp['humidity'].resample('W').mean().round(2).reset_index(drop=True)

        avg['tempMax'] = weekly_max
        avg['tempMin'] = weekly_min
        avg['feelsLikeTemp'] = weekly_feels
        avg['humidity'] = weekly_hum

        countries.append(avg[['date', 'country', 'city', 'averageTempF', 
                    'tempMax', 'tempMin', 'feelsLikeTemp', 'humidity']])

    return pd.concat(countries, ignore_index=True)

def get_monthly_temp(df):
    countries =[]
    for (country, city), grp in df.groupby(['country','city']):
        avg = grp[['temp']].resample('M').mean().round(2)
        avg.reset_index(inplace=True)
        avg['date'] = avg['date'].dt.strftime('%Y-%m-%d')
        avg['averageTempF'] = avg['temp']
        avg['country'] = country
        avg['city'] = city

        monthly_max = grp['temp'].resample('M').max().round(2).reset_index(drop=True)
        monthly_min = grp['temp'].resample('M').min().round(2).reset_index(drop=True)
        monthly_feels = grp['feelslike'].resample('M').mean().round(2).reset_index(drop=True)
        monthly_hum = grp['humidity'].resample('M').mean().round(2).reset_index(drop=True)

        avg['tempMax'] = monthly_max
        avg['tempMin'] = monthly_min
        avg['feelsLikeTemp'] = monthly_feels
        avg['humidity'] = monthly_hum

        countries.append(avg[['date', 'country', 'city', 'averageTempF',
                            'tempMax', 'tempMin', 'feelsLikeTemp', 'humidity']])
    
    return pd.concat(countries, ignore_index=True)

def get_daily_wind_and_air(df):
    countries =[]
    for (country, city), grp in df.groupby(['country','city']):
        avg = grp[['windspeed']].resample('D').mean().round(2)
        avg.reset_index(inplace=True)
        avg['date'] = avg['date'].dt.strftime('%Y-%m-%d')
        avg['windSpeed'] = avg['windspeed']
        avg['country'] = country
        avg['city'] = city

        winddir = grp['winddir'].resample('D').max().round(2).reset_index(drop=True)
        pressure = grp['pressure'].resample('D').max().round(2).reset_index(drop=True)
        windgust = grp['windgust'].resample('D').max().round(2).reset_index(drop=True)

        avg['windDir'] = winddir
        avg['pressure'] = pressure
        avg['windGust'] = windgust

        countries.append(avg[['date', 'country', 'city', 'windSpeed',
                    'windDir', 'pressure', 'windGust']])
    
    return pd.concat(countries, ignore_index=True)

def get_solar_daily(df):
    countries =[]
    for (country, city), grp in df.groupby(['country','city']):
        avg = grp[['solarradiation']].resample('D').mean().round(2)
        avg.reset_index(inplace=True)
        avg['date'] = avg['date'].dt.strftime('%Y-%m-%d')
        avg['solarRadiation'] = avg['solarradiation']
        avg['country'] = country
        avg['city'] = city

        solarenergy = grp['solarenergy'].resample('D').max().round(2).reset_index(drop=True)
        uvindex = grp['uvindex'].resample('D').max().round(2).reset_index(drop=True)
        severerisk = grp['severerisk'].resample('D').max().round(2).reset_index(drop=True)

        avg['solarEnergy'] = solarenergy
        avg['uvIndex'] = uvindex
        avg['severeRisk'] = severerisk

        countries.append(avg[['date', 'country', 'city', 'solarRadiation',
                    'solarEnergy', 'uvIndex', 'severeRisk']])

    return pd.concat(countries, ignore_index=True)

def get_conditions(df: pd.DataFrame):
    all_pct = []

    for (country, city), grp in df.groupby(['country', 'city']):
        dummies = pd.get_dummies(grp['conditions'])
        pct = (dummies.groupby(grp.index.month).mean().mul(100).round(2))

        pct.index = [calendar.month_name[m] for m in pct.index]
        pct = pct.reset_index().rename(columns={'index': 'month'})

        pct = pct.melt(
            id_vars=['month'],
            var_name='condition',
            value_name='percentage'
        )

        pct['country'] = country
        pct['city']    = city

        all_pct.append(pct)

    return pd.concat(all_pct, ignore_index=True)[['month', 'country', 'city', 'condition', 'percentage']]

def get_countrys(df: pd.DataFrame):
    countries_df = df[['country', 'city', 'latitude', 'longitude', 'timezone', 'timeZoneOffset']]
    return countries_df