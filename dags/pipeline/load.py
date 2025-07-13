import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, PrimaryKeyConstraint, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import declarative_base
from airflow.providers.postgres.hooks.postgres import PostgresHook

Base = declarative_base()

class countrydb(Base):
    __tablename__ = 'country_db'
    country = Column(String, nullable=False)
    city = Column(String, nullable=False)
    latitude = Column(Numeric(6,2), nullable=False)
    longitude = Column(Numeric(6,2), nullable=False)
    timezone = Column(String, nullable=False)
    timeZoneOffset = Column(Numeric, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('country', 'city', name='pk_country'),
    )

class weatherdaily(Base):
    __tablename__ = 'tempDaily'
    country = Column(String, nullable=False)
    city = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    averageTempF = Column(Numeric(8,2), nullable=False)
    feelsLikeTemp = Column(Numeric(8,2), nullable=False)
    tempMax = Column(Numeric(8,2), nullable=False)
    tempMin = Column(Numeric(8,2), nullable=False)
    humidity = Column(Numeric(8,2), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('country', 'city', 'date', name='pk_tempDaily'),
        ForeignKeyConstraint(['country', 'city'], ['country_db.country', 'country_db.city'], name='fk_location'),
    )

class weatherweekly(Base):
    __tablename__ = 'tempWeekly'
    country = Column(String, nullable=False)
    city = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    averageTempF = Column(Numeric(8,2), nullable=False)
    feelsLikeTemp = Column(Numeric(8,2), nullable=False)
    tempMax = Column(Numeric(8,2), nullable=False)
    tempMin = Column(Numeric(8,2), nullable=False)
    humidity = Column(Numeric(8,2), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('country', 'city', 'date', name='pk_tempWeekly'),
        ForeignKeyConstraint(['country', 'city'], ['country_db.country', 'country_db.city'], name='fk_location'),
    )

class weathermonthly(Base):
    __tablename__ = 'tempMonthly'
    country = Column(String, nullable=False)
    city = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    averageTempF = Column(Numeric(8,2), nullable=False)
    feelsLikeTemp = Column(Numeric(8,2), nullable=False)
    tempMax = Column(Numeric(8,2), nullable=False)
    tempMin = Column(Numeric(8,2), nullable=False)
    humidity = Column(Numeric(8,2), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('country', 'city', 'date', name='pk_tempMonthly'),
        ForeignKeyConstraint(['country', 'city'], ['country_db.country', 'country_db.city'], name='fk_location'),
    )

class weatherair(Base):
    __tablename__ = 'weatherWindAir'
    country = Column(String, nullable=False)
    city = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    windSpeed = Column(Numeric(8,2), nullable=False)
    windDir = Column(Numeric(8,2), nullable=False)
    pressure = Column(Numeric(8,2), nullable=False)
    windGust = Column(Numeric(8,2), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('country', 'city', 'date', name='pk_windAir'),
        ForeignKeyConstraint(['country', 'city'], ['country_db.country', 'country_db.city'], name='fk_location'),
    )

class weathersolar(Base):
    __tablename__ = 'weatherSolarDaily'
    country = Column(String, nullable=False)
    city = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    solarRadiation = Column(Numeric(8,2), nullable=True)
    solarEnergy = Column(Numeric(8,2), nullable=True)
    uvIndex = Column(Numeric(8,2), nullable=True)
    severeRisk = Column(Numeric(8,2), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('country', 'city', 'date', name='pk_solarDaily'),
        ForeignKeyConstraint(['country', 'city'], ['country_db.country', 'country_db.city'], name='fk_location'),
    )

class weatherconditions(Base):
    __tablename__ = 'weatherConditions'
    country = Column(String,  nullable=False)
    city = Column(String,  nullable=False)
    month = Column(String,  nullable=False)
    condition = Column(String,  nullable=False)
    percentage = Column(Numeric(6,2), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('country','city','month','condition', name='pk_weatherConditions'),
        ForeignKeyConstraint(['country','city'],['country_db.country','country_db.city'],name='fk_weatherconditions_country'),
    )

def load(**kwargs):
    hook = PostgresHook(postgres_conn_id="enter db here")
    engine = hook.get_sqlalchemy_engine()
    Base.metadata.create_all(engine)

    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='transform')

    if not result:
        raise ValueError("Load got no data from transform")
    
    temp_daily = result['daily_temp_avg']
    temp_weekly = result['weekly_temp_avg']
    temp_monthly = result['monthly_temp_avg']
    wind_air = result['daily_wind_and_air']
    solar_daily = result['solar_daily']
    conditions_pct = result['conditions_pct']
    countries_db = result['countries_df']

    df_temp_daily = pd.DataFrame(temp_daily)
    df_temp_weekly = pd.DataFrame(temp_weekly)
    df_temp_monthly = pd.DataFrame(temp_monthly)
    df_wind_air = pd.DataFrame(wind_air)
    df_solar_daily = pd.DataFrame(solar_daily)
    df_conditions_pct = pd.DataFrame(conditions_pct)
    df_countries_db = pd.DataFrame(countries_db)
    df_countries_db = df_countries_db.drop_duplicates(subset=['country','city'])

    df_countries_db.to_sql('country_db', engine, if_exists='append', index=False)
    df_temp_daily.to_sql('tempDaily', engine, if_exists='append', index=False)
    df_temp_weekly.to_sql('tempWeekly', engine, if_exists='append', index=False)
    df_temp_monthly.to_sql('tempMonthly', engine, if_exists='append', index=False)
    df_wind_air.to_sql('weatherWindAir', engine, if_exists='append', index=False)
    df_solar_daily.to_sql('weatherSolarDaily', engine, if_exists='append', index=False)
    df_conditions_pct.to_sql('weatherConditions', engine, if_exists='append', index=False)
    
    print("Data loaded successfully")
