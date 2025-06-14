import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, Numeric, PrimaryKeyConstraint
from sqlalchemy.orm import declarative_base
from airflow.providers.postgres.hooks.postgres import PostgresHook

Base = declarative_base()

#--- Table Schemas ---#
class weatherdaily(Base):
    __tablename__ = 'weather_daily'
    Country = Column(String, nullable=False)
    Date = Column(Date, nullable=False)
    Average = Column(Numeric(6,2), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('Country', 'Date', name='pk_weather_daily'),
    )

class weathermonth(Base):
    __tablename__ = 'weather_monthly'
    Country = Column(String, nullable=False)
    Date = Column(Date, nullable=False)
    Average = Column(Numeric(6,2), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('Country', 'Date', name='pk_weather_monthly'),
    )

class WeatherConditions(Base):
    __tablename__ = 'weather_conditions'
    Month = Column(String, nullable=False)
    Condition = Column(String, nullable=False)
    Percentage = Column(Numeric(6,2), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('Month','Condition', name='pk_weather_conditions'),
    )

#--- Load Function ---#
def load(**kwargs):
    hook   = PostgresHook(postgres_conn_id='postgres_db')
    engine = hook.get_sqlalchemy_engine()
    Base.metadata.create_all(engine)

    ti = kwargs['ti']
    daily = ti.xcom_pull(task_ids='transform', key='daily_avg')
    monthly = ti.xcom_pull(task_ids='transform', key='monthly_temp')
    cond_pct = ti.xcom_pull(task_ids='transform', key='conditions_pct')

    df_daily = pd.DataFrame(daily)
    df_monthly = pd.DataFrame(monthly)
    df_cond_pct = pd.DataFrame(cond_pct)

    df_daily.to_sql('weather_daily', con=engine, index=False, if_exists='replace')
    df_monthly.to_sql('weather_monthly', con=engine, index=False, if_exists='replace')
    df_cond_pct.to_sql('weather_conditions', con=engine, index=False, if_exists='replace')