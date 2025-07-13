import pandas as pd
from pymongo import MongoClient

#--- Connect to MongoDB and Pull Data ---#
def fetch_and_load(**kwargs):
    client = MongoClient('ENTER URL HERE')
    db = client["WWAI"]
    collection = db["Test.Weather"]

    cursor = collection.find()
    
    records = []
    for doc in cursor:
        country = doc.get("resolvedAddress")
        city = doc.get("address")
        for day in doc.get("days", []):
            date_day = day.get("datetime")
            for hour in day.get("hours", []):
                records.append({
                    "country": country,
                    "city": city,
                    "latitude": doc.get('latitude'),
                    'longitude': doc.get('longitude'),
                    'timezone': doc.get('timezone'),
                    'timeZoneOffset': doc.get('tzoffset'),

                    "date": date_day,
                    "time": hour.get("datetime"),

                    "temp": hour.get("temp"),
                    'feelslike': hour.get('feelslike'),
                    "humidity": hour.get("humidity"),

                    "windspeed": hour.get("windspeed"),
                    "winddir": hour.get("winddir"),
                    'pressure': hour.get('pressure'),
                    "windgust": hour.get("windgust"),

                    "solarradiation": hour.get('solarradiation'),
                    "solarenergy": hour.get('solarenergy'),
                    "uvindex": hour.get('uvindex'),
                    "severerisk": hour.get('severerisk'),

                    "conditions": hour.get("conditions"),

                    "precip": hour.get("precip"),
                    "preciptype": hour.get("preciptype"),
                })
    
    if not records:
        raise ValueError("No records fetched—check your DB/collection names and projection")

    df = pd.DataFrame(records)
    path = f"/tmp/weathers-{kwargs['ds']}.parquet"
    df.to_parquet(path)
    return path
