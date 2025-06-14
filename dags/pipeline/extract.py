import pandas as pd
from pymongo import MongoClient

#--- Connect to MongoDB and Pull Data ---#
def fetch_and_load(**kwargs):
    client = MongoClient('mongodb+srv://garretgallo06:Mvan6WHgkRFJDWRx@cluster0.srwmbf0.mongodb.net/')
    db = client["WWAI"]
    collection = db["Test.Weather"]

    cursor = collection.find({},{"resolvedAddress": 1, "days": 1, "_id": 0})

    records = []
    for doc in cursor:
        country = doc.get("resolvedAddress")
        for day in doc.get("days", []):
            records.append({
                "country":     country,
                "date":        day.get("datetime"), 
                "temp":        day.get("temp"),      
                "tempmax":     day.get("tempmax"),
                "tempmin":     day.get("tempmin"),
                "humidity":    day.get("humidity"),
                "precip":      day.get("precip"),
                "preciptype":  day.get("preciptype"),
                "snow":        day.get("snow"),
                "snowdepth":   day.get("snowdepth"),
                "windgust":    day.get("windgust"),
                "windspeed":   day.get("windspeed"),
                "winddirection": day.get("winddir"),
                "conditions":  day.get("conditions"),
            })

    if not records:
        raise ValueError("No records fetched—check your DB/collection names and projection")

    return records