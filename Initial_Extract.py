import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

#--- Fetch from API ---#
url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Tokyo/2024-06-13/2025-06-13?unitGroup=us&key=V29UE9AMYM6D62AS4CKTZHGCP&contentType=json"
response = requests.get(url, timeout=10)
response.raise_for_status()
payload = response.json()

#--- Connect to MongoDB ---#
client = MongoClient(
    'mongodb+srv://garretgallo06:Mvan6WHgkRFJDWRx@cluster0.srwmbf0.mongodb.net/',
    tls = True,
    tlsAllowInvalidCertificates=True)
db = client['WWAI']
collection = db['Test.Weather']

#--- Insert Data ---#
if isinstance(payload, list):
    try:
        result = collection.insert_many(payload, ordered=False)
        print(f"Inserted {len(result.inserted_ids)} documents.")
    except BulkWriteError as bwe:
        print("Some inserts failed:", bwe.details)
else:
    result = collection.insert_one(payload)
    print("Inserted one doc with _id:", result.inserted_id)