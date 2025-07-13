import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

#--- Fetch from API ---#
url = "INSERT URL HERE"
response = requests.get(url, timeout=10)
response.raise_for_status()
payload = response.json()

#--- Connect to MongoDB ---#
client = MongoClient(
    'ENTER URL HERE',
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
