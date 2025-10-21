from .utils.mongo import MongoConnection
import asyncio
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

async def main():
    mongo = MongoConnection(collections=os.getenv("DISEASE_COLLECTION"))
    mongo.connect()
    _, collection = mongo.get_db()
    
    results = []
    for doc in collection.find({}, {"diseaseLocation": 1, "_id": 1}):
        results.append({
            "id": str(doc["_id"]),  # convert ObjectId to string
            "latitude": doc.get("diseaseLocation", {}).get("latitude"),
            "longitude": doc.get("diseaseLocation", {}).get("longitude")
        })

    # Convert to DataFrame
    df = pd.DataFrame(results)

    # Export to Parquet
    df.to_parquet("disease_locations.parquet", engine="pyarrow", index=False)

    print("Exported to disease_locations.parquet")

if __name__ == "__main__":
    asyncio.run(main())