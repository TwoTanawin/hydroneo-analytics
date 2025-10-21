from .utils.mongo import MongoConnection
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

async def main():
    mongo = MongoConnection(collections=os.getenv("DISEASE_COLLECTION"))
    mongo.connect()
    collection = mongo.get_db()
    
    # Include _id and diseaseLocation
    for doc in collection.find({}, {"diseaseLocation": 1, "_id": 1}):
        print({
            "id": str(doc["_id"]),                # convert ObjectId to string
            "diseaseLocation": doc.get("diseaseLocation")
        })

if __name__ == "__main__":
    asyncio.run(main())