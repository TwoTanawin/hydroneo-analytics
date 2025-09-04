import os
from dotenv import load_dotenv
import logging
from bson import json_util

from pymongo import MongoClient
from bson import ObjectId

load_dotenv()
logging.basicConfig(level=logging.INFO)

class MongoConnection:
    def __init__(self, timeout_ms: int = 5000):
        self.uri = os.getenv("MONGO_URI")
        self.timeout_ms = timeout_ms
        self.db_name = os.getenv("DATABASE_NAME")
        self.collections = os.getenv("FARM_COLLECTION")
        
    def connect(self) -> MongoClient:
        if not self.uri:
            raise ValueError("MONGO_URI is not set")
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=self.timeout_ms)
        self.client.admin.command("ping")
        logging.info("Mongo connected")
        return self.client
    
    def get_db(self):
        if not self.db_name or not self.collections:
            raise ValueError("DATABASE_NAME or COLLECTIONS is not set")
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collections]
        return self.db, self.collection

def url_to_dict(url:str)->dict:
    parts = url.split("/")
    dct = {}
    for i in range(0, len(parts) - 1, 2):
        dct[parts[i]] = parts[i + 1]
    return dct

if __name__=="__main__":
    mongo = MongoConnection()
    client = mongo.connect()
    db, collection = mongo.get_db()
    
    url = "organisations/62612ad065d5a85acf3f7ef2/farms/625e69c565d5a85acf3edd6a/ponds/625e6a0365d5a85acf3eddb4/cycles/6268fbd2eb614b1924ddcf49/688c85ea3787096263fef288.webp"
    data = url_to_dict(url)
    
    farmId = data["farms"]
    
    try:
        farm_oid = ObjectId("625e69c565d5a85acf3edd6a")
        query = {'_id': farm_oid}
    except:
        query = {'_id': farmId}
        
    result = collection.find_one(query)

    if result:
        print("Farm document found:")
        print(json_util.dumps(result, indent=4))
    else:
        print(f"No farm found with ID {farmId}")