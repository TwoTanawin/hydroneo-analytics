import os
from dotenv import load_dotenv
import logging
from bson import json_util

from pymongo import MongoClient

load_dotenv()
logging.basicConfig(level=logging.INFO)

class MongoConnection:
    def __init__(self, timeout_ms: int = 5000):
        self.uri = os.getenv("MONGO_URI")
        self.timeout_ms = timeout_ms
        self.db_name = os.getenv("DATABASE_NAME")
        self.collections = os.getenv("COLLECTIONS")
        
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
    
    
if __name__ == "__main__":
    mongo = MongoConnection()
    client = mongo.connect()
    db, collection = mongo.get_db()

    types = collection.distinct("type")
    logging.info("All notification types: %s", types)

    output_dir = "output_notifications"
    os.makedirs(output_dir, exist_ok=True)

    for t in types:
        first_doc = collection.find_one({"type": t})
        if first_doc:
            file_path = os.path.join(output_dir, f"{t}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json_util.dumps(first_doc, indent=4, ensure_ascii=False))
            print(f"✔ Saved to {file_path}")