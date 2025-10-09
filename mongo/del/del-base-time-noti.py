import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
logging.basicConfig(level=logging.INFO)

class MongoConnection:
    def __init__(self, timeout_ms: int = 5000):
        self.uri = os.getenv("MONGO_URI")
        self.timeout_ms = timeout_ms
        self.db_name = os.getenv("DATABASE_NAME")
        self.collections = "notification_resource_notifications"
        
    def connect(self) -> MongoClient:
        if not self.uri:
            raise ValueError("MONGO_URI is not set")
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=self.timeout_ms)
        self.client.admin.command("ping")
        logging.info("Mongo connected")
        return self.client
    
    def get_db(self):
        if not self.db_name or not self.collections:
            raise ValueError("DATABASE_NAME or NOTIFICATION_COLLECTION is not set")
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collections]
        return self.db, self.collection
    

if __name__ == "__main__":
    mongo = MongoConnection()
    client = mongo.connect()
    db, collection = mongo.get_db()
    
    # Define range for October 6–7, 2025 (UTC full days)
    start_date = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_date   = datetime(2025, 10, 9, 23, 59, 59, tzinfo=timezone.utc)
    
    # IMPORTANT: field name is createTimestamp (no "d")
    query = {"createTimestamp": {"$gte": start_date, "$lte": end_date}}

    # Count matches
    match_count = collection.count_documents(query)
    print(f"Matched {match_count} documents")

    # Preview first 5 docs
    print("Preview of docs to be deleted:")
    for m in collection.find(query).limit(5):
        print(m)

    # Confirm + delete
    confirm = input("Proceed with delete? (yes/no): ").strip().lower()
    if confirm == "yes":
        result = collection.delete_many(query)
        print(f"Deleted {result.deleted_count} documents")
    else:
        print("Delete aborted")
