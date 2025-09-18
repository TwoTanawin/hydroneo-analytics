import os
from dotenv import load_dotenv
import logging
from pymongo import MongoClient
from bson import ObjectId

load_dotenv()
logging.basicConfig(level=logging.INFO)

class MongoConnection:
    def __init__(self, collection, timeout_ms: int = 5000):
        self.uri = "mongodb://root:hydroneo@localhost:27017/admin?authSource=admin&directConnection=true"
        self.timeout_ms = timeout_ms
        self.db_name = "TitanDB"  # explicitly set DB
        self.collections = collection
        
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
    # Connect to organisation and account collections
    org_conn = MongoConnection(collection="organisation_resource_organisations")
    account_conn = MongoConnection(collection="account_resource_accounts")

    org_client = org_conn.connect()
    account_client = account_conn.connect()

    org_db, org_collection = org_conn.get_db()
    acc_db, acc_collection = account_conn.get_db()

    # Example: pick a single organisation by _id
    org_id = ObjectId("62612ad065d5a85acf3f7ef2")  # wrap in ObjectId
    organisation = org_collection.find_one({"_id": org_id})
    if not organisation:
        logging.warning(f"Organisation {org_id} not found")
        exit(1)

    # Get all memberIds
    member_ids = organisation.get("memberIds", [])
    logging.info(f"Member IDs in organisation {org_id}: {member_ids}")

    # Query accounts to get device tokens
    accounts_cursor = acc_collection.find(
        {"_id": {"$in": member_ids}}, 
        {"pushNotificationTokens": 1, "username": 1}
    )
    
    account_device_tokens = {}
    for account in accounts_cursor:
        account_id = account["_id"]
        tokens = account.get("pushNotificationTokens", [])
        account_device_tokens[account_id] = tokens

    logging.info("Account device tokens:")
    for acc_id, tokens in account_device_tokens.items():
        logging.info(f"{acc_id}: {tokens}")
