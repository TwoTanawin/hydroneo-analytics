import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import timedelta
from collections import defaultdict

load_dotenv()
logging.basicConfig(level=logging.INFO)

class MongoConnection:
    def __init__(self, timeout_ms: int = 5000):
        self.uri = os.getenv("MONGO_URI")
        self.timeout_ms = timeout_ms
        self.db_name = os.getenv("DATABASE_NAME")
        self.collections = "message_resource_messages"
        
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
    
def time_analytics(matches):
    timestamps = []
    
    for m in matches:
        timestamps.append(m.get("createdTimestamp"))
        print(m.get("createdTimestamp"))
        
    print("Timestamps:")
    for t in timestamps:
        print(t)

    print("\nTime gaps (in minutes):")
    for i in range(1, len(timestamps)):
        gap = timestamps[i-1] - timestamps[i]
        gap_minutes = gap.total_seconds() / 60
        print(f"{timestamps[i-1]}  ->  {timestamps[i]}  =  {gap_minutes:.3f} minutes")
        
def display_data(matches):
    for m in matches:
        print(f"{m}")
        
def check_15min_duplicates(matches):
    timestamps = [m.get("createdTimestamp") for m in matches if m.get("createdTimestamp")]
    timestamps.sort(reverse=True)  # already sorted by Mongo, but safe

    print("Checking time gaps...")
    last_time = None
    for t in timestamps:
        if last_time:
            gap = last_time - t
            gap_minutes = gap.total_seconds() / 60
            print(f"{last_time} -> {t} = {gap_minutes:.3f} minutes")

            # Check if gap is within 15 minutes window
            if gap_minutes < 10:
                print(f"⚠️ Duplicate detected (gap only {gap_minutes:.3f} min)")
        last_time = t
        
def check_duplicate_info(matches, window_minutes=15):
    seen = defaultdict(list)  # key -> list of timestamps
    duplicates = []

    for m in matches:
        org = str(m.get("organisationId"))
        farm = str(m.get("farmId"))
        pond = str(m.get("pondId"))
        gw = str(m.get("gatewayId"))
        type_ = m.get("type")
        wp = m.get("data", {}).get("nodes", {}).get("waterParameterClass", {}).get("value")

        ts = m.get("createdTimestamp")
        if not ts:
            continue

        # composite key for uniqueness
        key = (org, farm, pond, gw, type_, wp)

        # check previous timestamps for this key
        for prev in seen[key]:
            gap = abs((ts - prev).total_seconds()) / 60
            if gap < window_minutes:
                duplicates.append((key, prev, ts, gap))

        seen[key].append(ts)

    # print duplicates
    if not duplicates:
        print("✅ No duplicates found within window")
    else:
        print("⚠️ Duplicates found:")
        for key, prev, ts, gap in duplicates:
            print(f"{key} -> {prev} vs {ts} (gap {gap:.3f} min)")
    
if __name__ == "__main__":
    mongo = MongoConnection()
    client = mongo.connect()
    db, collection = mongo.get_db()

    
    query = {}

    matches = list(
        collection.find(query).sort("createdTimestamp", -1).limit(10)
    )
    
    # time_analytics(matches)
    # display_data(matches)
    check_15min_duplicates(matches)
    # check_duplicate_info(matches, window_minutes=15)


