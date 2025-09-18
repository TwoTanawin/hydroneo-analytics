from pymongo import MongoClient
from bson import ObjectId

# connect using admin for auth, but we will use TitanDB
client = MongoClient(
    "mongodb://root:hydroneo@localhost:27017,localhost:27018/admin?authSource=admin"
)

# select TitanDB
db = client["TitanDB"]

# now use organisations collection
org_collection = db["organisation_resource_organisations"]

org_id = ObjectId("62612ad065d5a85acf3f7ef2")
organisation = org_collection.find_one({"_id": org_id})

if organisation:
    print("✅ Found organisation:", organisation["name"])
else:
    print("⚠️ Organisation not found")
