import requests
import os
from dotenv import load_dotenv

load_dotenv()

OPENAPI_BASE_URL = os.getenv("OPENAPI_BASE_URL")
OPENAPI_AUTH_TOKEN = os.getenv("OPENAPI_AUTH_TOKEN")

sensor_types = [
    'YDS', 'NDP', 'SAP', 'SES', 'OCS', 'OMS', 
    'STOF', 'LUX', 'IDS', 'WSS', 'WDS', 'RFS', 'OGPS'
]

url = f"{OPENAPI_BASE_URL}/api/v1/sensors"
headers = {
    "Authorization": f"Bearer {OPENAPI_AUTH_TOKEN}"
}

for sensor in sensor_types:
    params = {
        "limit": 10,
        "offset": 0,
        "sensorTypes": sensor
    }

    response = requests.get(url, headers=headers, params=params)
    
    print("="*20 + f" {sensor} " + "="*20)
    print("Status Code:", response.status_code)
    
    try:
        print("Response JSON:", response.json())
    except ValueError:
        print("Response is not JSON:", response.text)
