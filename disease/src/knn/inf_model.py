import numpy as np
import pandas as pd
import joblib
import os
import asyncio

async def assign_new_point(lat: float, lon: float, model, km: float) -> int:
    """
    Assign a new point to an existing KMeans cluster.
    Returns the cluster ID.
    """
    point = np.array([[lat, lon]])  # KMeans expects 2D input
    cluster_id = model.predict(point)[0]
    return int(cluster_id)

async def main():
    # Paths
    clustered_data_path = r"E:\Hydroneo\Analytics\disease\data\clustered_kmeans.parquet"
    model_dir = r"E:\Hydroneo\Analytics\disease\models"

    # Load clustered dataset (with cluster assignments)
    df = pd.read_parquet(clustered_data_path, engine="pyarrow")

    # Radii and corresponding models
    radii = [10, 30, 50]
    models = {
        km: joblib.load(os.path.join(model_dir, f"kmeans_{km}km_model.pkl"))
        for km in radii
    }
    
    # Example new coordinates
    new_lat, new_lon = 6.6198218, 100.0785343
    print(f"\n📍 New point: ({new_lat}, {new_lon})\n")

    for km in radii:
        model = models[km]
        cluster_id = await assign_new_point(new_lat, new_lon, model, km)
        print(f"✅ At {km}km radius → belongs to cluster {cluster_id}")

if __name__=="__main__":
    asyncio.run(main())
