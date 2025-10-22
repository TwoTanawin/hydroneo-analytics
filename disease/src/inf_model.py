import numpy as np
import pandas as pd
import joblib
import os
import asyncio

async def km_to_radians(km: float) -> float:
    return km / 6371.0

async def assign_new_point(lat: float, lon: float, df: pd.DataFrame, km: float) -> int:
    """
    Assign a new point to an existing DBSCAN cluster.
    Returns the cluster ID, or -1 if noise/outlier.
    """
    # Convert new point to radians
    point_rad = np.radians([lat, lon])

    # Get coordinates of training data
    coords = np.radians(df[['latitude', 'longitude']].to_numpy())
    labels = df[f'cluster_{km}km'].to_numpy()

    # Compute Euclidean distances in radian space
    distances = np.linalg.norm(coords - point_rad, axis=1)

    # Neighborhood threshold
    eps = await km_to_radians(km)

    # Find neighbors within eps
    neighbors_idx = np.where(distances <= eps)[0]

    if len(neighbors_idx) == 0:
        return -1  # noise/outlier

    # Pick the cluster of the first neighbor
    return labels[neighbors_idx[0]]

async def main():
    # Paths
    clustered_data_path = r"E:\Hydroneo\Analytics\disease\data\disease_clusters.parquet"
    model_dir = r"E:\Hydroneo\Analytics\disease\models"

    # Load clustered dataset
    df = pd.read_parquet(clustered_data_path, engine="pyarrow")

    # Radii and corresponding models
    radii = [10]
    models = {
        km: joblib.load(os.path.join(model_dir, f"dbscan_{km}km_model.pkl"))
        for km in radii
    }
    
        # Example new coordinates
    new_lat, new_lon = 6.6198218, 100.0785343

    print(f"\n📍 New point: ({new_lat}, {new_lon})\n")
    for km in radii:
        cluster_id = await assign_new_point(new_lat, new_lon, df, km)
        if cluster_id == -1:
            print(f"❌ At {km}km radius → NOISE (not part of any cluster)")
        else:
            print(f"✅ At {km}km radius → belongs to cluster {cluster_id}")
            
if __name__=="__main__":
    asyncio.run(main())