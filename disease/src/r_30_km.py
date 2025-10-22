import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np
import matplotlib.pyplot as plt
from geopy.distance import geodesic


def km_to_radians(km: float) -> float:
    return km / 6371.0


def cluster_locations(df: pd.DataFrame, eps_km: float, min_samples: int = 2) -> pd.Series:
    coords = df[['latitude', 'longitude']].to_numpy()
    coords_rad = np.radians(coords)
    db = DBSCAN(eps=km_to_radians(eps_km), min_samples=min_samples, metric='haversine')
    
    return db.fit_predict(coords_rad)


def assign_new_point(new_lat: float, new_lon: float, df: pd.DataFrame,
                     cluster_col: str, eps_km: float) -> int:
    for cluster_id in df[cluster_col].unique():
        if cluster_id == -1:  # skip noise
            continue
        cluster_points = df[df[cluster_col] == cluster_id][['latitude', 'longitude']].to_numpy()
        for lat, lon in cluster_points:
            distance = geodesic((new_lat, new_lon), (lat, lon)).km
            if distance <= eps_km:
                return cluster_id
    return -1  # noise


def plot_clusters(df: pd.DataFrame, cluster_col: str,
                  new_lat: float = None, new_lon: float = None, new_cluster: int = None):
    plt.figure(figsize=(8, 6))

    for cluster_id in sorted(df[cluster_col].unique()):
        if cluster_id == -1:
            label = "Noise"
            color = "gray"
        else:
            label = f"Cluster {cluster_id}"
            color = None
        subset = df[df[cluster_col] == cluster_id]
        plt.scatter(subset['longitude'], subset['latitude'],
                    c=color, s=100, edgecolors='k', label=label, alpha=0.7)

    if new_lat is not None and new_lon is not None:
        plt.scatter(new_lon, new_lat, c='red', s=150, marker='*',
                    label=f"New point → {('Noise' if new_cluster == -1 else f'Cluster {new_cluster}')}")
    
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"DBSCAN Clusters ({cluster_col})")
    plt.legend(title="Clusters")
    plt.show()



if __name__ == "__main__":
    # Load your data
    df = pd.read_parquet(r"E:\Hydroneo\Analytics\disease\data\disease_locations.parquet", engine="pyarrow")

    # Choose cluster radius (km)
    eps_km = 100
    cluster_col = f"cluster_{eps_km}km"
    df[cluster_col] = cluster_locations(df, eps_km=eps_km)

    # Test a new point
    new_lat, new_lon = 16.335354  ,102.254739
    new_cluster = assign_new_point(new_lat, new_lon, df, cluster_col, eps_km)
    print(f"New point ({new_lat}, {new_lon}) belongs to cluster: {new_cluster}")

    # Plot result
    plot_clusters(df, cluster_col, new_lat, new_lon, new_cluster)
