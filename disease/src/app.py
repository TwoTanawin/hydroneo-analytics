import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import time

def haversine_vectorized(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth's radius in km

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c

def distance_score_vectorized(distances):
    scores = np.zeros_like(distances, dtype=int)
    scores[distances < 1] = 100
    scores[(distances >= 1) & (distances < 10)] = 80
    scores[(distances >= 10) & (distances < 20)] = 60
    scores[(distances >= 20) & (distances < 50)] = 40
    # distances >= 50 will stay 0
    return scores

def train_kmeans(df: pd.DataFrame, n_clusters: int):
    coords = df[['latitude', 'longitude']].to_numpy()
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['cluster'] = kmeans.fit_predict(coords)
    return df, kmeans

def predict_cluster(kmeans, new_lat, new_lon) -> int:
    new_point = np.array([[new_lat, new_lon]])
    return kmeans.predict(new_point)[0]

def main():
    start = time.time()
    # Load DataFrame
    df = pd.read_parquet(
        r"E:\Hydroneo\Analytics\disease\data\cleaned_data_removed_ZERO.parquet", 
        engine="pyarrow"
    )

    # Train KMeans
    n_clusters = 3
    df, kmeans = train_kmeans(df, n_clusters)
    print("Cluster centers (lat, lon):")
    print(kmeans.cluster_centers_)

    # Reference point
    new_lat, new_lon = 13.556924, 100.0950911
    cluster_id = predict_cluster(kmeans, new_lat, new_lon)
    print(f"Cluster ID: {cluster_id}")

    # Filter cluster
    df_cluster = df[df['cluster'] == cluster_id].copy()

    # Vectorized distance calculation
    df_cluster['distance_km'] = haversine_vectorized(
        df_cluster['latitude'].to_numpy(),
        df_cluster['longitude'].to_numpy(),
        new_lat,
        new_lon
    )

    # Vectorized score calculation
    df_cluster['score'] = distance_score_vectorized(df_cluster['distance_km'].to_numpy())

    # Top 10
    top10 = df_cluster.sort_values(by='score', ascending=False).head(10)
    print(top10[['id', 'latitude', 'longitude', 'cluster', 'distance_km', 'score']])
    
    end = time.time()
    print(f"Time taken: {end - start:.4f} seconds")

if __name__ == "__main__":
    main()
