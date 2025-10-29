import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2
from sklearn.cluster import KMeans


def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth's radius in km

    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

def distance_score(distance_km):
    if distance_km < 1:
        return 100
    elif distance_km < 10:
        return 80
    elif distance_km < 20:
        return 60
    elif distance_km < 50:
        return 40
    else:
        return 0

def train_kmeans(df: pd.DataFrame, n_clusters: int) -> tuple[pd.DataFrame, KMeans]:
    coords = df[['latitude', 'longitude']].to_numpy()
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['cluster'] = kmeans.fit_predict(coords)
    return df, kmeans

def predict_cluster(kmeans, new_lat, new_lon)->int:
    new_point = np.array([[new_lat, new_lon]])

    cluster_id = kmeans.predict(new_point)[0]
    
    return cluster_id

def main():
    # Load your DataFrame
    df = pd.read_parquet(
        r"E:\Hydroneo\Analytics\disease\data\cleaned_data_removed_ZERO.parquet", 
        engine="pyarrow"
    )

    # --- Train KMeans ---
    n_clusters = 3
    df, kmeans = train_kmeans(df, n_clusters)
    print("Cluster centers (lat, lon):")
    print(kmeans.cluster_centers_)

    # --- Reference point ---
    new_lat, new_lon = 13.556924, 100.0950911

    # Distance & score to reference point
    df['distance_km'] = df.apply(
        lambda row: haversine(row['latitude'], row['longitude'], new_lat, new_lon),
        axis=1
    )
    df['score'] = df['distance_km'].apply(distance_score)

    # --- Distance & score to cluster center ---
    def distance_to_center(row):
        cluster_center = kmeans.cluster_centers_[row['cluster']]
        return haversine(row['latitude'], row['longitude'], cluster_center[0], cluster_center[1])

    df['distance_to_center_km'] = df.apply(distance_to_center, axis=1)
    df['center_score'] = df['distance_to_center_km'].apply(distance_score)

    # Predict cluster for the new point
    cluster_id = predict_cluster(kmeans, new_lat, new_lon)
    print(f"New point belongs to cluster: {cluster_id}")

    # Show results
    print(df[['id', 'latitude', 'longitude', 'cluster', 
              'distance_km', 'score', 'distance_to_center_km', 'center_score']])

if __name__ == "__main__":
    main()