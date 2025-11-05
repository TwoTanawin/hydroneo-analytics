import pandas as pd
import numpy as np
import joblib
import onnxruntime as ort
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
    return scores

def load_model(joblib_model_path):
    return joblib.load(joblib_model_path)

def load_onnx_model(onnx_model_path):
    session = ort.InferenceSession(onnx_model_path)
    print(f"✅ ONNX model loaded: {onnx_model_path}")
    return session

def predict_cluster(kmeans, new_lat, new_lon) -> int:
    new_point = np.array([[new_lat, new_lon]])
    return kmeans.predict(new_point)[0]

def predict_onnx_cluster(session, new_lat, new_lon):
    new_point = np.array([[new_lat, new_lon]], dtype=np.float32)
    
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    
    # Run inference
    predicted_cluster = session.run([output_name], {input_name: new_point})[0][0]
    
    return int(predicted_cluster)

def main():
    start = time.time()
    
    # Load DataFrame from CSV
    df = pd.read_csv(
        "/Volumes/PortableSSD/Hydroneo/analytics/disease/data/cleaned_data_removed_ZERO.csv"
    )
    
    # Load pre-trained ONNX model
    onnx_model_path = "/Volumes/PortableSSD/Hydroneo/analytics/disease/models/kmean_2_model_20251029_111224.onnx"
    session = load_onnx_model(onnx_model_path)

    # Reference point
    new_lat, new_lon = 15.3925, 100.1031
    cluster_id = predict_onnx_cluster(session, new_lat, new_lon)
    print(f"✅ Predicted cluster: {cluster_id}")

    # Predict cluster for all rows in DataFrame
    coords = df[['latitude', 'longitude']].to_numpy(dtype=np.float32)
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    df['cluster'] = session.run([output_name], {input_name: coords})[0]

    # Filter by cluster
    df_cluster = df[df['cluster'] == cluster_id].copy()

    # Vectorized distance calculation
    df_cluster['distance_km'] = haversine_vectorized(
        df_cluster['latitude'].to_numpy(),
        df_cluster['longitude'].to_numpy(),
        new_lat,
        new_lon
    )

    # Filter distances less than or equal to 100 km
    df_near = df_cluster[df_cluster['distance_km'] <= 100].copy()
    print(f"✅ Found {len(df_near)} records within 100 km")

    # Vectorized score calculation
    df_near['score'] = distance_score_vectorized(df_near['distance_km'].to_numpy())

    # Show top 10 nearest points
    top10_near = df_near.sort_values(by='distance_km', ascending=True).head(10)
    print(top10_near[['id', 'latitude', 'longitude', 'distance_km', 'score']])

    
    end = time.time()
    print(f"Time taken: {end - start:.4f} seconds")
    
if __name__ == "__main__":
    main()
