import pandas as pd
import numpy as np
import joblib
import onnxruntime as ort

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
    # Load DataFrame
    df = pd.read_parquet(
        r"E:\Hydroneo\Analytics\disease\data\cleaned_data_removed_ZERO.parquet", 
        engine="pyarrow"
    )
    
    # Load pre-trained KMeans model
    # joblib_model_path = r'E:\Hydroneo\Analytics\disease\models\kmean_2_model_20251029_110536.pkl'
    onnx_model_path = r"E:\Hydroneo\Analytics\disease\models\kmean_2_model_20251029_110536.onnx"
    
    session = load_onnx_model(onnx_model_path)

    # Reference point
    new_lat, new_lon = 13.556924, 100.0950911
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

    # Vectorized score calculation
    df_cluster['score'] = distance_score_vectorized(df_cluster['distance_km'].to_numpy())

    # Top 10
    top10 = df_cluster.sort_values(by='score', ascending=False).head(10)
    print(top10[['id', 'latitude', 'longitude', 'cluster', 'distance_km', 'score']])

if __name__ == "__main__":
    main()