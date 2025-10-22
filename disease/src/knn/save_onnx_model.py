import numpy as np
import pandas as pd
import onnxruntime as ort
import os
import asyncio

async def assign_new_point(lat: float, lon: float, session: ort.InferenceSession) -> int:
    """
    Assign a new point to an existing KMeans ONNX model.
    Returns the cluster ID.
    """
    point = np.array([[lat, lon]], dtype=np.float32)  # ONNX expects float32
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name

    pred = session.run([output_name], {input_name: point})[0]
    cluster_id = int(pred[0])
    return cluster_id

async def main():
    # Paths
    clustered_data_path = r"E:\Hydroneo\Analytics\disease\data\clustered_kmeans.parquet"
    model_dir = r"E:\Hydroneo\Analytics\disease\models"

    # Load clustered dataset (optional, just to check)
    df = pd.read_parquet(clustered_data_path, engine="pyarrow")

    # Radii and corresponding ONNX models
    radii = [10, 30, 50]
    sessions = {
        km: ort.InferenceSession(os.path.join(model_dir, f"kmeans_{km}km_model.onnx"))
        for km in radii
    }

    # Example new coordinates
    new_lat, new_lon = 6.6198218, 100.0785343
    print(f"\n📍 New point: ({new_lat}, {new_lon})\n")

    for km in radii:
        session = sessions[km]
        cluster_id = await assign_new_point(new_lat, new_lon, session)
        print(f"✅ At {km}km radius → belongs to cluster {cluster_id}")

if __name__=="__main__":
    asyncio.run(main())
