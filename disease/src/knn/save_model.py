import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import joblib
import os
import asyncio
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

def km_to_radians(km: float) -> float:
    return km / 6371.0

async def run_kmeans(df, km, model_dir, n_clusters=5):
    coords = df[['latitude', 'longitude']].to_numpy()

    # Train KMeans
    km_model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df[f'cluster_{km}km'] = km_model.fit_predict(coords)

    # Save sklearn model
    model_path = os.path.join(model_dir, f"kmeans_{km}km_model.pkl")
    joblib.dump(km_model, model_path)
    print(f"💾 Saved model → {model_path}")
    
    # Save ONNX model
    onnx_path = os.path.join(model_dir, f"kmeans_{km}km_model.onnx")
    await convert_to_onnx(km_model, onnx_path, n_features=2)  # lat + lon
    print(f"📦 Saved ONNX model → {onnx_path}")

    return df

async def convert_to_onnx(model, onnx_path, n_features: int = 2):
    try:
        initial_type = [("float_input", FloatTensorType([None, n_features]))]
        onnx_model = convert_sklearn(model, initial_types=initial_type)
        with open(onnx_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
    except Exception as e:
        print(f"⚠️ Could not convert to ONNX: {e}")

async def main():
    data_path = r"E:\Hydroneo\Analytics\disease\data\cleaned_data_removed_ZERO.parquet"
    output_path = r"E:\Hydroneo\Analytics\disease\data\clustered_kmeans.parquet"
    model_dir = r"E:\Hydroneo\Analytics\disease\models"

    os.makedirs(model_dir, exist_ok=True)

    # Load dataset
    df = pd.read_parquet(data_path)

    # Run KMeans for multiple radii (you can tune n_clusters separately per radius if you want)
    for km in [10, 30, 50]:
        df = await run_kmeans(df, km, model_dir, n_clusters=5)

    # Save clustered dataset
    df.to_parquet(output_path, index=False)
    print(f"✅ Saved clustered data to {output_path}")

if __name__ == "__main__":
    asyncio.run(main())
