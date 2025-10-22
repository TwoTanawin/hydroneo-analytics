import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import joblib
import os
import asyncio


def km_to_radians(km: float) -> float:
    return km / 6371.0

async def run_dbscan(df, km, model_dir, min_samples=2):
    coords = df[['latitude', 'longitude']].to_numpy()
    coords_rad = np.radians(coords)

    # Train DBSCAN
    db = DBSCAN(eps=km_to_radians(km), min_samples=min_samples, metric='haversine')
    df[f'cluster_{km}km'] = db.fit_predict(coords_rad)

    model_path = os.path.join(model_dir, f"dbscan_{km}km_model.pkl")
    joblib.dump(db, model_path)
    print(f"💾 Saved model → {model_path}")

    return df

async def main():
    data_path = r"E:\Hydroneo\Analytics\disease\data\cleaned_data_removed_ZERO.parquet"
    output_path = r"E:\Hydroneo\Analytics\disease\data\clustered_2.parquet"
    model_dir = r"E:\Hydroneo\Analytics\disease\models"

    os.makedirs(model_dir, exist_ok=True)

    # Load dataset
    df = pd.read_parquet(data_path)

    # Run DBSCAN for multiple radii and save models
    for km in [10, 30, 50]:
        df = await run_dbscan(df, km, model_dir)

    # Save clustered dataset
    df.to_parquet(output_path, index=False)
    print(f"✅ Saved clustered data to {output_path}")

if __name__ == "__main__":
    asyncio.run(main())
