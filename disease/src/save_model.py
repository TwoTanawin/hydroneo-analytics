import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import joblib
import os


def km_to_radians(km: float) -> float:
    """Convert kilometers to radians (for Earth radius)."""
    return km / 6371.0


def train_dbscan(df: pd.DataFrame, km: float, min_samples: int = 1, model_dir: str = "./models") -> pd.DataFrame:
    """Train DBSCAN on latitude/longitude and save model + labels."""
    coords = df[['latitude', 'longitude']].to_numpy()
    coords_rad = np.radians(coords)

    # Train model
    eps = km_to_radians(km)
    db = DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean")
    labels = db.fit_predict(coords_rad)

    # Add cluster labels to DataFrame
    df[f'cluster_{km}km'] = labels

    # Ensure model directory exists
    os.makedirs(model_dir, exist_ok=True)

    # Save model
    model_path = os.path.join(model_dir, f"dbscan_{km}km_model.pkl")
    joblib.dump(db, model_path)

    print(f"✅ Model saved: {model_path}")
    return df


if __name__ == "__main__":
    # Input/output
    input_file = r"E:\Hydroneo\Analytics\disease\data\disease_locations.parquet"
    output_file = r"E:\Hydroneo\Analytics\disease\data\disease_clusters.parquet"
    model_dir = r"E:\Hydroneo\Analytics\disease\models"

    # Load dataset
    df = pd.read_parquet(input_file, engine="pyarrow")

    # Train DBSCAN for multiple radii
    for km in [10, 30, 50]:
        df = train_dbscan(df, km=km, min_samples=2, model_dir=model_dir)

    # Save clustered results
    df.to_parquet(output_file, index=False)
    df.to_csv(output_file.replace(".parquet", ".csv"), index=False)

    print(f"\n✅ Results saved to: {output_file}")
    print(f"Also saved CSV: {output_file.replace('.parquet', '.csv')}")
