import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN


def km_to_radians(km: float) -> float:
    return km / 6371.0


def run_dbscan(df: pd.DataFrame, km: float, min_samples: int = 1) -> pd.DataFrame:
    coords = df[['latitude', 'longitude']].to_numpy()
    coords_rad = np.radians(coords)

    eps = km_to_radians(km)
    db = DBSCAN(eps=eps, min_samples=min_samples, metric='euclidean')

    df[f'cluster_{km}km'] = db.fit_predict(coords_rad)
    return df


def run_multi_dbscan(
    input_path: str, 
    output_path: str, 
    radii: list = [10, 30, 50],
    engine: str = "pyarrow"
) -> pd.DataFrame:

    # Load data
    df = pd.read_parquet(input_path, engine=engine)

    for km in radii:
        df = run_dbscan(df, km)

    df.to_parquet(output_path, index=False)
    df.to_csv(output_path.replace(".parquet", ".csv"), index=False)

    return df


if __name__ == "__main__":
    input_file = r"E:\Hydroneo\Analytics\disease\data\disease_locations.parquet"
    output_file = r"E:\Hydroneo\Analytics\disease\data\disease_clusters.parquet"

    df_result = run_multi_dbscan(input_file, output_file)

    print(df_result.head())
    print("\n✅ Results saved to:")
    print(output_file)
    print(output_file.replace(".parquet", ".csv"))
