import pandas as pd

# Path to your Parquet file
parquet_file = r"E:\Hydroneo\Analytics\disease\data\cleaned_data_removed_ZERO.parquet"

# Load the Parquet file
df = pd.read_parquet(parquet_file)

# Path to save the CSV
csv_file = r"E:\Hydroneo\Analytics\disease\data\cleaned_data_removed_ZERO.csv"

# Save as CSV
df.to_csv(csv_file, index=False)

print(f"CSV file saved at: {csv_file}")
