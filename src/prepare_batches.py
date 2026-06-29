import shutil
from pathlib import Path
import pandas as pd

RAW_FILE = Path("data/raw/AV-GPS-Dataset-1.csv")
BATCH_DIR = Path("data/batches")
STREAM_DIR = Path("data/stream_input")

ROWS_TO_USE = 20000
BATCH_SIZE = 500

BATCH_DIR.mkdir(parents=True, exist_ok=True)
STREAM_DIR.mkdir(parents=True, exist_ok=True)

# Clean old batch files
for folder in [BATCH_DIR, STREAM_DIR]:
    for file in folder.glob("*.csv"):
        file.unlink()

df = pd.read_csv(RAW_FILE)

# Create event timestamp from Clock Date + Clock Time
df["event_time"] = pd.to_datetime(
    df["Clock Date"].astype(str) + " " + df["Clock Time"].astype(str),
    errors="coerce"
)

# Keep only required columns
df = df[["event_time", "Velocity (m/s)", "Data Type"]].dropna()

# Sort by timestamp so streaming behaves like real event-time data
df = df.sort_values("event_time").head(ROWS_TO_USE)

# Create clean columns for Spark
df["vehicle_id"] = "AV_1"
df["speed_mps"] = df["Velocity (m/s)"]
df["speed_kmh"] = df["speed_mps"] * 3.6
df["data_type"] = df["Data Type"]

df = df[["vehicle_id", "event_time", "speed_mps", "speed_kmh", "data_type"]]

# Convert timestamp to Spark-friendly format
df["event_time"] = df["event_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

# Write small CSV files as streaming batches
for i in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[i:i + BATCH_SIZE]
    batch_file = BATCH_DIR / f"batch_{i // BATCH_SIZE:04d}.csv"
    batch.to_csv(batch_file, index=False)

print(f"Created {len(list(BATCH_DIR.glob('*.csv')))} batch files in {BATCH_DIR}")
print("Prepared data columns:")
print(df.head())