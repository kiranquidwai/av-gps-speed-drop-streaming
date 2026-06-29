import time
import shutil
import threading
from pathlib import Path
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, to_timestamp
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, IntegerType)

# Paths
BATCH_DIR = Path("batches")
STREAM_DIR = Path("stream_input")
CHECKPOINT_DIR = Path("checkpoints/av_speed_alerts")

STREAM_DIR.mkdir(parents=True, exist_ok=True)

# Clean old stream files
for file in STREAM_DIR.glob("*.csv"):
    file.unlink()

# Clean old checkpoint so Spark processes files again
if CHECKPOINT_DIR.exists():
    shutil.rmtree(CHECKPOINT_DIR)

batch_files = sorted(BATCH_DIR.glob("*.csv"))
TOTAL_BATCHES = len(batch_files)

if TOTAL_BATCHES == 0:
    raise FileNotFoundError("No batch files found. Run: python src/prepare_batches.py")

# Global state
all_alerts = []
previous_window = {}
emitted_alerts = set()
finished_processing = threading.Event()
final_printed = False

# Send batch files automatically
def send_batches():
    time.sleep(8)

    print(f"\nSending {TOTAL_BATCHES} batch files to Spark stream...\n")

    for file in batch_files:
        shutil.copy(file, STREAM_DIR / file.name)
        time.sleep(1)

    print("All batches sent. Waiting for Spark to finish processing...\n")

# Print final anomaly table once
def print_final_alert_table():
    print("\n====================================== SPEED DROP ANOMALY ALERTS ========================================")

    if not all_alerts:
        print("No speed-drop anomalies found.")
        print("========================================================================================================\n")
        return

    header = [
        "window_start",
        "window_end",
        "vehicle_id",
        "previous_avg",
        "current_avg",
        "speed_drop"]

    widths = [20, 20, 10, 14, 12, 10]

    line = "+"
    for w in widths:
        line += "-" * (w + 2) + "+"

    print(line)
    print(
        f"| {header[0]:<{widths[0]}} "
        f"| {header[1]:<{widths[1]}} "
        f"| {header[2]:<{widths[2]}} "
        f"| {header[3]:<{widths[3]}} "
        f"| {header[4]:<{widths[4]}} "
        f"| {header[5]:<{widths[5]}} |"
    )
    print(line)

    for r in all_alerts:
        print(
            f"| {r[0]:<{widths[0]}} "
            f"| {r[1]:<{widths[1]}} "
            f"| {r[2]:<{widths[2]}} "
            f"| {r[3]:<{widths[3]}} "
            f"| {r[4]:<{widths[4]}} "
            f"| {r[5]:<{widths[5]}} |"
        )
    print(line)
    print("Alert condition: Speed drop > 20 km/h between consecutive sliding windows")
    print("========================================================================================================\n")

# Spark setup
spark = (
    SparkSession.builder
    .appName("AV-GPS-Speed-Drop-Alert")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("speed_mps", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("data_type", IntegerType(), True),
])

raw_stream = (
    spark.readStream
    .schema(schema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .csv(str(STREAM_DIR))
)

events = (
    raw_stream
    .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
    .filter(col("event_time").isNotNull())
    .filter(col("speed_kmh").isNotNull())
)

avg_speed = (
    events
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute", "10 seconds"),
        col("vehicle_id")
    )
    .agg(avg("speed_kmh").alias("avg_speed_kmh"))
)

# Batch processing and anomaly detection
def process_batch(batch_df, batch_id):
    global final_printed

    if batch_df.limit(1).count() == 0:
        return

    rows = (
        batch_df
        .select(
            col("vehicle_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_speed_kmh")
        )
        .orderBy("vehicle_id", "window_start")
        .collect()
    )

    for row in rows:
        vehicle_id = row["vehicle_id"]
        window_start = row["window_start"]
        window_end = row["window_end"]
        current_avg = row["avg_speed_kmh"]

        previous = previous_window.get(vehicle_id)

        if previous is not None:
            previous_start = previous["window_start"]
            previous_avg = previous["avg_speed_kmh"]

            time_gap = window_start - previous_start

            if timedelta(seconds=0) < time_gap <= timedelta(seconds=20):
                speed_drop = previous_avg - current_avg
                alert_key = (vehicle_id, str(window_start))

                if speed_drop > 20 and alert_key not in emitted_alerts:
                    emitted_alerts.add(alert_key)

                    all_alerts.append([
                        str(window_start),
                        str(window_end),
                        vehicle_id,
                        f"{previous_avg:.2f}",
                        f"{current_avg:.2f}",
                        f"{speed_drop:.2f}"
                    ])

        if previous is None or window_start > previous_window[vehicle_id]["window_start"]:
            previous_window[vehicle_id] = {
                "window_start": window_start,
                "avg_speed_kmh": current_avg
            }

    if batch_id >= TOTAL_BATCHES - 1 and not final_printed:
        final_printed = True
        print_final_alert_table()
        finished_processing.set()

# Start streaming
sender_thread = threading.Thread(target=send_batches, daemon=True)
sender_thread.start()

query = (
    avg_speed.writeStream
    .outputMode("update")
    .foreachBatch(process_batch)
    .option("checkpointLocation", str(CHECKPOINT_DIR))
    .trigger(processingTime="5 seconds")
    .start()
)
print("\nSpark Structured Streaming job started.")
print("Watching folder: stream_input")

finished_processing.wait()
time.sleep(5)
query.awaitTermination()