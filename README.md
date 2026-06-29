# AV GPS Speed Drop Detection — Spark Structured Streaming Pipeline

## Scenario

**Scenario A: Autonomous Fleet Telemetry**

This project builds a Spark Structured Streaming pipeline to detect sudden deceleration in autonomous vehicle GPS telemetry. The system simulates streaming by splitting a CSV dataset into small batch files and automatically sending those files into a watched directory.

The alert condition is:

**Speed drop greater than 20 km/h between consecutive sliding windows.**

---

## Dataset

Dataset used:

**AV-GPS-Dataset-1.csv**

The original dataset contains GPS and velocity readings from an autonomous vehicle. For this assignment, a sample of **20,000 rows** was selected from the full dataset, which satisfies the instruction that a sample of **10,000–50,000 rows** is sufficient.

The sample was divided into:

```text
40 batch files
500 rows per batch
```

These batch files are copied into the streaming input folder to simulate real-time vehicle telemetry.

---

## Columns Used

| Output field | Source / transformation                  |
| ------------ | ---------------------------------------- |
| `vehicle_id` | Derived fixed value: `AV_1`              |
| `event_time` | `Clock Date` + `Clock Time`              |
| `speed_mps`  | `Velocity (m/s)`                         |
| `speed_kmh`  | `Velocity (m/s) × 3.6`                   |
| `data_type`  | `Data Type` column retained from dataset |

The dataset contains one autonomous vehicle, so a fixed vehicle ID, `AV_1`, is created for streaming and window grouping.

---

## Project Structure

```text
ASS_6/
│
├── raw/
│   └── AV-GPS-Dataset-1.csv
│
├── batches/
│   └── batch_0000.csv ... batch_0039.csv
│
├── stream_input/
│   └── watched folder used by Spark readStream
│
├── checkpoints/
│   └── av_speed_alerts/
│
├── src/
│   ├── prepare_batches.py
│   ├── av_streaming_alert.py
│   └── send_batches.py
│
└── README.md
```

---

## Requirements

Install Python libraries:

```bash
pip install pyspark pandas
```

Software required:

```text
Python 3.8+
PySpark
Java 11 or Java 17
```

---

## How to Run the Project

Run all commands from the project root folder:

```text
ASS_6/
```

Example:

```powershell
cd C:\Users\kiran\Desktop\ass_6
```

---

## Step 1: Prepare Streaming Batch Files

Run:

```powershell
python src/prepare_batches.py
```

This script:

1. Reads `raw/AV-GPS-Dataset-1.csv`
2. Creates `event_time` from `Clock Date` and `Clock Time`
3. Converts speed from m/s to km/h
4. Selects 20,000 rows
5. Splits the sample into 40 CSV batch files
6. Saves the files into the `batches/` folder

Expected output:

```text
Created 40 batch files in batches
Prepared data columns:
vehicle_id  event_time           speed_mps  speed_kmh  data_type
AV_1        2022-02-18 14:52:52  0.0        0.00       0
```

---

## Step 2: Start the Spark Streaming Job

Run:

```powershell
python src/av_streaming_alert.py
```

This script starts Spark Structured Streaming and watches the folder:

```text
stream_input/
```

The script automatically copies batch files from:

```text
batches/
```

into:

```text
stream_input/
```

This simulates a live stream using Spark `readStream`.

---

## Expected Console Output

When the alert condition is detected, the program prints one final anomaly table:

```text
===== SPEED DROP ANOMALY ALERTS =====
+----------------------+----------------------+------------+----------------+--------------+------------+
| window_start         | window_end           | vehicle_id | previous_avg   | current_avg  | speed_drop |
+----------------------+----------------------+------------+----------------+--------------+------------+
| 2022-04-04 13:06:50  | 2022-04-04 13:07:50  | AV_1       | 47.46          | 9.41         | 38.04      |
| 2022-04-04 13:27:30  | 2022-04-04 13:28:30  | AV_1       | 55.56          | 5.40         | 50.16      |
| 2022-04-05 11:00:30  | 2022-04-05 11:01:30  | AV_1       | 113.86         | 0.02         | 113.84     |
| 2022-04-05 12:55:30  | 2022-04-05 12:56:30  | AV_1       | 41.35          | 9.12         | 32.23      |
+----------------------+----------------------+------------+----------------+--------------+------------+
Alert condition: Speed drop > 20 km/h between consecutive sliding windows
```

Take a screenshot of this anomaly table for submission.

---

## Streaming Logic

The pipeline performs the following steps:

1. Reads incoming CSV files using Spark `readStream`
2. Parses the event timestamp
3. Filters invalid timestamps and missing speed values
4. Applies a watermark on event time
5. Computes average speed using a sliding window
6. Compares each vehicle’s current window average with the previous window average
7. Prints an anomaly alert if the average speed drops by more than 20 km/h

---

## Window Configuration

| Parameter              | Value                                |
| ---------------------- | ------------------------------------ |
| Window type            | Sliding window                       |
| Window duration        | 1 minute                             |
| Slide interval         | 10 seconds                           |
| Watermark              | 2 minutes                            |
| Alert threshold        | Speed drop greater than 20 km/h      |
| Streaming input method | Watched directory using `readStream` |

---

## Why I Chose This Window Type

I chose a **sliding window** because sudden deceleration must be detected continuously, not only at fixed non-overlapping intervals. A 1-minute window gives enough readings to calculate a stable average speed, while the 10-second slide allows the pipeline to check for rapid speed changes frequently.

This matches the autonomous fleet telemetry scenario because an accident-like event may occur within a short time period. If a tumbling window were used, a sudden drop could be split across two separate windows and become harder to detect. The sliding window reduces that risk because consecutive windows overlap and update every 10 seconds.

---

## Where the Pipeline Requires State

The pipeline requires state in two main places.

First, Spark maintains state for the sliding-window aggregation. Since the pipeline calculates average speed per vehicle over event-time windows, Spark must keep partial values such as sums and counts until each window is complete. The `withWatermark("event_time", "2 minutes")` setting tells Spark how long to keep late-arriving event-time data before old window state can be removed.

Second, the alert logic requires state because the current window’s average speed must be compared with the previous window’s average speed for the same vehicle. The program stores the previous window average in memory and compares it against the current average. Without this state, the system could calculate average speed, but it could not detect whether the speed dropped by more than 20 km/h from one window to the next.

---

## Alert Condition

The alert condition is:

```text
previous_avg_speed - current_avg_speed > 20 km/h
```

When this condition is true, the pipeline prints:

```text
window_start
window_end
vehicle_id
previous average speed
current average speed
speed drop
```

This satisfies the assignment requirement to trigger an alert when a vehicle’s average speed drops by more than 20 km/h between consecutive sliding windows.

---

## Notes

The dataset has one autonomous vehicle, so the project uses a derived fixed vehicle ID called `AV_1`. The main purpose of the assignment is to demonstrate Spark Structured Streaming, event-time watermarking, sliding-window aggregation, and stateful alert detection.
