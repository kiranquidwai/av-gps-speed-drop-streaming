# AV GPS Speed Drop Detection — Spark Structured Streaming Pipeline

**Autonomous Fleet Telemetry**

This project is created using a Spark Structured Streaming pipeline which is detecting sudden deceleration in autonomous vehicle GPS telemetry. This system simulates streaming by splitting a CSV dataset into small batch files then automatically sending those files into a watched directory.

The alert condition is: Speed drop greater than 20 km/h between consecutive sliding windows.

---

## Dataset

Dataset used:

**AV-GPS-Dataset-1.csv**

The dataset contains GPS and velocity readings from an autonomous vehicle for which,  20,000 sample rows are selected from the full dataset.

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
Python 
PySpark
Java
```

---

## How to Run the Project

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
## Where the Pipeline Requires State

I have chosen the sliding window because the continuous detection of sudden speed change is needed, so for this, overlapping  intervals are required and not the non-overlapping intervals. A 1-minute window has given enough readings to calculate a stable average speed, while the 10-second slide allows the pipeline to check for rapid speed changes frequently. This matches the autonomous fleet telemetry scenario because an accident-like event may occur within a short time period. 

---

## Where the Pipeline Requires State

The pipeline requires state in two places.

The pipeline needs state in two places.

1. Spark needs to remember the data inside each sliding window, so when the program calculates the average speed for each vehicle over a 1-minute window, Spark should keep the speed values long enough to calculate the average. Therefore the watermark will help the Spark to decide when old window data is no longer needed and can be cleared.

2. The alert logic also needs memory, so to detect a sudden speed drop, the program should compare the current window’s average speed with the previous window’s average speed for the same vehicle. Therefore the previous average speed is stored temporarily. Without storing this previous value, the program could only show the current speed average, but it would not know whether the vehicle slowed down by more than 20 km/h.


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
