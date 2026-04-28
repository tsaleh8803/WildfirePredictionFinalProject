# Wildfire Prediction Cloud Pipeline

This project builds a distributed wildfire prediction pipeline using live NASA FIRMS hotspot data, NOAA/NWS forecast weather data, Hadoop HDFS, Apache Spark, and a Streamlit-compatible GeoJSON export.

## Cluster Layout

- `node-master`: Spark Master + HDFS NameNode
- `node-worker1`: Spark Worker + HDFS DataNode
- `node-worker2`: Spark Worker + HDFS DataNode
- `node-worker3`: Spark Worker + HDFS DataNode

## Important Project Files

```text
wildfire_shared.py
wildfire_spark.py
fetch_nws_weather_region.py
export_predictions_geojson.py
test_prediction_algorithm.py
raw_fire/
output/
data/
```

Expected project path:

```bash
/home/exouser/wildfire_project
```

If the project volume is mounted at `/media/volume/WildfirePredictionV1-CourseProject`, `/data` should point there:

```bash
sudo rm -rf /data
sudo ln -s /media/volume/WildfirePredictionV1-CourseProject /data
```

---

## 1. Set Environment Variables

Run on the master before using Spark or HDFS:

```bash
export HADOOP_HOME=/data/hadoop
export SPARK_HOME=/data/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
```

Verify:

```bash
which hdfs
which spark-submit
```

---

## 2. Start HDFS and Spark

### On master

```bash
start-dfs.sh
start-master.sh
jps
```

Expected master processes include:

```text
NameNode
SecondaryNameNode
Master
```

Check HDFS:

```bash
hdfs dfsadmin -report
```

You should see:

```text
Live datanodes (3)
```

### On each worker

```bash
export HADOOP_HOME=/data/hadoop
export SPARK_HOME=/data/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

hdfs --daemon start datanode
start-worker.sh spark://node-master:7077
```

Check:

```bash
jps
ps -ef | grep -i datanode
ps -ef | grep -i spark.deploy.worker.Worker
```

---

## 3. Fetch Region-Level Weather Data

Run this if weather data is missing or stale:

```bash
cd /home/exouser/wildfire_project

spark-submit \
  --master spark://node-master:7077 \
  --conf spark.eventLog.enabled=false \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.shuffle.mapStatus.compression.codec=lz4 \
  --conf spark.sql.shuffle.partitions=3 \
  --executor-memory 1g \
  --total-executor-cores 3 \
  fetch_nws_weather_region.py
```

Verify:

```bash
hdfs dfs -ls /user/exouser/wildfire/weather_region_daily
hdfs dfs -cat /user/exouser/wildfire/weather_region_daily/part-*.csv | head
```

Expected columns:

```text
region_id,date,temp_mean,temp_max,wind_speed_mean,wind_speed_max,wind_dir_mean
```

---

## 4. Run the Distributed Spark Prediction Pipeline

```bash
cd /home/exouser/wildfire_project

spark-submit \
  --master spark://node-master:7077 \
  --conf spark.eventLog.enabled=false \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.shuffle.mapStatus.compression.codec=lz4 \
  --conf spark.sql.shuffle.partitions=3 \
  --executor-memory 1g \
  --total-executor-cores 3 \
  wildfire_spark.py
```

Spark writes outputs to HDFS:

```text
/user/exouser/wildfire/output/predictions_spark
/user/exouser/wildfire/output/daily_features_spark
```

Verify:

```bash
hdfs dfs -ls /user/exouser/wildfire/output/predictions_spark
hdfs dfs -ls /user/exouser/wildfire/output/daily_features_spark
```

You should see `_SUCCESS` and `part-*.csv`.

Preview predictions:

```bash
hdfs dfs -cat /user/exouser/wildfire/output/predictions_spark/part-*.csv | head -20
```

---

## 5. Export Predictions for the Frontend

The frontend map expects:

```text
/home/exouser/wildfire_project/data/predictions_map.geojson
```

Run:

```bash
python3 export_predictions_geojson.py
```

This writes:

```text
output/predictions.csv
data/predictions_map.geojson
```

Verify:

```bash
ls -lh output/predictions.csv
ls -lh data/predictions_map.geojson
```

The GeoJSON contains real-time prediction properties such as:

```text
cell_id
region_id
target_date
score
predicted_fire_next_day
current_hotspot_count
burning_neighbors
two_hop_burning_neighbors
avg_frp
max_frp
temp_mean
temp_max
wind_speed_mean
wind_speed_max
weather_risk
```

It should not include `actual_fire_next_day` for real-time dashboard display.

---

## 6. Streamlit Frontend

The dashboard should read:

```python
GEOJSON_PATH = "/home/exouser/wildfire_project/data/predictions_map.geojson"
```

Optional CSV path:

```python
PREDICTION_CSV = "/home/exouser/wildfire_project/output/predictions.csv"
```

Run:

```bash
streamlit run app.py
```

---

## 7. Backtesting Evaluation

Real-time predictions cannot be scored until the target day has passed. For precision, recall, accuracy, and F1, use backtesting.

Example:

```bash
spark-submit \
  --master spark://node-master:7077 \
  --conf spark.eventLog.enabled=false \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.shuffle.mapStatus.compression.codec=lz4 \
  --conf spark.sql.shuffle.partitions=3 \
  --executor-memory 1g \
  --total-executor-cores 3 \
  wildfire_spark.py --backtest 2026-04-24
```

Export:

```bash
hdfs dfs -get -f /user/exouser/wildfire/output/predictions_spark/part-*.csv predictions_backtest.csv
hdfs dfs -get -f /user/exouser/wildfire/output/daily_features_spark/part-*.csv daily_features.csv
```

Calculate metrics:

```bash
python3 - <<'PY'
import pandas as pd

pred = pd.read_csv("predictions_backtest.csv")
daily = pd.read_csv("daily_features.csv")

truth = daily[["date", "cell_id", "hotspot_count"]].copy()
truth = truth.rename(columns={"date": "target_date"})
truth["actual_fire_next_day"] = (truth["hotspot_count"] > 0).astype(int)
truth = truth[["target_date", "cell_id", "actual_fire_next_day"]].drop_duplicates()

df = pred.merge(truth, on=["target_date", "cell_id"], how="left")
df["actual_fire_next_day"] = df["actual_fire_next_day"].fillna(0).astype(int)

tp = ((df["predicted_fire_next_day"] == 1) & (df["actual_fire_next_day"] == 1)).sum()
fp = ((df["predicted_fire_next_day"] == 1) & (df["actual_fire_next_day"] == 0)).sum()
tn = ((df["predicted_fire_next_day"] == 0) & (df["actual_fire_next_day"] == 0)).sum()
fn = ((df["predicted_fire_next_day"] == 0) & (df["actual_fire_next_day"] == 1)).sum()

precision = tp / (tp + fp) if (tp + fp) else 0
recall = tp / (tp + fn) if (tp + fn) else 0
accuracy = (tp + tn) / len(df) if len(df) else 0
f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

print("Rows:", len(df))
print("TP:", tp)
print("FP:", fp)
print("TN:", tn)
print("FN:", fn)
print("Precision:", round(precision, 3))
print("Recall:", round(recall, 3))
print("Accuracy:", round(accuracy, 3))
print("F1:", round(f1, 3))
PY
```

---

## 8. Local Algorithm Test on Saved Raw Files

To test only the prediction algorithm without Spark:

```bash
python3 test_prediction_algorithm.py
```

This uses files in:

```text
raw_fire/
```

Expected outputs:

```text
output/test_predictions.csv
output/test_evaluation_rows.csv
```

---

## 9. Runtime / Scalability Tests

Use `/usr/bin/time -p`.

### Local baseline

```bash
/usr/bin/time -p spark-submit \
  --master local[*] \
  --conf spark.eventLog.enabled=false \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.shuffle.mapStatus.compression.codec=lz4 \
  --conf spark.sql.shuffle.partitions=3 \
  wildfire_spark.py
```

### Cluster with 1, 2, or 3 executor cores

Change only `--total-executor-cores`.

```bash
/usr/bin/time -p spark-submit \
  --master spark://node-master:7077 \
  --conf spark.eventLog.enabled=false \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.shuffle.mapStatus.compression.codec=lz4 \
  --conf spark.sql.shuffle.partitions=3 \
  --executor-memory 1g \
  --total-executor-cores 3 \
  wildfire_spark.py
```

Example observed runtimes:

```text
Single-node local mode: 72.32 s
1 worker cluster:       81.10 s
2 worker cluster:       80.37 s
3 worker cluster:       81.98 s
```

For this data size, local execution can be faster because cluster scheduling, network transfer, HDFS access, and executor coordination overhead outweigh the benefit of parallelism.

---

## 10. Common Issues

### `spark-submit: No such file or directory`

Set paths again:

```bash
export HADOOP_HOME=/data/hadoop
export SPARK_HOME=/data/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
```

### Broken `/home/exouser/wildfire_project` symlink

Check mount:

```bash
lsblk
df -h
```

If needed:

```bash
sudo rm -rf /data
sudo ln -s /media/volume/WildfirePredictionV1-CourseProject /data
```

### Spark says no resources accepted

Start workers:

```bash
start-worker.sh spark://node-master:7077
```

### DataNode status looks wrong

Use these checks instead:

```bash
hdfs dfsadmin -report
ps -ef | grep -i datanode
ss -ltnp | grep 9866
```

### GeoJSON export fails

Make sure `wildfire_spark.py` outputs:

```text
min_lon
min_lat
max_lon
max_lat
```

These are required to build grid polygons.

---

## 11. End-to-End Quick Run

When the cluster is already running:

```bash
cd /home/exouser/wildfire_project

spark-submit \
  --master spark://node-master:7077 \
  --conf spark.eventLog.enabled=false \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.shuffle.mapStatus.compression.codec=lz4 \
  --conf spark.sql.shuffle.partitions=3 \
  --executor-memory 1g \
  --total-executor-cores 3 \
  wildfire_spark.py

python3 export_predictions_geojson.py

ls -lh output/predictions.csv
ls -lh data/predictions_map.geojson
```

Main frontend output:

```text
/home/exouser/wildfire_project/data/predictions_map.geojson
