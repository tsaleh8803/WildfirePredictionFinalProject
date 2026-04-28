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

Run this if weather data needs to be updated:

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
