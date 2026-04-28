# Wildfire Prediction Cloud Pipeline

This project builds a distributed wildfire prediction pipeline using live NASA FIRMS hotspot data, NOAA/NWS forecast weather data, Hadoop HDFS, Apache Spark, and a Streamlit-compatible GeoJSON export.

## Cluster Layout

- `node-master`: Spark Master + HDFS NameNode
- `node-worker1`: Spark Worker + HDFS DataNode
- `node-worker2`: Spark Worker + HDFS DataNode
- `node-worker3`: Spark Worker + HDFS DataNode

## Core Project Files

### `wildfire_shared.py`

Shared utility module used across the project. This file contains reusable functions for:

- loading and cleaning NASA FIRMS wildfire hotspot data  
- creating the geospatial 0.25° grid system  
- assigning hotspots to grid cells  
- computing neighboring cells and partition ownership  
- helper logic reused by Spark and local scripts  

This file centralizes common logic so multiple scripts use the same preprocessing and spatial definitions.

### `wildfire_spark.py`

Main distributed prediction pipeline executed with Apache Spark.

Responsibilities:

- loads live wildfire hotspot data from FIRMS  
- loads weather forecasts from HDFS weather outputs  
- performs distributed daily aggregation of hotspot activity  
- computes burning neighbors and halo/boundary cell interactions  
- generates wildfire risk scores for each grid cell  
- predicts next-day wildfire activity  
- writes final outputs to HDFS:

```text
/user/exouser/wildfire/output/predictions_spark
/user/exouser/wildfire/output/daily_features_spark
```
### `fetch_nws_weather_region.py`

Regional weather ingestion pipeline using the National Weather Service API.

Responsibilities:

- identify representative wildfire-active cells for each region  
- convert grid centroids into forecast lookup points  
- fetch hourly weather forecasts  
- parse wind speed and wind direction data  
- aggregate hourly forecasts into daily summaries  
- write weather data to HDFS for Spark processing  

Output path:

```text
/user/exouser/wildfire/weather_region_daily
```

### `export_predictions_geojson.py`

Post-processing export script that converts Spark outputs into map-ready geospatial files.

Responsibilities:

- copies prediction CSV outputs from HDFS to local storage  
- validates geometry boundary columns  
- reconstructs grid polygons from min/max lat/lon coordinates  
- exports wildfire predictions as GeoJSON  
- saves cleaned CSV copies for dashboard use  

Output files include:

```text
output/predictions.csv
data/predictions_map.geojson
```

### `wildfire_frontend.py`

Interactive dashboard built with Streamlit and Plotly for wildfire monitoring and prediction display.

Features:

- national U.S. wildfire severity dashboard  
- state-level choropleth risk maps  
- top 10 high-risk state rankings  
- severity class breakdown charts  
- detailed state metrics (temperature, wind, humidity, hotspots)  
- interactive wildfire prediction grid map  
- filters for prediction score, severity class, and display layers  
- ranked high-risk grid cells for next-day fire potential  

This file provides the user-facing visualization interface for decision support and exploratory analysis

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
