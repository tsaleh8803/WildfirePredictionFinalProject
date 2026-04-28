from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pandas as pd
import argparse

from wildfire_shared import (
    fetch_firms_area_csv,
    clean_viirs,
    create_grid,
    hotspots_to_grid,
    assign_worker_by_region,
    build_neighbor_map,
    build_boundary_halo_metadata,
)

HDFS_BASE = "hdfs:///user/exouser/wildfire"

HOTSPOTS_PATH = f"{HDFS_BASE}/hotspots_partitioned"
OWNER_PATH = f"{HDFS_BASE}/owner_map"
BOUNDARY_PATH = f"{HDFS_BASE}/boundary_cells"
HALO_PATH = f"{HDFS_BASE}/halo_cells"
NEIGHBOR_PATH = f"{HDFS_BASE}/neighbor_edges"
WEATHER_REGION_DAILY_PATH = f"{HDFS_BASE}/weather_region_daily"

PREDICTIONS_PATH = f"{HDFS_BASE}/output/predictions_spark"
DAILY_FEATURES_PATH = f"{HDFS_BASE}/output/daily_features_spark"

MAP_KEY = "b09dbfb7dcc38d00d2105b9459473005"
FIRMS_SOURCE = "VIIRS_NOAA21_NRT"
FIRMS_BBOX = "-125,24,-66,50"
FIRMS_DAY_RANGE = 3
CELL_SIZE_DEG = 0.25
IGNITION_THRESHOLD = 15.0
NUM_REGIONS = 3


def write_intermediate_data_to_hdfs(spark, joined_pdf, owner_df, boundary_df, halo_df, neighbor_rows):
    hotspots_sdf = spark.createDataFrame(joined_pdf)
    owner_sdf = spark.createDataFrame(owner_df)
    boundary_sdf = spark.createDataFrame(boundary_df)
    halo_sdf = spark.createDataFrame(halo_df)
    neighbor_sdf = spark.createDataFrame(pd.DataFrame(neighbor_rows))

    hotspots_sdf.write.mode("overwrite").partitionBy("region_id").parquet(HOTSPOTS_PATH)
    owner_sdf.write.mode("overwrite").parquet(OWNER_PATH)
    boundary_sdf.write.mode("overwrite").parquet(BOUNDARY_PATH)
    halo_sdf.write.mode("overwrite").parquet(HALO_PATH)
    neighbor_sdf.write.mode("overwrite").parquet(NEIGHBOR_PATH)


def read_intermediate_data_from_hdfs(spark):
    hotspots_sdf = spark.read.parquet(HOTSPOTS_PATH)
    owner_sdf = spark.read.parquet(OWNER_PATH)
    boundary_sdf = spark.read.parquet(BOUNDARY_PATH)
    halo_sdf = spark.read.parquet(HALO_PATH)
    neighbor_sdf = spark.read.parquet(NEIGHBOR_PATH)

    return hotspots_sdf, owner_sdf, boundary_sdf, halo_sdf, neighbor_sdf


def read_region_weather_data_from_hdfs(spark):
    weather_schema = T.StructType([
        T.StructField("region_id", T.IntegerType(), True),
        T.StructField("date", T.StringType(), True),
        T.StructField("temp_mean", T.DoubleType(), True),
        T.StructField("temp_max", T.DoubleType(), True),
        T.StructField("wind_speed_mean", T.DoubleType(), True),
        T.StructField("wind_speed_max", T.DoubleType(), True),
        T.StructField("wind_dir_mean", T.DoubleType(), True),
    ])

    try:
        return (
            spark.read
            .option("header", True)
            .schema(weather_schema)
            .csv(WEATHER_REGION_DAILY_PATH)
        )
    except Exception:
        return spark.createDataFrame([], schema=weather_schema)


def build_distinct_target_dates(spark, daily_sdf):
    date_rows = (
        daily_sdf.select("date")
        .distinct()
        .orderBy("date")
        .collect()
    )

    date_pairs = []
    for i in range(len(date_rows) - 1):
        date_pairs.append({
            "date": date_rows[i]["date"],
            "target_date": date_rows[i + 1]["date"]
        })

    if not date_pairs:
        schema = T.StructType([
            T.StructField("date", T.StringType(), True),
            T.StructField("target_date", T.StringType(), True),
        ])
        return spark.createDataFrame([], schema=schema)

    return spark.createDataFrame(date_pairs)


def main():
    spark = (
        SparkSession.builder
        .appName("WildfireHotspotPrediction")
        .config("spark.io.compression.codec", "lz4")
        .config("spark.shuffle.mapStatus.compression.codec", "lz4")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .getOrCreate()
    )

    # First step: data fetching and grid assignment on the driver, 
    # then write intermediate data to HDFS for workers to consume in parallel.
    raw = fetch_firms_area_csv(MAP_KEY, FIRMS_SOURCE, FIRMS_BBOX, FIRMS_DAY_RANGE)
    df = clean_viirs(raw)

    if df.empty:
        raise ValueError("No rows left after cleaning/filtering.")

    min_lon = df["longitude"].min() - CELL_SIZE_DEG
    max_lon = df["longitude"].max() + CELL_SIZE_DEG
    min_lat = df["latitude"].min() - CELL_SIZE_DEG
    max_lat = df["latitude"].max() + CELL_SIZE_DEG

    grid = create_grid(min_lon, min_lat, max_lon, max_lat, CELL_SIZE_DEG)
    owner_df = assign_worker_by_region(grid, num_workers=NUM_REGIONS)
    neighbor_map = build_neighbor_map(grid)
    boundary_df, halo_df = build_boundary_halo_metadata(grid, neighbor_map, owner_df)

    joined_pdf = hotspots_to_grid(df, grid)
    joined_pdf = joined_pdf.merge(owner_df[["cell_id", "region_id"]], on="cell_id", how="left")

    neighbor_rows = []
    for cell_id, neighbors in neighbor_map.items():
        for n in neighbors:
            neighbor_rows.append({
                "cell_id": int(cell_id),
                "neighbor_cell_id": int(n)
            })

    write_intermediate_data_to_hdfs(
        spark=spark,
        joined_pdf=joined_pdf,
        owner_df=owner_df,
        boundary_df=boundary_df,
        halo_df=halo_df,
        neighbor_rows=neighbor_rows
    )

    # Second stage: Read data from HDFS and perform feature engineering 
    # and prediction in parallel on the workers.
    hotspots_sdf, owner_sdf, boundary_sdf, halo_sdf, neighbor_sdf = read_intermediate_data_from_hdfs(spark)
    weather_region_sdf = read_region_weather_data_from_hdfs(spark)

    hotspots_sdf = hotspots_sdf.repartition("region_id")

    hotspots_sdf = (
        hotspots_sdf
        .withColumn("is_high_conf", F.when(F.col("confidence").isin("high", "h"), 1).otherwise(0))
        .withColumn("is_nominal_or_high", F.when(F.col("confidence").isin("nominal", "n", "high", "h"), 1).otherwise(0))
    )

    daily_sdf = (
        hotspots_sdf
        .groupBy("date", "cell_id", "region_id")
        .agg(
            F.count("*").alias("hotspot_count"),
            F.avg("frp").alias("avg_frp"),
            F.max("frp").alias("max_frp"),
            F.avg("bright_ti4").alias("avg_brightness"),
            F.max("bright_ti4").alias("max_brightness"),
            F.sum("is_high_conf").alias("high_conf_count"),
            F.sum("is_nominal_or_high").alias("nominal_or_high_count")
        )
    )

    boundary_daily_sdf = (
        daily_sdf.alias("d")
        .join(
            boundary_sdf.select("region_id", "cell_id").alias("b"),
            on=["region_id", "cell_id"],
            how="inner"
        )
        .withColumn("is_active", F.lit(1))
        .select(
            F.col("region_id").alias("source_region_id"),
            "date",
            "cell_id",
            "hotspot_count",
            "avg_frp",
            "max_frp",
            "avg_brightness",
            "max_brightness",
            "high_conf_count",
            "nominal_or_high_count",
            "is_active"
        )
    )

    halo_input_sdf = (
        halo_sdf.alias("h")
        .join(
            boundary_daily_sdf.alias("b"),
            (F.col("h.halo_cell_id") == F.col("b.cell_id")) &
            (F.col("h.halo_owner_region") == F.col("b.source_region_id")),
            how="inner"
        )
        .select(
            F.col("h.region_id").alias("region_id"),
            F.col("b.date").alias("date"),
            F.col("h.halo_cell_id").alias("cell_id"),
            F.col("b.is_active").alias("is_active")
        )
    )

    local_active_sdf = (
        daily_sdf
        .select("date", "cell_id", "region_id")
        .withColumn("is_active", F.lit(1))
    )

    combined_active_sdf = (
        local_active_sdf
        .unionByName(halo_input_sdf)
        .dropDuplicates()
    )

    neighbor_activity_sdf = (
        neighbor_sdf.alias("n")
        .join(
            combined_active_sdf.alias("a"),
            F.col("n.neighbor_cell_id") == F.col("a.cell_id"),
            how="inner"
        )
        .select(
            F.col("a.date").alias("date"),
            F.col("a.region_id").alias("region_id"),
            F.col("n.cell_id").alias("cell_id"),
            F.col("n.neighbor_cell_id").alias("active_neighbor_cell_id")
        )
    )

    burning_neighbors_sdf = (
        neighbor_activity_sdf
        .groupBy("date", "region_id", "cell_id")
        .agg(F.countDistinct("active_neighbor_cell_id").alias("burning_neighbors"))
    )

    distinct_dates_sdf = build_distinct_target_dates(spark, daily_sdf)

    weather_region_join_sdf = (
    weather_region_sdf
    .withColumnRenamed("date", "weather_date")
    )

    prediction_base_sdf = (
    owner_sdf.select("cell_id", "region_id").alias("base")
    .join(daily_sdf.alias("d"), on=["cell_id", "region_id"], how="left")
    .join(burning_neighbors_sdf.alias("bn"), on=["date", "cell_id", "region_id"], how="left")
    .join(distinct_dates_sdf.alias("td"), on="date", how="inner")
    .join(
        weather_region_join_sdf.alias("w"),
        (F.col("base.region_id") == F.col("w.region_id")) &
        (F.col("td.target_date") == F.col("w.weather_date")),
        how="left"
    )
    .drop(F.col("w.region_id"))
    .drop(F.col("w.weather_date"))
    .fillna({
        "hotspot_count": 0,
        "avg_frp": 0.0,
        "max_frp": 0.0,
        "avg_brightness": 0.0,
        "max_brightness": 0.0,
        "high_conf_count": 0,
        "nominal_or_high_count": 0,
        "burning_neighbors": 0,
        "temp_mean": 0.0,
        "temp_max": 0.0,
        "wind_speed_mean": 0.0,
        "wind_speed_max": 0.0,
        "wind_dir_mean": 0.0,
    })
)

    prediction_sdf = (
        prediction_base_sdf
        .withColumn(
            "score",
            1.5 * F.col("burning_neighbors") +
            1.5 * F.col("hotspot_count") +
            0.08 * F.col("avg_frp") +
            0.02 * F.col("max_frp") +
            0.01 * F.col("avg_brightness") +
            0.5 * F.col("high_conf_count") +
            0.25 * F.col("nominal_or_high_count") +
            0.04 * F.col("temp_max") +
            0.12 * F.col("wind_speed_max")
        )
        .withColumn(
            "predicted_fire_next_day",
            F.when(F.col("score") >= IGNITION_THRESHOLD, 1).otherwise(0)
        )
        .filter((F.col("hotspot_count") > 0) | (F.col("burning_neighbors") > 0))
        .select(
            "date",
            "target_date",
            "region_id",
            "cell_id",
            F.col("hotspot_count").alias("current_hotspot_count"),
            "burning_neighbors",
            "avg_frp",
            "max_frp",
            "avg_brightness",
            "high_conf_count",
            "temp_mean",
            "temp_max",
            "wind_speed_mean",
            "wind_speed_max",
            "wind_dir_mean",
            "score",
            "predicted_fire_next_day"
        )
    )

    print("daily rows:", daily_sdf.count())
    print("prediction rows:", prediction_sdf.count())

    prediction_sdf.write.mode("overwrite").csv(PREDICTIONS_PATH, header=True)
    daily_sdf.write.mode("overwrite").csv(DAILY_FEATURES_PATH, header=True)

    spark.stop()


if __name__ == "__main__":
    main()