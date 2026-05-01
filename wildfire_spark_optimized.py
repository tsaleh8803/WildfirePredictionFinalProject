from html import parser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel
import pandas as pd
import argparse
import glob
import time
from contextlib import contextmanager

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
CELL_SIZE_DEG = 0.25
IGNITION_THRESHOLD = 15.0


@contextmanager
def timed_stage(name, timings):
    start = time.perf_counter()
    print(f"\n=== START: {name} ===")
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        timings.append((name, elapsed))
        print(f"=== END: {name}: {elapsed:.2f} seconds ===")


def parse_args():
    parser = argparse.ArgumentParser(description="Distributed wildfire hotspot prediction pipeline")
    parser.add_argument("--day-range", type=int, default=3, help="Number of FIRMS days to fetch")
    parser.add_argument("--num-regions", type=int, default=3, help="Number of geographic ownership regions")
    parser.add_argument("--partitions", type=int, default=24, help="Spark partitions for parallel stages")
    parser.add_argument("--write-intermediates", action="store_true", help="Write intermediate Parquet files to HDFS for inspection")
    parser.add_argument("--predictions-path", default=PREDICTIONS_PATH)
    parser.add_argument("--daily-features-path", default=DAILY_FEATURES_PATH)
    parser.add_argument("--use-local-raw-fire", action="store_true", help="Use local raw_fire files instead of FIRMS API")
    parser.add_argument("--raw-fire-dir", default="raw_fire", help="Directory containing local FIRMS raw fire txt/csv files")
    return parser.parse_args()


def make_spark(partitions):
    return (
        SparkSession.builder
        .appName("WildfireHotspotPredictionOptimized")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.default.parallelism", str(partitions))
        .config("spark.io.compression.codec", "lz4")
        .config("spark.shuffle.mapStatus.compression.codec", "lz4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


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
        return spark.read.option("header", True).schema(weather_schema).csv(WEATHER_REGION_DAILY_PATH)
    except Exception:
        return spark.createDataFrame([], schema=weather_schema)


def build_distinct_target_dates_distributed(daily_sdf):
    """Avoid collect(); compute next available date using Spark window functions."""
    dates = daily_sdf.select("date").distinct()
    window = __import__("pyspark.sql.window").sql.window.Window.orderBy("date")
    return (
        dates
        .withColumn("target_date", F.lead("date").over(window))
        .filter(F.col("target_date").isNotNull())
    )


def create_intermediate_spark_dataframes(spark, joined_pdf, owner_df, boundary_df, halo_df, neighbor_rows, partitions):
    hotspots_sdf = spark.createDataFrame(joined_pdf).repartition(partitions, "region_id", "cell_id")
    owner_sdf = spark.createDataFrame(owner_df).repartition(partitions, "region_id", "cell_id")
    boundary_sdf = spark.createDataFrame(boundary_df).repartition(partitions, "region_id", "cell_id")
    halo_sdf = spark.createDataFrame(halo_df).repartition(partitions, "region_id")
    neighbor_sdf = spark.createDataFrame(pd.DataFrame(neighbor_rows)).repartition(partitions, "cell_id")
    return hotspots_sdf, owner_sdf, boundary_sdf, halo_sdf, neighbor_sdf


def write_intermediate_data_to_hdfs(hotspots_sdf, owner_sdf, boundary_sdf, halo_sdf, neighbor_sdf):
    hotspots_sdf.write.mode("overwrite").partitionBy("region_id").parquet(HOTSPOTS_PATH)
    owner_sdf.write.mode("overwrite").parquet(OWNER_PATH)
    boundary_sdf.write.mode("overwrite").parquet(BOUNDARY_PATH)
    halo_sdf.write.mode("overwrite").parquet(HALO_PATH)
    neighbor_sdf.write.mode("overwrite").parquet(NEIGHBOR_PATH)


def main():
    args = parse_args()
    timings = []
    spark = make_spark(args.partitions)

    try:
        with timed_stage("Fetch and clean FIRMS data on driver", timings):
            if args.use_local_raw_fire:
                files = sorted(glob.glob(f"{args.raw_fire_dir}/*.txt")) + sorted(glob.glob(f"{args.raw_fire_dir}/*.csv"))

                if not files:
                    raise ValueError(f"No .txt or .csv files found in {args.raw_fire_dir}")

                raw_frames = []
                for path in files:
                    print(f"Loading local fire file: {path}")
                    raw_frames.append(pd.read_csv(path))

                raw = pd.concat(raw_frames, ignore_index=True)
            else:
                raw = fetch_firms_area_csv(MAP_KEY, FIRMS_SOURCE, FIRMS_BBOX, args.day_range)

            df = clean_viirs(raw)

            print(f"FIRMS rows: {len(df)}")
            print(f"FIRMS data size: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
            print(f"FIRMS rows: {len(df)}")
            print(f"FIRMS data size: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
            if df.empty:
                raise ValueError("No rows left after cleaning/filtering.")

        with timed_stage("Build grid, ownership, boundary, and halo metadata", timings):
            min_lon = df["longitude"].min() - CELL_SIZE_DEG
            max_lon = df["longitude"].max() + CELL_SIZE_DEG
            min_lat = df["latitude"].min() - CELL_SIZE_DEG
            max_lat = df["latitude"].max() + CELL_SIZE_DEG

            grid = create_grid(min_lon, min_lat, max_lon, max_lat, CELL_SIZE_DEG)
            owner_df = assign_worker_by_region(grid, num_workers=args.num_regions)
            neighbor_map = build_neighbor_map(grid)
            boundary_df, halo_df = build_boundary_halo_metadata(grid, neighbor_map, owner_df)

            joined_pdf = hotspots_to_grid(df, grid)
            joined_pdf = joined_pdf.merge(owner_df[["cell_id", "region_id"]], on="cell_id", how="left")

            neighbor_rows = [
                {"cell_id": int(cell_id), "neighbor_cell_id": int(n)}
                for cell_id, neighbors in neighbor_map.items()
                for n in neighbors
            ]

        with timed_stage("Convert pandas metadata to Spark DataFrames", timings):
            hotspots_sdf, owner_sdf, boundary_sdf, halo_sdf, neighbor_sdf = create_intermediate_spark_dataframes(
                spark, joined_pdf, owner_df, boundary_df, halo_df, neighbor_rows, args.partitions
            )

            owner_sdf = owner_sdf.persist(StorageLevel.MEMORY_AND_DISK)
            boundary_sdf = boundary_sdf.persist(StorageLevel.MEMORY_AND_DISK)
            halo_sdf = halo_sdf.persist(StorageLevel.MEMORY_AND_DISK)
            neighbor_sdf = neighbor_sdf.persist(StorageLevel.MEMORY_AND_DISK)

        if args.write_intermediates:
            with timed_stage("Optional write intermediate data to HDFS", timings):
                write_intermediate_data_to_hdfs(hotspots_sdf, owner_sdf, boundary_sdf, halo_sdf, neighbor_sdf)

        with timed_stage("Read weather data", timings):
            weather_region_sdf = read_region_weather_data_from_hdfs(spark).repartition(args.partitions, "region_id")

        with timed_stage("Build daily fire features", timings):
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
                    F.sum("is_nominal_or_high").alias("nominal_or_high_count"),
                )
                .repartition(args.partitions, "region_id", "cell_id")
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            daily_rows = daily_sdf.count()

        with timed_stage("Build halo and burning-neighbor features", timings):
            boundary_daily_sdf = (
                daily_sdf.alias("d")
                .join(F.broadcast(boundary_sdf.select("region_id", "cell_id")).alias("b"), on=["region_id", "cell_id"], how="inner")
                .withColumn("is_active", F.lit(1))
                .select(
                    F.col("region_id").alias("source_region_id"),
                    "date",
                    "cell_id",
                    "is_active",
                )
            )

            halo_input_sdf = (
                halo_sdf.alias("h")
                .join(
                    boundary_daily_sdf.alias("b"),
                    (F.col("h.halo_cell_id") == F.col("b.cell_id"))
                    & (F.col("h.halo_owner_region") == F.col("b.source_region_id")),
                    how="inner",
                )
                .select(
                    F.col("h.region_id").alias("region_id"),
                    F.col("b.date").alias("date"),
                    F.col("h.halo_cell_id").alias("cell_id"),
                    F.col("b.is_active").alias("is_active"),
                )
            )

            local_active_sdf = daily_sdf.select("date", "cell_id", "region_id").withColumn("is_active", F.lit(1))
            combined_active_sdf = local_active_sdf.unionByName(halo_input_sdf).dropDuplicates()

            neighbor_activity_sdf = (
                neighbor_sdf.alias("n")
                .join(combined_active_sdf.alias("a"), F.col("n.neighbor_cell_id") == F.col("a.cell_id"), how="inner")
                .select(
                    F.col("a.date").alias("date"),
                    F.col("a.region_id").alias("region_id"),
                    F.col("n.cell_id").alias("cell_id"),
                    F.col("n.neighbor_cell_id").alias("active_neighbor_cell_id"),
                )
            )

            burning_neighbors_sdf = (
                neighbor_activity_sdf
                .groupBy("date", "region_id", "cell_id")
                .agg(F.countDistinct("active_neighbor_cell_id").alias("burning_neighbors"))
                .repartition(args.partitions, "region_id", "cell_id")
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            burning_neighbors_sdf.count()

        with timed_stage("Build predictions", timings):
            distinct_dates_sdf = build_distinct_target_dates_distributed(daily_sdf)
            weather_region_join_sdf = weather_region_sdf.withColumnRenamed("date", "weather_date")

            prediction_base_sdf = (
                owner_sdf.select("cell_id", "region_id").alias("base")
                .join(daily_sdf.alias("d"), on=["cell_id", "region_id"], how="left")
                .join(burning_neighbors_sdf.alias("bn"), on=["date", "cell_id", "region_id"], how="left")
                .join(distinct_dates_sdf.alias("td"), on="date", how="inner")
                .join(
                    weather_region_join_sdf.alias("w"),
                    (F.col("base.region_id") == F.col("w.region_id"))
                    & (F.col("td.target_date") == F.col("w.weather_date")),
                    how="left",
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
                    1.5 * F.col("burning_neighbors")
                    + 1.5 * F.col("hotspot_count")
                    + 0.08 * F.col("avg_frp")
                    + 0.02 * F.col("max_frp")
                    + 0.01 * F.col("avg_brightness")
                    + 0.5 * F.col("high_conf_count")
                    + 0.25 * F.col("nominal_or_high_count")
                    + 0.04 * F.col("temp_max")
                    + 0.12 * F.col("wind_speed_max")
                )
                .withColumn("predicted_fire_next_day", F.when(F.col("score") >= IGNITION_THRESHOLD, 1).otherwise(0))
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
                    "predicted_fire_next_day",
                )
                .repartition(args.partitions, "region_id")
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            prediction_rows = prediction_sdf.count()

        with timed_stage("Write outputs", timings):
            prediction_sdf.write.mode("overwrite").option("header", True).csv(args.predictions_path)
            daily_sdf.write.mode("overwrite").option("header", True).csv(args.daily_features_path)

        print("\n================ RUNTIME SUMMARY ================")
        for name, elapsed in timings:
            print(f"{name:55s} {elapsed:10.2f} seconds")
        print("-------------------------------------------------")
        print(f"daily rows: {daily_rows}")
        print(f"prediction rows: {prediction_rows}")
        print(f"regions: {args.num_regions}")
        print(f"spark partitions: {args.partitions}")
        print("=================================================\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
