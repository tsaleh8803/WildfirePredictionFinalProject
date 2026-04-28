import re
import time
import pandas as pd
import requests
from pyspark.sql import SparkSession

from wildfire_shared import (
    fetch_firms_area_csv,
    clean_viirs,
    create_grid,
    hotspots_to_grid,
    assign_worker_by_region,
)

MAP_KEY = "b09dbfb7dcc38d00d2105b9459473005"
FIRMS_SOURCE = "VIIRS_NOAA21_NRT"
FIRMS_BBOX = "-125,24,-66,50"
FIRMS_DAY_RANGE = 3
CELL_SIZE_DEG = 0.25
NUM_REGIONS = 3

HDFS_BASE = "hdfs:///user/exouser/wildfire"
WEATHER_REGION_DAILY_PATH = f"{HDFS_BASE}/weather_region_daily"

HEADERS = {
    "User-Agent": "wildfire-prediction-project (exouser@example.com)",
    "Accept": "application/geo+json",
}

DIR_TO_DEG = {
    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
    "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
    "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5
}

# Parses wind speed strings like "5 mph" or "5 to 10 mph" 
# and returns the average speed in mph.
def parse_wind_speed(speed_str):
    if not speed_str or pd.isna(speed_str):
        return None
    nums = [float(x) for x in re.findall(r"\d+\.?\d*", str(speed_str))]
    if not nums:
        return None
    return sum(nums) / len(nums)

# Parses wind direction strings like "NW" and returns the corresponding degree value.
def parse_wind_direction(dir_str):
    if not dir_str or pd.isna(dir_str):
        return None
    return DIR_TO_DEG.get(str(dir_str).strip().upper())

# Fetches NWS point metadata for the given latitude and longitude.
def get_nws_point_metadata(lat, lon):
    url = f"https://api.weather.gov/points/{lat},{lon}"
    r = requests.get(url, headers=HEADERS, timeout=30)

    if r.status_code == 404:
        return None

    r.raise_for_status()
    props = r.json()["properties"]

    return {
        "forecast_hourly_url": props["forecastHourly"],
        "grid_id": props["gridId"],
        "grid_x": props["gridX"],
        "grid_y": props["gridY"],
    }

# Fetches the hourly forecast from the given NWS forecast URL and returns the JSON response.
def get_nws_hourly_forecast(forecast_hourly_url):
    r = requests.get(forecast_hourly_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

# Builds a list of hourly weather rows for the given region and representative point.
def build_hourly_weather_rows(region_id, lat, lon):
    meta = get_nws_point_metadata(lat, lon)
    if meta is None:
        return []

    forecast = get_nws_hourly_forecast(meta["forecast_hourly_url"])
    periods = forecast.get("properties", {}).get("periods", [])

    rows = []
    for p in periods:
        rows.append({
            "region_id": int(region_id),
            "timestamp": p.get("startTime"),
            "temperature": p.get("temperature"),
            "wind_speed_mph": parse_wind_speed(p.get("windSpeed")),
            "wind_dir_deg": parse_wind_direction(p.get("windDirection")),
        })
    return rows

# Aggregates hourly weather data into daily summaries for each region.
def aggregate_daily_weather(hourly_df):
    if hourly_df.empty:
        return pd.DataFrame(columns=[
            "region_id", "date",
            "temp_mean", "temp_max",
            "wind_speed_mean", "wind_speed_max",
            "wind_dir_mean"
        ])

    df = hourly_df.copy()
    # keep the local forecast day from the original timestamp string
    df["date"] = df["timestamp"].astype(str).str[:10]

    # parse timestamp safely for any later use
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    daily = (
        df.groupby(["region_id", "date"], as_index=False)
        .agg(
            temp_mean=("temperature", "mean"),
            temp_max=("temperature", "max"),
            wind_speed_mean=("wind_speed_mph", "mean"),
            wind_speed_max=("wind_speed_mph", "max"),
            wind_dir_mean=("wind_dir_deg", "mean"),
        )
    )
    return daily

# For each region, pick one representative active cell centroid to fetch weather data from.
def build_region_representative_points(joined_pdf, owner_df, grid):
    """
    Pick one representative active cell centroid per region.
    Computes centroids directly from geometry.
    """

    # compute centroids safely
    projected = grid.to_crs(epsg=3857).copy()
    projected["geometry"] = projected.geometry.centroid
    centroids = projected.to_crs(epsg=4326)

    centroids["center_lon"] = centroids.geometry.x
    centroids["center_lat"] = centroids.geometry.y

    grid_lookup = centroids[["cell_id", "center_lat", "center_lon"]].copy()

    active_cells = (
        joined_pdf[["cell_id"]]
        .drop_duplicates()
        .merge(owner_df[["cell_id", "region_id"]], on="cell_id", how="left")
    )

    reps = (
        active_cells
        .merge(grid_lookup, on="cell_id", how="left")
        .sort_values(["region_id", "cell_id"])
        .groupby("region_id", as_index=False)
        .first()
    )

    return reps[["region_id", "center_lat", "center_lon"]].copy()


def main():
    spark = (
        SparkSession.builder
        .appName("FetchNWSWeatherRegion")
        .config("spark.io.compression.codec", "lz4")
        .config("spark.shuffle.mapStatus.compression.codec", "lz4")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .getOrCreate()
    )

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

    joined_pdf = hotspots_to_grid(df, grid)
    joined_pdf = joined_pdf.merge(owner_df[["cell_id", "region_id"]], on="cell_id", how="left")

    rep_points = build_region_representative_points(joined_pdf, owner_df, grid)

    print("region representative points:")
    print(rep_points)

    all_rows = []
    errors = []

    for idx, row in rep_points.iterrows():
        region_id = int(row["region_id"])
        lat = float(row["center_lat"])
        lon = float(row["center_lon"])

        print(f"fetching weather for region {region_id} ({idx + 1}/{len(rep_points)})", flush=True)

        try:
            rows = build_hourly_weather_rows(region_id, lat, lon)
            all_rows.extend(rows)
        except Exception as e:
            print(f"error for region {region_id}: {e}", flush=True)
            errors.append({
                "region_id": region_id,
                "lat": lat,
                "lon": lon,
                "error": str(e)
            })

        time.sleep(0.2)

    hourly_df = pd.DataFrame(all_rows)
    daily_weather_df = aggregate_daily_weather(hourly_df)

    print("weather daily rows:", len(daily_weather_df))
    print("weather errors:", len(errors))

    if daily_weather_df.empty:
        print("No weather rows were collected; skipping write.")
        spark.stop()
        return

    weather_sdf = spark.createDataFrame(daily_weather_df)

    (
        weather_sdf.write
        .mode("overwrite")
        .option("header", True)
        .csv(WEATHER_REGION_DAILY_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()