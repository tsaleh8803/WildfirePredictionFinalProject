from pathlib import Path
import subprocess

import pandas as pd
import geopandas as gpd
from shapely.geometry import box

PROJECT = Path("/home/exouser/wildfire_project")
OUTPUT_DIR = PROJECT / "output"
DATA_DIR = PROJECT / "data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_PRED_CSV = OUTPUT_DIR / "predictions.csv"
GEOJSON_OUT = DATA_DIR / "predictions_map.geojson"

HDFS_PRED_PATH = "/user/exouser/wildfire/output/predictions_spark/part-*.csv"


def run(cmd):
    subprocess.run(cmd, shell=True, check=True)


def main():
    print("Copying Spark predictions from HDFS...")
    run(f"hdfs dfs -get -f {HDFS_PRED_PATH} {LOCAL_PRED_CSV}")

    pred = pd.read_csv(LOCAL_PRED_CSV)

    # This is real-time prediction data, so remove validation-only columns.
    pred = pred.drop(columns=["actual_fire_next_day"], errors="ignore")

    required = {"min_lon", "min_lat", "max_lon", "max_lat"}
    missing = required - set(pred.columns)

    if missing:
        raise ValueError(
            f"Missing required geometry columns: {missing}. "
            "Update wildfire_spark.py to include min_lon, min_lat, max_lon, max_lat in prediction output."
        )

    pred["geometry"] = pred.apply(
        lambda r: box(
            float(r["min_lon"]),
            float(r["min_lat"]),
            float(r["max_lon"]),
            float(r["max_lat"]),
        ),
        axis=1,
    )

    gdf = gpd.GeoDataFrame(pred, geometry="geometry", crs="EPSG:4326")
    gdf.to_file(GEOJSON_OUT, driver="GeoJSON")

    # Also save clean CSV locally.
    pred.drop(columns=["geometry"]).to_csv(LOCAL_PRED_CSV, index=False)

    print(f"Wrote CSV: {LOCAL_PRED_CSV}")
    print(f"Wrote GeoJSON: {GEOJSON_OUT}")
    print(f"Rows exported: {len(gdf)}")


if __name__ == "__main__":
    main()
