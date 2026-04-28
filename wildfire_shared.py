
from io import StringIO
from pathlib import Path
import math
import pandas as pd
import geopandas as gpd
import requests
import json
from shapely.geometry import box

# Compute centroids of grid cells in a projected CRS for accuracy, then convert back to lat/lon.
def build_cell_centroids(grid):

    projected = grid.to_crs(epsg=3857)
    projected["geometry"] = projected.geometry.centroid
    centroids = projected.to_crs(epsg=4326)

    centroids["centroid_lon"] = centroids.geometry.x
    centroids["centroid_lat"] = centroids.geometry.y

    return centroids[["cell_id", "centroid_lat", "centroid_lon"]].copy()

# Fetch FIRMS VIIRS hotspot data for a specified area and time range, returning a DataFrame.
def fetch_firms_area_csv(map_key: str, source: str, bbox: str, day_range: int = 1) -> pd.DataFrame:
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/{source}/{bbox}/{day_range}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        return pd.DataFrame()

    df = pd.read_csv(StringIO(text))
    df["source_api"] = source
    return df


#Clean FIRMS VIIRS data, ensuring consistent types, 
# handling missing values, and filtering to the US bounding box.
def clean_viirs(
    df: pd.DataFrame,
    use_bounding_box: bool = True,
    min_lon: float = -125,
    max_lon: float = -66,
    min_lat: float = 24,
    max_lat: float = 50
) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    required = ["latitude", "longitude", "acq_date", "acq_time"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    numeric_cols = ["latitude", "longitude", "bright_ti4", "bright_ti5", "frp", "scan", "track"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "confidence" in df.columns:
        df["confidence"] = df["confidence"].astype(str).str.strip().str.lower()
    else:
        df["confidence"] = "unknown"

    def normalize_time(x):
        s = str(x).strip()
        if s in {"nan", "NaN", "None", ""}:
            return None

        if ":" in s:
            parts = s.split(":")
            if len(parts) >= 2:
                hh = parts[0].zfill(2)
                mm = parts[1].zfill(2)
                return f"{hh}:{mm}"

        if "." in s:
            s = s.split(".")[0]

        s = "".join(ch for ch in s if ch.isdigit())
        if not s:
            return None

        s = s.zfill(4)
        hh = s[:2]
        mm = s[2:]

        try:
            hh_i = int(hh)
            mm_i = int(mm)
            if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
                return f"{hh}:{mm}"
        except ValueError:
            return None

        return None

    df["acq_date"] = df["acq_date"].astype(str).str.strip()
    df["acq_time_norm"] = df["acq_time"].apply(normalize_time)

    df["timestamp"] = pd.to_datetime(
        df["acq_date"] + " " + df["acq_time_norm"],
        format="%Y-%m-%d %H:%M",
        errors="coerce"
    )

    df = df.dropna(subset=["latitude", "longitude", "timestamp"])

    if use_bounding_box:
        df = df[
            (df["longitude"] >= min_lon) & (df["longitude"] <= max_lon) &
            (df["latitude"] >= min_lat) & (df["latitude"] <= max_lat)
        ].copy()

    if "frp" not in df.columns:
        df["frp"] = 0.0
    df["frp"] = df["frp"].fillna(0.0)

    if "bright_ti4" not in df.columns:
        df["bright_ti4"] = 0.0
    df["bright_ti4"] = df["bright_ti4"].fillna(0.0)

    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
    return df

# Create a regular grid of square cells covering the specified bounding box.
def create_grid(min_lon, min_lat, max_lon, max_lat, cell_size_deg=0.25) -> gpd.GeoDataFrame:
    cells = []
    cell_id = 0

    lon = min_lon
    while lon < max_lon:
        lat = min_lat
        while lat < max_lat:
            cells.append({
                "cell_id": cell_id,
                "min_lon": lon,
                "min_lat": lat,
                "max_lon": lon + cell_size_deg,
                "max_lat": lat + cell_size_deg,
                "geometry": box(lon, lat, lon + cell_size_deg, lat + cell_size_deg)
            })
            cell_id += 1
            lat += cell_size_deg
        lon += cell_size_deg

    return gpd.GeoDataFrame(cells, geometry="geometry", crs="EPSG:4326")

# Deterministically assign each grid cell to a worker region based on its centroid.
def assign_worker_by_region(grid: gpd.GeoDataFrame, num_workers: int = 3) -> pd.DataFrame:
    owner_df = grid[["cell_id", "min_lon", "max_lon", "min_lat", "max_lat"]].copy()

    owner_df["center_lon"] = (owner_df["min_lon"] + owner_df["max_lon"]) / 2.0
    owner_df["center_lat"] = (owner_df["min_lat"] + owner_df["max_lat"]) / 2.0

    owner_df = owner_df.sort_values(
        by=["center_lon", "center_lat", "cell_id"]
    ).reset_index(drop=True)

    n = len(owner_df)
    owner_df["partition_rank"] = range(n)
    owner_df["region_id"] = ((owner_df["partition_rank"] * num_workers) // n) + 1
    owner_df["region_id"] = owner_df["region_id"].clip(upper=num_workers)

    return owner_df[
        [
            "cell_id",
            "region_id",
            "min_lon", "max_lon", "min_lat", "max_lat",
            "center_lon", "center_lat",
            "partition_rank"
        ]
    ]

# Build a mapping of each cell_id to a list of neighboring cell_ids that share a boundary.
def build_neighbor_map(grid: gpd.GeoDataFrame) -> dict:
    neighbor_map = {}
    for _, row in grid.iterrows():
        cell_id = int(row["cell_id"])
        geom = row["geometry"]
        neighbors = grid[grid.geometry.touches(geom)]["cell_id"].astype(int).tolist()
        neighbor_map[cell_id] = neighbors
    return neighbor_map

# For each cell that has neighbors in different regions, 
# create metadata about the boundary and halo cells.
def build_boundary_halo_metadata(grid, neighbor_map, owner_df):
    owner_lookup = dict(
        zip(owner_df["cell_id"].astype(int), owner_df["region_id"].astype(int))
    )

    boundary_rows = []
    halo_rows = []

    for cell_id, neighbors in neighbor_map.items():
        my_region = owner_lookup[int(cell_id)]
        foreign_neighbors = []
        foreign_regions = set()

        for n in neighbors:
            n = int(n)
            neighbor_region = owner_lookup[n]
            if neighbor_region != my_region:
                foreign_neighbors.append(n)
                foreign_regions.add(neighbor_region)

                halo_rows.append({
                    "region_id": my_region,
                    "halo_cell_id": n,
                    "halo_owner_region": neighbor_region,
                    "touches_owned_cell_id": int(cell_id)
                })

        if foreign_neighbors:
            boundary_rows.append({
                "region_id": my_region,
                "cell_id": int(cell_id),
                "num_foreign_neighbors": len(foreign_neighbors),
                "foreign_neighbor_regions": ",".join(map(str, sorted(foreign_regions)))
            })

    boundary_df = pd.DataFrame(boundary_rows).drop_duplicates()
    halo_df = pd.DataFrame(halo_rows).drop_duplicates()

    return boundary_df, halo_df

# Deterministically assign each hotspot to exactly one grid cell.
def hotspots_to_grid(df: pd.DataFrame, grid: gpd.GeoDataFrame) -> pd.DataFrame:

    if df.empty:
        return pd.DataFrame()

    required_df_cols = {"latitude", "longitude"}
    missing_df = required_df_cols - set(df.columns)
    if missing_df:
        raise ValueError(f"df missing required columns: {sorted(missing_df)}")

    required_grid_cols = {"cell_id", "min_lon", "max_lon", "min_lat", "max_lat"}
    missing_grid = required_grid_cols - set(grid.columns)
    if missing_grid:
        raise ValueError(f"grid missing required columns: {sorted(missing_grid)}")

    out = df.copy()

    global_min_lon = float(grid["min_lon"].min())
    global_max_lon = float(grid["max_lon"].max())
    global_min_lat = float(grid["min_lat"].min())
    global_max_lat = float(grid["max_lat"].max())

    unique_widths = sorted((grid["max_lon"] - grid["min_lon"]).round(10).unique())
    unique_heights = sorted((grid["max_lat"] - grid["min_lat"]).round(10).unique())

    if len(unique_widths) != 1 or len(unique_heights) != 1:
        raise ValueError("Grid is not regular; deterministic assignment requires uniform cell size.")

    cell_width = float(unique_widths[0])
    cell_height = float(unique_heights[0])

    n_cols = int(round((global_max_lon - global_min_lon) / cell_width))
    n_rows = int(round((global_max_lat - global_min_lat) / cell_height))

    if n_cols <= 0 or n_rows <= 0:
        raise ValueError("Invalid grid dimensions computed.")

    def compute_index(value, min_value, cell_size, n_bins, is_max_boundary=False):
        rel = (value - min_value) / cell_size

        # floor puts boundary values into the cell to the east/north
        idx = int(math.floor(rel))

        # If point lies exactly on the global max boundary, clamp it inward
        if is_max_boundary and abs(value - (min_value + n_bins * cell_size)) < 1e-9:
            idx = n_bins - 1

        # Clamp safely into valid range
        idx = max(0, min(idx, n_bins - 1))
        return idx

    out["col_idx"] = out["longitude"].apply(
        lambda x: compute_index(x, global_min_lon, cell_width, n_cols, is_max_boundary=True)
    )
    out["row_idx"] = out["latitude"].apply(
        lambda y: compute_index(y, global_min_lat, cell_height, n_rows, is_max_boundary=True)
    )

    # Match create_grid() numbering:
    # outer loop = lon columns
    # inner loop = lat rows
    out["cell_id"] = out["col_idx"] * n_rows + out["row_idx"]

    valid_cell_ids = set(grid["cell_id"].astype(int).tolist())
    out = out[out["cell_id"].isin(valid_cell_ids)].copy()

    return out.drop(columns=["col_idx", "row_idx"])