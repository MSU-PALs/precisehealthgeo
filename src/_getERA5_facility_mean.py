import os
import pandas as pd
import geopandas as gpd
import ee
import geemap

# Authenticate and initialise Earth Engine API
try:
    ee.Initialize(project="precise-413717")
except Exception:
    ee.Authenticate()
    ee.Initialize(project="precise-413717")

# define study period (from anc records)
start = "2018-01-01"
end = "2024-12-31"

# shapefile path
neighborhoods = r"../data/external/"

# read neighborhoods shapefile
gdf = gpd.read_file(
    os.path.join(neighborhoods, "PRECISE_Health_Facilities.shp"),
).rename(columns={"facility_c": "facility_code", "facility_n": "village_name"})


for country in ["gambia"]:
    outdir = os.path.join(r"../data/interim/", country.capitalize())
    os.makedirs(outdir, exist_ok=True)

    # filter for country
    gdf = gdf[gdf["Country"] == country.capitalize()]

    # convert gdf to ee feature collection
    fc = geemap.gdf_to_ee(gdf)

    # load ERA5-Land dataset
    images = (
        ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR")
        .filterDate(start, end)
        # select ambient temperature at 2m
        .select("temperature_2m")
        # convert Kelvin to Celsius
        .map(
            lambda img: img.subtract(273.15).copyProperties(img, ["system:time_start"])
        )
    )

    # function to extract daily temperature for each centroid
    def extract_temperature(image):
        exposure_date = ee.Date(image.get("system:time_start")).format("YYYY-MM-dd")
        # extract temperature at centroid location
        reduced = image.sampleRegions(
            collection=fc,
            scale=10000,  # ~9km resolution of ERA5-Land
            geometries=False,
        )
        return reduced.map(lambda feature: feature.set("exposure_day", exposure_date))

    # extract for study period for each centroid
    dataset = images.map(extract_temperature).flatten()

    # # convert to pandas dataframe
    dataset = geemap.ee_to_df(dataset).rename(columns={"temperature_2m": "ERA-5_mean"})

    dataset.to_pickle(os.path.join(outdir, "ERA-5_daily_facility_mean.pkl"))
