"""
This code is a script that downloads, processes, 
and stores weather forecast data from the University 
Corporation for Atmospheric Research (UCAR) GFS dataset. 
It uses the xarray library to process the data and typer library 
to handle command-line arguments.
"""

# Import the required libraries.
import os
import tempfile
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import httpx
import typer
import xarray as xr
from dotenv import load_dotenv
from tqdm import tqdm
import cfgrib

# import ecmwflibs

# Load environment variables from a .env file using load_dotenv().
load_dotenv()


def create_ds(path: Path) -> xr.Dataset:
    """
    create_ds function: Create an xarray Dataset
    from a GRIB file using the cfgrib engine.
    It extracts specific variables (temperature, u and v
    wind components, downward longwave radiation flux, and
    precipitation rate) and merges them into a single Dataset.
    """

    # Surface air temperature
    ds_t = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "t",
                "typeOfLevel": "surface",
                "stepType": "instant",
            }
        },
    )
    ds_t = ds_t.t

    # Surface snow depth
    ds_snow = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "sde",
                "typeOfLevel": "surface",
                "stepType": "instant",
            }
        },
    )
    ds_snow = ds_snow.sde

    # Surface sunshine
    ds_sunsd = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "unknown",
                "typeOfLevel": "surface",
            }
        },
    )
    ds_sunsd = ds_sunsd.rename({"SUNSD": "surfaceSunShineDuration"})

    # Adding relative humidity
    ds_hum = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "r",
                "typeOfLevel": "isobaricInhPa",
                # "stepType": "instant",
            }
        },
    )
    ds_hum = ds_hum.isel(isobaricInhPa=0).r

    ds_dp = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"typeOfLevel": "surface", "stepType": "avg"}},
    )
    ds_d = ds_dp.dlwrf
    ds_p = ds_dp.prate
    ds_dswrf = ds_dp.dswrf
    # Total percipitation
    # Issues with TP
    # ds_tp = ds_dp.tp
    # Categorical freezing rain
    ds_cfrzr = ds_dp.cfrzr

    # Need to also import Total Cloud Cover for each layer
    ds_mcl = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "unknown",
                "typeOfLevel": "middleCloudLayer",
            }
        },
    )
    ds_mcl = ds_mcl.rename({"tcc": "tcc_middle"})

    ds_hcl = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "unknown",
                "typeOfLevel": "highCloudLayer",
            }
        },
    )
    ds_hcl = ds_hcl.rename({"tcc": "tcc_high"})

    ds_acl = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "unknown",
                "typeOfLevel": "atmosphere",
            }
        },
    )
    ds_acl = ds_acl.rename({"tcc": "tcc_atmosphere"})

    ds_lcl = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "unknown",
                "typeOfLevel": "lowCloudLayer",
            }
        },
    )
    ds_lcl = ds_lcl.rename({"tcc": "tcc_lowCloudLayer"})

    ds_blcl = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "unknown",
                "typeOfLevel": "boundaryLayerCloudLayer",
            }
        },
    )
    ds_blcl = ds_blcl.rename({"tcc": "tcc_boundaryLayer"})

    ds_ccl = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "unknown",
                "typeOfLevel": "convectiveCloudLayer",
            }
        },
    )
    ds_ccl = ds_ccl.rename({"tcc": "tcc_convectiveLayer"})

    ds_v = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {"cfVarName": "v", "typeOfLevel": "isobaricInhPa"},
            "indexpath": "",
        },
    )
    ds_v = ds_v.isel(isobaricInhPa=0).v

    ds_u = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {"cfVarName": "u", "typeOfLevel": "isobaricInhPa"},
            "indexpath": "",
        },
    )
    ds_u = ds_u.isel(isobaricInhPa=0).u

    # I addded compat='override', however instead i could create different collumn names to identify the data
    ds_merged = xr.merge(
        [
            ds_t,
            ds_u,
            ds_v,
            ds_d,
            ds_p,
            ds_dswrf,
            ds_hum,
            ds_mcl,
            ds_hcl,
            ds_ccl,
            ds_blcl,
            ds_lcl,
            ds_acl,
            ds_cfrzr,
        ],
        compat="override",
    )

    print(path)

    return ds_merged


def download_file(
    url: str,
    file_path: str,
    headers: Optional[dict] = None,
    cookies: Optional[httpx.Cookies] = None,
) -> str:
    """
    download_file function: Download a file from a given URL
    using the httpx library. It streams the file and saves it
    to a temporary location while displaying a progress bar using tqdm.
    """

    with tempfile.NamedTemporaryFile(delete=False) as file:
        with httpx.stream("GET", url, headers=headers, cookies=cookies, timeout=30) as res:
            total = int(res.headers["Content-Length"])
            with tqdm(total=total, unit_scale=True, unit_divisor=1024, unit="B") as progress:
                num_bytes_downloaded = res.num_bytes_downloaded
                for chunk in res.iter_bytes():
                    file.write(chunk)
                    progress.update(res.num_bytes_downloaded - num_bytes_downloaded)
                    num_bytes_downloaded = res.num_bytes_downloaded
    return file_path


def build_url(date: datetime, fc: int) -> str:
    """
    build_url function: Generate a URL for the GFS dataset
    based on a given datetime and forecast hour. The dataset
    is hosted on UCAR's RDA (Research Data Archive) server.
    """
    base_url = "https://rda.ucar.edu/data/ds084.1"
    ymd = date.strftime("%Y%m%d")
    return f"{base_url}/{date.year}/{ymd}/gfs.0p25.{ymd}{date.hour:02d}.f{fc:03d}.grib2"


def extract_latlong(
    ds: xr.Dataset, lat_min: float, lat_max: float, long_min: float, long_max: float
) -> None:

    it = ds.sel(latitude=slice(lat_max, lat_min), longitude=slice(long_min, long_max))
    # longitude=slice(long_min, long_max)
    # This just determines the size of the chunkingin each stage of the code.
    # dict(latitude=49, longitude=53)

    return it


# latitude goes from highest to lowest!


class UcarDownload:

    """
    UcarDownload class: Handle authentication and downloading of
    GFS data files from UCAR's RDA server. The __init__ method initializes
    the object and obtains the authentication cookies. The auth method handles the
    authentication process. The download_range method downloads GFS data files for a
    specified range of dates and yields the date, forecast hour, and the downloaded file's path.
    """

    def __init__(self) -> None:
        self.cookies = self.auth()

    def auth(self) -> httpx.Cookies:
        auth_url = "https://rda.ucar.edu/cgi-bin/login"
        auth_data = {
            "email": os.environ["UCAR_EMAIL"],
            "passwd": os.environ["UCAR_PASS"],
            "action": "login",
        }
        res = httpx.post(auth_url, data=auth_data)
        assert res.status_code == 200
        return res.cookies

    def download_range(
        self, start_date: datetime, end_date: datetime, dest_dir: Path
    ) -> Iterable[tuple[datetime, int, Path]]:
        date = start_date
        delta = timedelta(hours=6)

        while date < end_date:
            # XXX increase forcast to 3 - 72, do ending 73 as inclusive
            for fc in (3,6,9,12,18,24,36,48,60,72):
                url = build_url(date, fc)
                try:
                    file_path = dest_dir / f"{date:%Y%m%d}_{fc:03d}.grib2"
                    download_file(url, cookies=self.cookies, file_path=str(file_path))
                    yield date, fc, file_path
                except KeyError as e:
                    print(f"Failed for {date=}, {fc=} with {e}")
            date += delta


def main(
    start_date: datetime,
    end_date: datetime,
    dest_dir: Path,
    lat_min: float,
    lat_max: float,
    long_min: float,
    long_max: float,
) -> None:
    """
    main function: Set up the UcarDownload instance and download the
    GFS data files for the specified date range using the download_range
    method. For each file, create a Dataset using the create_ds function
    and save it as a zarr file in the destination directory. Finally, delete
    the temporary downloaded file.
    """

    downloader = UcarDownload()
    files = downloader.download_range(start_date, end_date,dest_dir)
    for date, fc, path in files:
        print(path)

        #ds = create_ds(path)

        #it = extract_latlong(ds, lat_min, lat_max, long_min, long_max)

        #ymdh = date.strftime("%Y%m%d_%H")
        #fname = f"{ymdh}_f{fc:03d}"

        #ds here changed from it as removing lat long extraction 
        #ds.to_zarr(dest_dir / fname, mode="w")
        #print(f"Saved zarr to 'dest/{fname}'")
        #path.unlink()


# poetry run psp/data_collection/IslandPV_NWPv2.py --help
# poetry run psp/data_collection/IslandPV_NWPv2.py START END DEST LATMIN LATMAX LONGMIN LONGMAX

# For Island A specifically:     13 15 35 37

# poetry run psp/data_collection/IslandPV_NWPv2.py 2018-01-01 2018-01-03  LATMIN LATMAX LONGMIN LONGMAX

# /Users/zakwatts/Coding/OCF/pv-site-prediction/data/latlong_filt_pre_merge/Test1

# poetry run python psp/data_collection/IslandPV_NWPv5.py 2018-01-01 2018-01-02 /Users/zakwatts/Coding/OCF/pv-site-prediction/data/latlong_filt_pre_merge/test6 13 15 35 37

if __name__ == "__main__":
    """
    The if __name__ == "__main__": block sets up the command-line arguments
    using the typer library and runs the main function when the script is executed.
    """

    typer.run(main)
