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
from typing import List
import xarray as xr
from dotenv import load_dotenv
from tqdm import tqdm
import cfgrib

# import ecmwflibs

# Load environment variables from a .env file using load_dotenv().
load_dotenv()


def create_ds(path: Path, date: datetime, fc: int) -> xr.Dataset:
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

    # Adding relative humidity
    ds_hum = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "cfVarName": "r",
                "typeOfLevel": "isobaricInhPa",
                "stepType": "instant",
            }
        },
    )
    ds_hum = ds_hum.isel(isobaricInhPa=0).r

    ds_dp = xr.open_dataset(
        path,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"typeOfLevel": "surface", "stepType": "avg"}},
    )
    
    #Only certaiin data is available for forecast horizon 0, so we dont collect the follow data variables.
    if fc > 0:
        ds_d = ds_dp.dlwrf
        # Total percipitation
        ds_p = ds_dp.prate
        ds_dswrf = ds_dp.dswrf

        # Categorical freezing rain
        ds_cfrzr = ds_dp.cfrzr

    
    else:
        print("forecast is 0, no dlwrf,prate,dswrf,cfrzr - continuing")
        
    #The GFS service used under went an upgrade in 2021-03-22. The format of the cloud cover variables changed as a result of this.
    if date > datetime(2021,3,22):

        
        # Need to also import Total Cloud Cover for each layer
        ds_mcl = xr.open_dataset(
            path,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "cfVarName": "mcc",
                    "typeOfLevel": "middleCloudLayer",
                    "stepType": "instant",
                }
            },
        )
        #tcc to mcc
        ds_mcl = ds_mcl.rename({"mcc": "tcc_middle"})

        ds_hcl = xr.open_dataset(
            path,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "cfVarName": "hcc",
                    "typeOfLevel": "highCloudLayer",
                    "stepType": "instant",
                }
            },
        )
        #tcc to hcc
        ds_hcl = ds_hcl.rename({"hcc": "tcc_high"})

        ds_lcl = xr.open_dataset(
            path,
            engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "cfVarName": "lcc",
                    "typeOfLevel": "lowCloudLayer",
                    "stepType": "instant"
                }
            },
        )
        ds_lcl = ds_lcl.rename({"lcc": "tcc_low"})

     #No cloud cover variables were available beofre the upgrade for the forecast horizon 0. 
    elif date <= datetime(2022,3,22) and fc != 0:
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
        #tcc to mcc
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
        #tcc to hcc
        ds_hcl = ds_hcl.rename({"tcc": "tcc_high"})

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
        ds_lcl = ds_lcl.rename({"tcc": "tcc_low"})
    
    else:
        print("Invalid date or fc=0")
    
    
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

    
    if fc > 0:
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
                ds_lcl,
                ds_cfrzr,
            ],
            # any non-matching coordinates or dimensions in the input arrays will be dropped with compat="override"
            #Fine for different variables for the 0 fc and greater than 0 case.
            compat="override",
        )
    elif fc == 0:
        ds_merged = xr.merge(
            [
                ds_t,
                ds_u,
                ds_v,
                ds_hum,
                # ds_mcl,
                # ds_hcl,
                # ds_lcl,
            ],
            # any non-matching coordinates or dimensions in the input arrays will be dropped with compat="override"
            compat="override",
        )
    else:
        print("Invalid forecast")


    print(path)

    return ds_merged


def download_file(
    url: str,
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
    return file.name


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

    #Reverse lat_max lat_min as latitude goes from highest to lowest!
    #Longitude Range: Westernmost=180W Easternmost=180E
    #Latitude Range: Southernmost=90S Northernmost=90N
    
    #DETAILED COVERAGE INFORMATION0.25° x 0.25° from 0E to 359.75E and 90N to 90S (1440 x 721 Longitude/Latitude)
    
    it = ds.sel(latitude=slice(lat_max, lat_min), longitude=slice(long_min, long_max)).chunk(
        dict(latitude=9, longitude=9)
    )
    #Chunking size should be related to the breakdown of data in that dimension
     
    return it


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
        self, start_date: datetime, end_date: datetime, steps: List[int]
    ) -> Iterable[tuple[datetime, int, Path]]:
        date = start_date
        
        #Data is collected every 6 hours as thats when the forecasts are made.
        delta = timedelta(hours=6)

        while date < end_date:
            # to increase forcast add script or (3,6,9,12,...)
            for fc in steps:
                url = build_url(date, fc)
                try:
                    file = Path(download_file(url, cookies=self.cookies))
                    yield date, fc, file
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
    steps: str = typer.Option("", help="List of steps, separated by comma such as 3,6,9,12"),
) -> None:
    """
    main function: Set up the UcarDownload instance and download the
    GFS data files for the specified date range using the download_range
    method. For each file, create a Dataset using the create_ds function
    and save it as a zarr file in the destination directory. Finally, delete
    the temporary downloaded file.
    """
    steps_list = [int(step.strip()) for step in steps.split(",")] if steps else []
    missing_dates_info = []


    downloader = UcarDownload()
    files = downloader.download_range(start_date, end_date, steps_list)
    for date, fc, path in files:

        try:
            print(path)
            ds = create_ds(path, date, fc)
            it = extract_latlong(ds, lat_min, lat_max, long_min, long_max)
        except:
            print(f"An error occurred while processing data for {date}, and path{path}")
            missing_dates_info.append((date, fc, path))
            continue


        ymdh = date.strftime("%Y%m%d_%H")
        fname = f"{ymdh}_f{fc:03d}"
        it.to_zarr(dest_dir / fname, mode="w")
        print(f"Saved zarr to 'dest/{fname}'")
        path.unlink()
    
        # Print the missing dates information at the end
    print("\nMissing Dates Information:")
    for missing_date, missing_fc, missing_path in missing_dates_info:
        print(f"Date: {missing_date}, Forecast Hour: {missing_fc}, Path: {missing_path}")


if __name__ == "__main__":
    """
    The if __name__ == "__main__": block sets up the command-line arguments
    using the typer library and runs the main function when the script is executed.
    """

    typer.run(main)
    
