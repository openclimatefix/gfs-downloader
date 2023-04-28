from pathlib import Path

import typer
import xarray as xr
from tqdm import tqdm


# Ensure no hidden files like .DS_Store are in the folder or log files!!
# (3,6,9,12,18,24,36,48,60,72)

def merge_zarrs(path: Path, out_path: Path) -> None:
    print(f"Merging zarrs from '{path}'")
    zarrs_f3 = []
    zarrs_f6 = []
    """
    zarrs_f9 = []
    zarrs_f12 = []
    zarrs_f18 = []
    zarrs_f24 = []
    zarrs_f36 = []
    zarrs_f48 = []
    zarrs_f60 = []
    zarrs_f72 = []
    """


    for task_folder in tqdm(sorted(path.glob('task_*'))):
        if task_folder.is_dir():
            for d in task_folder.glob('*'):
        # for d in tqdm(sorted(path.iterdir()))::
                if d.is_dir():
                    ds = xr.open_zarr(d)
                    if "f003" in d.stem:
                        zarrs_f3.append(ds)
                    elif "f006" in d.stem:
                        zarrs_f6.append(ds)
                    """"
                    elif "f009" in d.stem:
                        zarrs_f9.append(ds)
                    elif "f012" in d.stem:
                        zarrs_f12.append(ds)
                    elif "f018" in d.stem:
                        zarrs_f18.append(ds)
                    elif "f024" in d.stem:
                        zarrs_f24.append(ds)
                    elif "f036" in d.stem:
                        zarrs_f36.append(ds)
                    elif "f048" in d.stem:
                        zarrs_f48.append(ds)
                    elif "f060" in d.stem:
                        zarrs_f60.append(ds)
                    elif "f072" in d.stem:
                        zarrs_f72.append(ds)
                    """

    

    ds_f3 = xr.concat(zarrs_f3, dim="time")
    ds_f6 = xr.concat(zarrs_f6, dim="time")
    """
    ds_f9 = xr.concat(zarrs_f9, dim="time")
    ds_f12 = xr.concat(zarrs_f12, dim="time")
    ds_f18 = xr.concat(zarrs_f18, dim="time")
    ds_f24 = xr.concat(zarrs_f24, dim="time")
    ds_f36 = xr.concat(zarrs_f36, dim="time")
    ds_f48 = xr.concat(zarrs_f48, dim="time")
    ds_f60 = xr.concat(zarrs_f60, dim="time")
    ds_f72 = xr.concat(zarrs_f72, dim="time")
    """
    

    ds_out = xr.concat(
        [ds_f3, ds_f6], dim = "step" # ds_f9, ds_f12, ds_f18, ds_f24, ds_f36, ds_f48, ds_f60, ds_f72], dim="step"
    )  # ds_f15, ds_f18, ds_f21, ds_f24 ], dim="step")
    print(f"Saving single zarr to '{out_path}'")
    ds_out.to_zarr(
        out_path,
        encoding={
            "time": {"dtype": "int64", "_FillValue": -1},
            "valid_time": {"dtype": "int64", "_FillValue": -1},
        },
    )


def main(
    path_in: Path,
    path_out: Path,
) -> None:
    merge_zarrs(path_in, path_out)


if __name__ == "__main__":
    typer.run(main)
