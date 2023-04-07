from pathlib import Path

import typer
import xarray as xr
from tqdm import tqdm


# Ensure no hidden files like .DS_Store are in the folder!!


def merge_zarrs(path: Path, out_path: Path) -> None:
    print(f"Merging zarrs from '{path}'")
    zarrs_f3 = []
    zarrs_f6 = []
    zarrs_f9 = []
    zarrs_f12 = []
    zarrs_f15 = []
    zarrs_f18 = []
    zarrs_f21 = []
    zarrs_f24 = []

    for d in tqdm(sorted(path.iterdir())):
        ds = xr.open_zarr(d)
        if "f003" in d.stem:
            zarrs_f3.append(ds)
        elif "f006" in d.stem:
            zarrs_f6.append(ds)
        elif "f009" in d.stem:
            zarrs_f9.append(ds)
        elif "f012" in d.stem:
            zarrs_f12.append(ds)
        """
        elif "f015" in d.stem:
            zarrs_f15.append(ds)
        elif "f018" in d.stem:
            zarrs_f18.append(ds)
        elif "f021" in d.stem:
            zarrs_f21.append(ds)
        elif "f024" in d.stem:
            zarrs_f24.append(ds)
        """

    ds_f3 = xr.concat(zarrs_f3, dim="time")
    ds_f6 = xr.concat(zarrs_f6, dim="time")
    ds_f9 = xr.concat(zarrs_f9, dim="time")
    ds_f12 = xr.concat(zarrs_f12, dim="time")
    """
    ds_f15 = xr.concat(zarrs_f15, dim="time")
    ds_f18 = xr.concat(zarrs_f18, dim="time")
    ds_f21 = xr.concat(zarrs_f21, dim="time")
    ds_f24 = xr.concat(zarrs_f24, dim="time")
    """
    ds_out = xr.concat(
        [ds_f3, ds_f6, ds_f9, ds_f12], dim="step"
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


# poetry run python psp/data_collection/merge_zarrv2.py ./data/saved_nwp_data/ ./data/testmerge/


# poetry run python psp/data_collection/merge_zarrv3.py ./data/latlong_filt_pre_merge/test6/ ./data/latlong_filt_pre_merge/test6merge/
