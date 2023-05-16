import xarray as xr
from pathlib import Path
import typer

def merge_zarr_files(path_in1: Path, path_in2: Path, path_out: Path, merge_dim: str):
    # Open the input Zarr files as xarray Datasets
    ds1 = xr.open_zarr(str(path_in1))
    ds2 = xr.open_zarr(str(path_in2))

    # Merge the two Datasets
    ds_merged = xr.concat([ds1, ds2], dim=merge_dim)

    # Save the merged Dataset as a Zarr file
    ds_merged.to_zarr(str(path_out), mode='w')

def main(
    path_in1: Path,
    path_in2: Path,
    path_out: Path,
    merge_dim: str = typer.Option("", help="Dimension to merge on - (Either 'step' or 'time')")
) -> None:
    merge_zarr_files(path_in1, path_in2, path_out, merge_dim)

if __name__ == "__main__":
    typer.run(main)
