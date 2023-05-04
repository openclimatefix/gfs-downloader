"""
Merge step files downloaded from the other script.

We assume that the files are organized as such:

    /dir/step-0/*.zarr
    /dir/step-3/*.zarr
    ...


FIXME better docstring
"""
from pathlib import Path

import typer
import xarray as xr
from tqdm import tqdm


def _find_zarrs_in_dir(directory: Path) -> list[Path]:
    return [x for x in directory.iterdir() if x.suffix == '.zarr']

def main(
    path_in: Path,
    path_out: Path,
) -> None:
    step_dirs = [x for x in path_in.iterdir() if x.isdir()]
    step_datasets: list[xr.Dataset] = []
    for step_dir in step_dirs:
        print(f'Treating dir {step_dir}')
        dataset_paths = _find_zarrs_in_dir(step_dir)
        datasets = [xr.open_zarr(d) for d in dataset_paths]
        # Merge the steps along the time dimension.
        # FIXME make sure they are in order! This could be done by sorting on the file names and
        # making sure that they are guaranteed to be in order.
        merged = xr.concat(datasets, dim='time')
        step_datasets.append(merged)

    # Merge all the step datasets in the step dimension
    merged_dataset = xr.concat(step_dataset, dim='step')

    print(f"Saving single zarr to '{out_path}'")
    ds_out.to_zarr(
        out_path,
        encoding={
            "time": {"dtype": "int64", "_FillValue": -1},
            "valid_time": {"dtype": "int64", "_FillValue": -1},
        },
    )

if __name__ == "__main__":
    typer.run(main)
