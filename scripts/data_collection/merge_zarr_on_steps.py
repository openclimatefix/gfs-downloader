from pathlib import Path

import typer
import xarray as xr
from tqdm import tqdm
from typing import List  # Add this import statement

import re

# Ensure no hidden files like .DS_Store are in the folder or log files!!

def merge_zarrs(path: Path, out_path: Path, steps: List[int]) -> None:
    print(f"Merging zarrs from '{path}'")

    zarrs = {step: [] for step in steps}

    for task_folder in tqdm(sorted(path.glob('task_*'))):
        if task_folder.is_dir():
            for d in task_folder.glob('*'):
                if d.is_dir():
                    ds = xr.open_zarr(d)
                    match = re.search(r"(\d{8}_\d{2})_f(\d{3})", d.stem)
                    if match:
                        date = match.group(1)
                        step = int(match.group(2))
                        if step in steps:
                            zarrs[step].append(ds)
                    else:
                        print(f"No step match found for directory: {d}")

    if all(len(zarrs[step]) == 0 for step in steps):
        print("No datasets found for the specified steps.")
        return

    ds_out = xr.concat([xr.concat(zarrs[step], dim="time") for step in steps if len(zarrs[step]) > 0], dim="step")
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
    steps: str = typer.Option("", help="List of steps"),
) -> None:
    steps_list = [int(step.strip()) for step in steps.split(",")] if steps else []
    merge_zarrs(path_in, path_out, steps_list)


if __name__ == "__main__":
    typer.run(main)
