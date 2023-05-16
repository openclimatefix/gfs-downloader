"""
Once merged, our zarr dataset in in a format where we have one (xarray) *Data variable* per NWP
variable.
This script put those into a new *Dimension* instead.
We end up with a 5D tensor with dimensions: (time, step, lat, lon, variable).
"""

import argparse
import pathlib

import xarray as xr


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input",
        type=pathlib.Path,
        help="Input zarr file, typically the output of the merging script",
    )
    parser.add_argument("output", type=pathlib.Path, help="Output zarr file")

    return parser.parse_args()


def main():
    args = _parse_args()

    if args.output.exists():
        raise RuntimeError(f'Output file "{args.output}" already exist')

    d = xr.open_dataset(args.input, engine="zarr")

    # Remove the attributes.
    d.attrs = {}

    # Remove the extra coordinates (only keep the dimensions).
    dims = set(d.dims)
    coords = set(d.coords)
    remove_coords = coords.difference(dims)
    for coord in remove_coords:
        del d.coords[coord]

    # Make the "variables" into a new Dimension.
    # Note that this returns a xr.DataArray.
    var_names = d.data_vars
    d2 = xr.concat([d[v] for v in var_names], dim="variable")

    # Set the coordinates to keep the names of the variables.
    d2 = d2.assign_coords(variable=("variable", var_names))

    # Turn the xr.DataArray into a xr.Dataset.
    d = xr.Dataset(dict(value=d2))

    # Chunk the dataset.
    # TODO This chunking is optimized for the "Island" use-case, they should be made customizable
    # for other use-cases.
    d = d.chunk(dict(time=20))


    d.to_zarr(args.output)


if __name__ == "__main__":
    main()