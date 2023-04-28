# gfs-downloader

NCEP GFS 0.25 Degree Global Forecast Grids Historical Archive: https://rda.ucar.edu/datasets/ds084.1/

Register and make an account here: https://rda.ucar.edu/login/

Create a .env file with this in (make sure to name the file .env and leave it in the root directory):

```
UCAR_EMAIL = "INSERT_EMAIL"
UCAR_PASS = "INESERT_PASSWORD"
```

### Download Operation

To see download script inputs: `poetry run Island_nwp_ext_latlon.py --help`

**Example (if using poetry):**

`poetry run python Island_nwp_ext_latlon.py Start_Date End_Date Output_Path LATMIN LATMAX LONGMIN LONGMAX`

or (for normal venv)

```
python Island_nwp_ext_latlon.py Start_Date End_Date Output_Path LATMIN LATMAX LONGMIN LONGMAX
```

Date format: YYYY-MM-DD
