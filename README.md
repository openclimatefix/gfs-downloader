# gfs-downloader

NCEP GFS 0.25 Degree Global Forecast Grids Historical Archive: https://rda.ucar.edu/datasets/ds084.1/

Register and make an account here: https://rda.ucar.edu/login/

Create a .env file with this in (make sure to name the file .env and leave it in the root directory):

```
UCAR_EMAIL = "INSERT_EMAIL"
UCAR_PASS = "INESERT_PASSWORD"
```

### Setting up the environment

```
Requirements.txt
```

Add others (dask, etc...)

### Download Operation

To see download script inputs: `poetry run Island_nwp_ext_latlon.py --help`

**Example [DEMO] (if using poetry):**

`poetry run python Island_nwp_ext_latlon.py Start_Date End_Date Output_Path LATMIN LATMAX LONGMIN LONGMAX`

or (for normal venv)

```
python Island_nwp_ext_latlon.py Start_Date End_Date Output_Path LATMIN LATMAX LONGMIN LONGMAX
```

Date format YYYY-MM-DD


To keep operation running in the background use nohup (a nohup.out file will be created to keep a log):

```
nohup python Island_nwp_ext_latlon.py Start_Date End_Date Output_Path LATMIN LATMAX LONGMIN LONGMAX &
```


Set the location of the output log, this example will create the nwp_run_v1.log at the location where the command was executed:

```
nohup python Island_nwp_ext_latlon.py Start_Date End_Date Output_Path LATMIN LATMAX LONGMIN LONGMAX >> nwp_run_v1.log 2>&1 &
```



Watch the log in the CLI live:

```
tail -f nwp_run_v1.log
```

Look at all the full logs with:

```
nano nwp_run_v1.log
```

Monitor scripts executing and uptime (This is also where you can find the PID in case the need to Kill a script) [Kill PID]:

```
ps aux | grep Island_nwp_ext_latlon.py
```




### Multitasks using parallel:

##### Generating dates:

Use the script `date_generator.py` to generated a "date_chunks" variable.

```
python date_generator.py Start_Date End_Date Num_Parallel
```

or manually set running:

```
date_chunks=$(cat <<EOF
2021-04-11 2021-06-19
2021-06-19 2021-08-27
2021-08-27 2021-11-04
2021-11-04 2022-01-18
2022-01-18 2022-03-22
2022-03-22 2022-04-07
2022-04-07 2022-05-30
2022-05-30 2022-07-15
2022-07-15 2022-08-07
2022-08-07 2022-10-15
EOF
)
```

check if either method set it correctly:

```
echo "$date_chunks"
```


Run this to create Num_Parallel tasks (Google Cloud Computing has a max of 10), stored at location Output_Path/task_{Number corresponding to execution order}.

Adjust depending on your file structure

The {1} and {2} load in the start and end date of the script.

```
echo "$date_chunks" | nohup parallel -j Num_Parallel --colsep ' ' 'python //gfs-downloader/scripts/data_collection/Island_nwp_ext_latlon.py {1} {2} //Output_Path/task_{#} LATMIN LATMAX LONGMIN LONGMAX' >> nwp_run_p1.log 2>&1 &
```
