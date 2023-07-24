# candidates etl and dataviz
 This repo contains the setup of a database and a ETL process to load a CSV data sample to it and then create dataviz for it.

## Setup
 ### Setup Virtual Environment
 To create, activate and setup a virtual environment run:
 ```bash
 python3.11 -m venv ./venv
 source ./venv/bin/activate
 pip install --U pip
 pip install -r requirements.txt
```

### Add Postgres spark jar
Download it from the following [link](https://jdbc.postgresql.org/download/postgresql-42.5.4.jar) and then add it to the `./spark/jars` folder.

