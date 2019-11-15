# airport_codes

This project processes United Nations Code for Trade and Transport Locations (http://www.unece.org/cefact/locode/welcome.html) data to extract the codes used to identify airport.

The data is available from http://www.unece.org/cefact/codesfortrade/codes_index.html in various formats, but this project has utilised the csv version of the data.

[Dagster](https://dagster.readthedocs.io/) is utilised to:

*  Extract the data from the downloaded zip file, and upload it to a mongoDb server
*  Download the data from a mongoDb server, process it and upload to a Postgres server

## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

The following packages are required:
* [dagster](https://github.com/dagster-io/dagster)
* [db_toolkit](https://github.com/ib-da-ncirl/db_toolkit)
* [dagster_toolkit](https://github.com/ib-da-ncirl/db_toolkit)
* [Menu](https://pypi.org/project/Menu/)

Install dependencies via

    pip install -r requirements.txt


## Setup

* Specify the path to the UN/LOCODE zip file, in the environment variable **UNLOCODE_PATH**.
* Create a configuration file for the mongoDb server, following the format outlined in [mongo_cfg.sample](https://github.com/ib-da-ncirl/db_toolkit/blob/master/db_toolkit/docs/mongo_cfg.sample).
* Specify the path to the configuration file, in the environment variable **MONGO_CFG**.
* Create a configuration file for the Postgres server, following the format outlined in [postgres_cfg.sample](https://github.com/ib-da-ncirl/db_toolkit/blob/master/db_toolkit/docs/postgres_cfg.sample).
* Specify the path to the configuration file, in the environment variable **POSTGRES_CFG**.
* Alternatively, each parameter may be entered via the console.

## Execution

From the project root directory, in a terminal window run 

    python airport_codes.py