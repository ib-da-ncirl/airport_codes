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
* [pandas](https://pandas.pydata.org/)
* [db_toolkit](https://github.com/ib-da-ncirl/db_toolkit)
* [dagster_toolkit](https://github.com/ib-da-ncirl/db_toolkit)
* [Menu](https://pypi.org/project/Menu/)

Install dependencies via

    pip install -r requirements.txt

## Setup

The application may be supplied by one of the following methods:
* Create a YAML configuration file named config.yaml, based on [sample.yaml](airport_codes/doc/sample.yaml), in the project root.
* Specify the path to the YAML configuration file, in the environment variable **AC_CFG**.
* Enter the path to the YAML configuration file via the console.

## Execution

From the project root directory, in a terminal window run 

    python airport_codes.py