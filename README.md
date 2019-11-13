# airport_codes

This project processes United Nations Code for Trade and Transport Locations (http://www.unece.org/cefact/locode/welcome.html) data to extract the codes used to identify airport.

The data is available from http://www.unece.org/cefact/codesfortrade/codes_index.html in various formats, but this project has utilised the csv version of the data.

[Dagster](https://dagster.readthedocs.io/) is utilised to:

*  Extract the data from the downloaded zip file, and upload it to a MongoDb server

## Environment

The following packages are required:
* [dagster](https://github.com/dagster-io/dagster)

    pip install dagster dagit

* [db_toolkit](https://github.com/ib-da-ncirl/db_toolkit)

    db_toolkit is available on [TestPyPI](https://test.pypi.org/project/db-toolkit/). Download either the [source distribution](https://packaging.python.org/glossary/#term-source-distribution-or-sdist) or [wheel](https://packaging.python.org/glossary/#term-wheel) file.
    
    pip install db_toolkit-X.X.X.YYY-py3-none-any.whl
    
    or 
    
    pip install db_toolkit-X.X.X.YYY.tar.gz

## Setup

* Specify the path to the UN/LOCODE zip file, in the environment variable **UNLOCODE_PATH**.
    
    Alternatively, it may be entered via the console
    
* Create a configuration file for the MongoDb server, following the format outlined in [mongo_cfg.sample](https://github.com/ib-da-ncirl/db_toolkit/blob/master/db_toolkit/docs/mongo_cfg.sample).

* Specify the path to the configuration file, in the environment variable **MONGO_CFG**.
    
    Alternatively, it may be entered via the console

## Execution

From the project root directory, in a terminal window run 

    python airport_codes.py