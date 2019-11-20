# The MIT License (MIT)
# Copyright (c) 2019 Ian Buttimer

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from dagster import (
    execute_pipeline,
    pipeline,
    ModeDefinition
)
from menu import Menu

from db_toolkit.misc.get_env import get_file_path
from load_cvs_node import (
    load_csv_from_zip,
    combine_csv_from_dict
)
from dagster_toolkit.postgres import postgres_warehouse_resource
from dagster_toolkit.mongo import (
    mongo_warehouse_resource,
    download_from_mongo
)
from upload_node import (
    upload_to_mongo,
    upload_to_postgres
)
from process_node import process_unlocode

"""
    For information regarding UN/LOCODE, see https://www.unece.org/cefact/locode/welcome.html
    For information regarding the data format, see https://service.unece.org/trade/locode/UNLOCODE_Manual.pdf and 
    https://service.unece.org/trade/locode/2019-1_UNLOCODE_SecretariatNotes.pdf
"""


@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'mongo_warehouse': mongo_warehouse_resource
            }
        )
    ]
)
def csv_to_mongo_pipeline():
    df_dict = load_csv_from_zip()
    df = combine_csv_from_dict(df_dict)
    upload_to_mongo(df)


@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'postgres_warehouse': postgres_warehouse_resource,
                'mongo_warehouse': mongo_warehouse_resource
            }
        )
    ]
)
def mongo_to_postgres_pipeline():
    raw = download_from_mongo()
    processed = process_unlocode(raw)
    upload_to_postgres(processed)


if __name__ == '__main__':

    # get path to UNLOCODE zip file
    filename = get_file_path('UNLOCODE_PATH', 'UN/LOCODE zip file')
    if filename is None:
        exit(0)
    # get path to mongodb config file
    mongo_cfg = get_file_path('MONGO_CFG', 'mongoDB configuration file')
    if mongo_cfg is None:
        exit(0)
    # get path to postgres config file
    postgres_cfg = get_file_path('POSTGRES_CFG', 'Postgres configuration file')
    if postgres_cfg is None:
        exit(0)

    # resource entries for environment_dict
    postgres_warehouse = {'config': {'postgres_cfg': postgres_cfg}}
    mongo_warehouse = {'config': {'mongo_cfg': mongo_cfg}}

    # names of columns in UN/LOCODE data
    unlocode_header = (
        'change',       # change indicator that shows if the entry has been modified in any way
        'lo',           # the ISO 3166 alpha-2 Country Code
        'code',         # a 3-character code for the place name
        'name_local',   # place name, whenever possible, in their national language
        'name',         # place name, without diacritic signs
        'subdivision',  # ISO 1-3 character code for the administrative division of the country, as per ISO 3166-2/1998
        'function',     # 1-digit function classifier code for the location
        'status',       # status of the entry
        'date',         # reference date, showing the year and month of request
        'iata',         # IATA code for the location if different from location code
        'geo_coord',    # geographical coordinates (latitude/longitude), ddmmN dddmmW, ddmmS dddmmE, etc., where the
                        # two last digits refer to minutes and the two or three first digits indicate the degrees
        'remark'        # reasons for the change
    )
    # field not required from mongo
    exclude_fields = {'_id': 0, 'change': 0, 'subdivision': 0, 'status': 0, 'date': 0, 'remark': 0}

    if filename is not None:

        def execute_csv_to_mongo_pipeline():
            """
            Execute the pipeline to upload the UN/LOCODE data from the zipped csv files to mongoDB
            """
            # environment dictionary
            csv_to_mongo_env_dict = {
                'solids': {
                    'load_csv_from_zip': {
                        'inputs': {
                            'zip_path': {'value': filename},
                            'pattern': {'value': r'.*UNLOCODE CodeListPart\d*\.csv'},
                            'encoding': {'value': 'latin_1'},
                            'header': {'value': unlocode_header}
                        }
                    }
                },
                'resources': {
                    'mongo_warehouse': mongo_warehouse
                }
            }
            result = execute_pipeline(csv_to_mongo_pipeline, environment_dict=csv_to_mongo_env_dict)
            assert result.success

        def execute_mongo_to_postgres_pipeline():
            """
            Execute the pipeline to retrieve the data from mongoDB, process and save the result to Postgres
            """
            # environment dictionary
            mongo_to_postgres_env_dict = {
                'solids': {
                    'download_from_mongo': {
                        'inputs': {
                            'sel_filter': {'value': {}},
                            'projection': {'value': exclude_fields}
                        }
                    }
                },
                'resources': {
                    'postgres_warehouse': postgres_warehouse,
                    'mongo_warehouse': mongo_warehouse
                }
            }
            result = execute_pipeline(mongo_to_postgres_pipeline, environment_dict=mongo_to_postgres_env_dict)
            assert result.success

        menu = Menu()
        menu.set_options([
            ("Save UN/LOCODE raw data to MongoDb", execute_csv_to_mongo_pipeline),
            ("Process UN/LOCODE raw data from MongoDb, and save to Postgres", execute_mongo_to_postgres_pipeline),
            ("Exit", Menu.CLOSE)
        ])
        menu.set_title("UN/LOCODE Data Processing Menu")
        menu.set_title_enabled(True)
        menu.open()


