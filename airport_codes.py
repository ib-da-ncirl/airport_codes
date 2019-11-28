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

from db_toolkit.misc import (
    get_file_path,
    test_file_path,
    load_yaml,
)
from load_cvs_node import (
    load_csv_from_zip,
    combine_csv_from_dict
)
from dagster_toolkit.postgres import postgres_warehouse_resource
from dagster_toolkit.mongo import (
    mongo_warehouse_resource,
    download_from_mongo
)
from dagster_toolkit.environ import (
    EnvironmentDict
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

    # get path to config file
    app_cfg_path = 'config.yaml'    # default in project root
    if not test_file_path(app_cfg_path):
        # no default so look for in environment or from console
        app_cfg_path = get_file_path('AC_CFG', 'AirportCodes configuration file')
        if app_cfg_path is None:
            exit(0)

    app_cfg = load_yaml(app_cfg_path)

    if app_cfg is not None:
        # check some basic configs exist
        for key in ['airport_codes', 'postgresdb', 'mongodb']:     # required root level keys
            if key not in app_cfg.keys():
                raise EnvironmentError(f'Missing {key} configuration key')
    else:
        raise EnvironmentError(f'Missing configuration')

    # resource entries for environment_dict
    postgres_warehouse = {'config': {'postgres_cfg': app_cfg['postgresdb']}}
    mongo_warehouse = {'config': {'mongo_cfg': app_cfg['mongodb']}}

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

    def execute_csv_to_mongo_pipeline():
        """
        Execute the pipeline to upload the UN/LOCODE data from the zipped csv files to mongoDB
        """
        # environment dictionary
        env_dict = EnvironmentDict() \
            .add_solid_input('load_csv_from_zip', 'zip_path', app_cfg['airport_codes']['unlocode_zip']) \
            .add_solid_input('load_csv_from_zip', 'pattern', r'.*UNLOCODE CodeListPart\d*\.csv') \
            .add_solid_input('load_csv_from_zip', 'encoding', 'latin_1') \
            .add_solid_input('load_csv_from_zip', 'header', unlocode_header) \
            .add_resource('mongo_warehouse', mongo_warehouse) \
            .build()
        result = execute_pipeline(csv_to_mongo_pipeline, environment_dict=env_dict)
        assert result.success

    def execute_mongo_to_postgres_pipeline():
        """
        Execute the pipeline to retrieve the data from mongoDB, process and save the result to Postgres
        """
        # environment dictionary
        env_dict = EnvironmentDict() \
            .add_solid_input('download_from_mongo', 'sel_filter', {}) \
            .add_solid_input('download_from_mongo', 'projection', exclude_fields) \
            .add_resource('postgres_warehouse', postgres_warehouse) \
            .add_resource('mongo_warehouse', mongo_warehouse) \
            .build()
        result = execute_pipeline(mongo_to_postgres_pipeline, environment_dict=env_dict)
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


