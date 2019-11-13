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

from dagster import execute_pipeline, pipeline
from miscx.io import get_file_path
from load_cvs_node import load_csv_from_zip
from load_cvs_node import combine_csv_from_dict
from upload_node import upload_to_mongo




@pipeline
def airport_codes_pipeline():
    df_dict = load_csv_from_zip()
    df = combine_csv_from_dict(df_dict)
    upload_to_mongo(df)



if __name__ == '__main__':

    # get path to UNLOCODE zip file
    filename = get_file_path('UNLOCODE_PATH')
    # get path to mongodb config file
    server_cfg = get_file_path('MONGO_CFG')

    if filename is not None:
        # environment dictionary
        environment_dict = {
            'solids': {
                'load_csv_from_zip': {
                    'inputs': {
                        'zip_path': {'value': filename},
                        'pattern': {'value': r'.*UNLOCODE CodeListPart\d*\.csv'},
                        'encoding': {'value': 'latin_1'},
                        'header': {'value': ('change', 'lo', 'code', 'name_local', 'name', 'subdivision', 'function',
                                             'status', 'date', 'iata', 'geo_coord', 'remark')}
                    }
                },
                'upload_to_mongo': {
                    'inputs': {
                        'server_cfg': {'value': server_cfg}
                    }
                }
            }
        }
        result = execute_pipeline(airport_codes_pipeline, environment_dict=environment_dict)
        assert result.success
