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

from db_toolkit.mongo import MongoDb
from dagster import solid


@solid
def upload_to_mongo(context, df, server_cfg):
    """
    Upload panda DataFrame to MongoDB server
    :param df: DataFrame
    :param server_cfg: path to server configuration
    :return: dictionary of panda DataFrames
    :rtype: dict
    """

    client = MongoDb(cfg_filename=server_cfg)

    if client.is_authenticated():
        db = client.get_connection()[client['dbname']]
        collection = db[client['collection']]

        # convert the DataFrame to a list like [{column -> value}, … , {column -> value}]
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
        entries = df.to_dict(orient='records')

        result = collection.insert_many(entries)


    context.log.info(
        f'Loaded {len(df)} files'
    )

    return df

