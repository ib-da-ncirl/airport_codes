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

from dagster import solid
from db_toolkit.postgres.postgresdb_sql import does_table_exist_sql
from db_toolkit.postgres.postgresdb_sql import count_sql
from db_toolkit.postgres.postgresdb_sql import estimate_count_sql
from psycopg2.extras import execute_values


@solid(required_resource_keys={'mongo_warehouse'})
def upload_to_mongo(context, df):
    """
    Upload panda DataFrame to mongoDB server
    :param context: execution context
    :param df: DataFrame
    :return: dictionary of panda DataFrames
    :rtype: dict
    """

    client = context.resources.mongo_warehouse.get_connection(context)

    if client is not None:
        # get database collection
        collection = client.get_collection()

        # convert the DataFrame to a list like [{column -> value}, … , {column -> value}]
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
        entries = df.to_dict(orient='records')

        context.log.info(f'Record upload in progress')
        result = collection.insert_many(entries)

        context.log.info(f'Uploaded {len(result.inserted_ids)} records')

        # tidy up
        client.close_connection()

    return df


@solid(required_resource_keys={'postgres_warehouse'})
def upload_to_postgres(context, df):
    """
    Upload panda DataFrame to Postgres server
    :param context: execution context
    :param df: DataFrame
    :return: dictionary of panda DataFrames
    :rtype: dict
    """

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        cursor = client.cursor()

        try:
            # this is a one time op so if table exists delete it and recreate
            cursor.execute('DROP TABLE IF EXISTS airport_codes')

            # create table
            create_table_query = '''CREATE TABLE IF NOT EXISTS airport_codes (
                        id           SERIAL PRIMARY KEY,
                        country_code TEXT   NOT NULL,
                        name_local   TEXT   NOT NULL,
                        name         TEXT   NOT NULL,
                        iata         TEXT   NOT NULL,
                        geo_coord    TEXT,
                        country      TEXT   NOT NULL
                        ); '''
            cursor.execute(create_table_query)

            insert_query = """ INSERT INTO airport_codes (
                            country_code,
                            name_local,
                            name,
                            iata,
                            geo_coord,
                            country
                            ) VALUES %s"""
            tuples = [tuple(x) for x in df.values]

            # psycopg2.extras.execute_values() doesn't return much, so calc existing & post-insert count
            # airport_codes won't be big, so use count_sql, for big tables estimate using estimate_count_sql
            cursor.execute(count_sql('airport_codes'))
            result = cursor.fetchone()
            pre_len = result[0]

            execute_values(cursor, insert_query, tuples)
            client.commit()

            cursor.execute(count_sql('airport_codes'))
            result = cursor.fetchone()
            post_len = result[0]

            context.log.info(f'Uploaded {post_len - pre_len} records')

        finally:
            # tidy up
            cursor.close()
            client.close_connection()

    return df

