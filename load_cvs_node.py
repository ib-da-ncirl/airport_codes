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

import csv
import re
from zipfile import ZipFile
import pandas as pd
import os.path as path

from dagster import solid


@solid
def load_csv_from_zip(context, zip_path, pattern, encoding, header):
    """
    Load csv files from a zip file into a dictionary of panda DataFrames,
    where the filename matches the specified pattern
    :param context: context object
    :param zip_path: path to zip file
    :param pattern: regex pattern to match csv files in zip file
    :param encoding: encoding to use when reading csv files
    :param header: header to use
    :return: dictionary of panda DataFrames
    :rtype: dict
    """
    # verify zip path
    if not path.exists(zip_path):
        raise ValueError(f'Invalid zip file path: {zip_path}')

    regex = re.compile(pattern)

    # use dictionary comprehension to load all the csv files in the zip file into a pandas data frame
    zip_file = ZipFile(zip_path)
    df = {
        file.filename: pd.read_csv(
            zip_file.open(file.filename), encoding=encoding, names=header)
        for file in zip_file.infolist()
        if regex.match(file.filename)
    }

    context.log.info(
        f'Loaded {len(df)} files'
    )

    return df


@solid
def combine_csv_from_dict(context, df_dict):
    """
    Combine a dictionary of panda DataFrames into a single DataFrame
    :param context: context object
    :param df_dict: dictionary of panda DataFrames
    :return: panda DataFrame or None
    :rtype: dict
    """
    df_count = len(df_dict)
    if df_count == 0:
        df = None
    else:
        df = pd.DataFrame()
        for key in df_dict.keys():
            df = df.append(df_dict[key])

    context.log.info(
        f'Merged {df_count} DataFrames'
    )

    return df
