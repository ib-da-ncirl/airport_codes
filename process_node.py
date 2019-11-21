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


@solid
def process_unlocode(context, df):
    """
    Process UN/LOCODE data to prepare it for upload
    :param context: execution context
    :param df: DataFrame containing data
    :return: panda DataFrame with the following format
                'lo',           # the ISO 3166 alpha-2 Country Code
                'name_local',   # place name, whenever possible, in their national language
                'name',         # place name, without diacritic signs
                'iata',         # IATA code for the location if different from location code
                'geo_coord',    # geographical coordinates (latitude/longitude), ddmmN dddmmW, ddmmS dddmmE, etc., where the
                                # two last digits refer to minutes and the two or three first digits indicate the degrees
                'country'       # country name
    :rtype: panda.DataFrame
    """
    pre_len = len(df)

    if pre_len > 0:
        # sorting by country code
        df.sort_values('lo', inplace=True)

        # drop duplicate values
        df.drop_duplicates(keep='first', inplace=True)

        post_len = len(df)
        if post_len != pre_len:
            context.log.info(f'Dropped {pre_len - post_len} duplicates of {pre_len}')

        # entries with no 'code' value are country headings or names that have been changed
        # e.g. lo code name_local         name               function iata geo_coord
        #      AD      .ANDORRA
        #      AE      Ruwais = Ar Ruways Ruwais = Ar Ruways

        # generate a dict of country ids
        # no 'code' value dataframe
        cc_df = df[df['code'] == '']
        # country name start with '.'
        cc_df = cc_df[cc_df['name_local'].str.startswith('.')]
        # remove leading '.' and convert to title case
        cc_df['name_local'] = cc_df['name_local'].str[1:].str.title()
        # create dict with country code as key and name as value
        country_codes = dict(zip(cc_df['lo'], cc_df['name_local']))

        # drop no 'code' entries
        pre_len = len(df)
        df = df.dropna(subset=['code'])

        post_len = len(df)
        if post_len != pre_len:
            context.log.info(f'Dropped {pre_len - post_len} country headings from {pre_len}')

        # function code 4 represents an airport so remove other entries
        # e.g. lo code name_local       name             function iata geo_coord
        #      AD ALV  Andorra la Vella Andorra la Vella --34-6-- nan  4230N 00131E
        pre_len = len(df)
        df = df[df['function'].str.find('4') >= 0]

        post_len = len(df)
        if post_len != pre_len:
            context.log.info(f'Dropped {pre_len - post_len} non-airport entries from {pre_len}')

        # copy the un/locode to the iata column if the iata column is not already set
        # e.g. lo code name_local       name             function iata geo_coord
        #      AD ALV  Andorra la Vella Andorra la Vella --34-6-- nan  4230N 00131E
        df['iata'] = df['iata'].where(df['iata'] != "", df['code'])

        # drop columns not required
        df = df.drop(columns=['code', 'function'])

        # add a country column by mapping country code to country name
        df['country'] = df['lo'].map(country_codes)

        # TODO lookup missing geo coordinates

        context.log.info(f'Processed data for {len(df)} airports')

    else:
        context.log.info(f'DataFrame empty')

    return df

