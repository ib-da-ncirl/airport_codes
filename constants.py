# The MIT License (MIT)
# Copyright (c) 2021 Ian Buttimer

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

"""
    For information regarding UN/LOCODE, see 
    https://www.unece.org/cefact/locode/welcome.html
    For information regarding the data format, see 
    https://unece.org/DAM/cefact/locode/UNLOCODE_Manual.pdf and 
    '2021-1 UNLOCODE SecretariatNotes.pdf' in data/loc211csv.zip
"""

# Columns presented in UN/LOCODE
# change indicator that shows if the entry has been modified in any way
COL_CHANGE: str = 'change'
# the ISO 3166 alpha-2 Country Code
COL_LO: str = 'lo'
# a 3-character code for the place name
COL_CODE: str = 'code'
# place name, whenever possible, in their national language
COL_LOCAL: str = 'name_local'
# place name, without diacritic signs
COL_NAME: str = 'name'
# ISO 1-3 character code for the administrative division of the country,
# as per ISO 3166-2/1998
COL_DIVISION: str = 'subdivision'
# 1-digit function classifier code for the location
COL_FUNCTION: str = 'function'
# status of the entry
COL_STATUS: str = 'status'
# reference date, showing the year and month of request
COL_DATE: str = 'date'
# IATA code for the location if different from location code
COL_IATA: str = 'iata'
# geographical coordinates (latitude/longitude), # ddmmN dddmmW, ddmmS dddmmE,
# etc., where the two last digits refer to minutes and the two or three
# first digits indicate the degrees
COL_COORD: str = 'geo_coord'
# reasons for the change
COL_REMARK: str = 'remark'

FUNCTION_PORT: str = '1'         # port, as defined in Rec. 16
FUNCTION_RAIL: str = '2'         # rail terminal
FUNCTION_ROAD: str = '3'         # road terminal
FUNCTION_AIRPORT: str = '4'      # airport
FUNCTION_POST: str = '5'         # postal exchange office
FUNCTION_MULTIMODAL: str = '6'   # multimodal functions, ICD's, etc.
FUNCTION_FIXED: str = '7'        # fixed transport functions (e.g. oil platform)
FUNCTION_BORDER: str = 'B'       # border crossing
FUNCTION_UNKNOWN: str = '0'      # function not known, to be specified

