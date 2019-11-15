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

import os


def get_file_path(environ, req_file_desc):
    """
    Get a file path, either from an environment variable or input
    :param environ: Environment variable to
    :param req_file_desc: Description of required file
    :return: file path or None
    """
    filename = None
    approve = 'n'
    if isinstance(environ, str):
        filename = os.environ.get(environ)
        if test_file_path(filename,
                          nonexistent=f'>> The path specified in the environment variable {environ} '
                                      f'does not exist, ignoring',
                          not_a_file=f'>> The path specified in the environment variable {environ} '
                                     f'is not a file, ignoring'):
            approve = 'y'

    if approve != 'q':
        print(f"Current directory is: {os.getcwd()}")
        while filename is None:
            filename = input(f"Enter path to {req_file_desc} [or 'q' to quit]: ")
            if filename.lower() == 'q':
                filename = None
                break

            if not test_file_path(filename,
                                  nonexistent=f'>> The path does not exist',
                                  not_a_file=f'>> Not a file'):
                filename = None

    return filename


def test_file_path(filename, nonexistent=None, not_a_file=None):
    """
    Test a file path, to see if it is exists and is a file
    :param filename: path to filesystem object to check
    :param nonexistent: message to display if nonexistent
    :param not_a_file: message to display if not a file
    :return: True if exists & is a file
    """
    is_a_file = False
    if filename is not None:
        if not os.path.exists(filename):
            if nonexistent is not None:
                print(nonexistent)
        else:
            is_a_file = os.path.isfile(filename)
            if not is_a_file:
                if not_a_file is not None:
                    print(not_a_file)

    return is_a_file
