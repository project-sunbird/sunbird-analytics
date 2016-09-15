from find_files import findFiles
from nose.tools import *
import os 
import pytest
dir_path = '/Users/ajitbarik/Ilimi/testing/nose'

def test_good_find_files():
	# directory is string and it exists
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(dir_path, 'Data')
    # files is a list
    files = ['.csv']
    result = findFiles(data_path,files)
    assert result == ['/Users/ajitbarik/Ilimi/testing/nose/Data/vectors.csv', '/Users/ajitbarik/Ilimi/testing/nose/Data/vectors_after.csv']


def test_bad_find_files_dir_not_exist():
	# directory is string but does not exists
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(dir_path, 'Data23425')
    # files is a list
    files = ['.csv']

    assert(findFiles(data_path,files) == [])

def test_bad_find_files_substring_not_list():
	# directory is string but does not exists
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(dir_path, 'Data')
    # files is a list
    files = '.csv'

    assert(findFiles(data_path,files) == [])