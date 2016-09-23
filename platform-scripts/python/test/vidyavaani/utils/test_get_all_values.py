import pytest
import os
import sys
# from collections import Counter

root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_utils = os.path.join(python_dir, 'main', 'vidyavaani', 'utils')
sys.path.insert(0, src_code_utils)

from get_all_values import getAllValues

def test_getAllValues_for_dict():
	sample_dict = {'id': '12', 'subject': ['english', 'hindi', 'maths'], 'marks': {'english': '87', 'hindi': '89', 'maths': '93'}}
	result = getAllValues(sample_dict)
	expected = ['12', 'english', 'hindi', 'maths', '87', '89', '93']
	assert sorted(result) == sorted(expected)

def test_getAllValues_for_list():
	sample_dict = ['12', 'english', 'hindi', ['maths', '87', '89', '93']]
	result = getAllValues(sample_dict)
	expected = ['12', 'english', 'hindi', 'maths', '87', '89', '93']
	assert sorted(result) == sorted(expected)

def test_getAllValues_for_string():
	sample_dict = 'testing'
	result = getAllValues(sample_dict)
	expected = ['testing']
	assert result == expected

def test_getAllValues_for_int():
	sample_dict = 2324
	result = getAllValues(sample_dict)
	expected = [2324]
	assert result == expected