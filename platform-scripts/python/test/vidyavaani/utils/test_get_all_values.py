import pytest
import os
# from collections import Counter
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