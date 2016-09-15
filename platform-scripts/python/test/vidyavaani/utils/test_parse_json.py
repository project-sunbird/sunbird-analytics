from parse_json import extract_json
import pytest
import os 
import ast

# test if file exist and is a good json file
def test_good_extract_json_case1():
	json_files = []
	data_path = '/Users/ajitbarik/Ilimi/testing/nose/Data/parse_json/gcase1'
	file = 'test.json'
	json_files.append(os.path.join(data_path, file))
	result = extract_json(json_files)
	f_expected = open('/Users/ajitbarik/Ilimi/testing/nose/Data/parse_json/gcase1/expected.txt', 'r')
	expected = ast.literal_eval(f_expected.read())
	assert result == expected

# if file is json backup file
def test_good_extract_json_case2():
	json_files = []
	data_path = '/Users/ajitbarik/Ilimi/testing/nose/Data/parse_json/bcase1'
	file = 'test.json.bk'
	json_files.append(os.path.join(data_path, file))
	result = extract_json(json_files)
	expected = {}
	assert result == expected

# if file is bad json file
def test_good_extract_json_case3():
	json_files = []
	data_path = '/Users/ajitbarik/Ilimi/testing/nose/Data/parse_json/bcase2'
	file = 'test.json'
	json_files.append(os.path.join(data_path, file))
	result = extract_json(json_files)
	expected = {}
	assert result == expected

# if 'json_files' is not string instead of being a list
def test_good_extract_json_case4():
	json_files = '/Users/ajitbarik/Ilimi/testing/nose/Data/parse_json/bcase2/test.json'
	result = extract_json(json_files)
	expected = {}
	assert result == expected