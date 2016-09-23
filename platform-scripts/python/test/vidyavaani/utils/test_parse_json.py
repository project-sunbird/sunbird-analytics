import pytest
import os 
import ast
import sys
#adding source utils file 
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_utils = os.path.join(python_dir, 'main', 'vidyavaani', 'utils')
data_loc = os.path.join(rec_dir(root,1), 'test_resources', 'parse_json')
sys.path.insert(0, src_code_utils)
from parse_json import extract_json

# test if file exist and is a good json file
def test_good_extract_json_case1():
	json_files = []
	data_path = os.path.join(data_loc, 'gcase1')
	file = 'test.json'
	json_files.append(os.path.join(data_path, file))
	result = extract_json(json_files)
	f_expected = open(os.path.join(data_path, 'expected.txt'), 'r')
	expected = ast.literal_eval(f_expected.read())
	assert result == expected

# if file is json backup file
def test_good_extract_json_case2():
	json_files = []
	data_path = os.path.join(data_loc, 'bcase1')
	file = 'test.json.bk'
	json_files.append(os.path.join(data_path, file))
	result = extract_json(json_files)
	expected = {}
	assert result == expected

# if file is bad json file
def test_good_extract_json_case3():
	json_files = []
	data_path = os.path.join(data_loc, 'bcase2')
	file = 'test.json'
	json_files.append(os.path.join(data_path, file))
	result = extract_json(json_files)
	expected = {}
	assert result == expected

# if 'json_files' is not string instead of being a list
def test_good_extract_json_case4():
	json_files = os.path.join(data_loc, 'bcase2', 'test.json')
	result = extract_json(json_files)
	expected = {}
	assert result == expected