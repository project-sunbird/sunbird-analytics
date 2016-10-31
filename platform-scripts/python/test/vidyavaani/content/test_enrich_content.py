import pytest
import os
import json
import sys
import codecs
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_content = os.path.join(python_dir, 'main', 'vidyavaani', 'content')
sys.path.insert(0, src_code_content)
from enrich_content_functions import createDirectory, enrichContent
root = os.path.dirname(os.path.abspath(__file__))
dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'enrich_content')

def test_enrichContent():
	data_file = os.path.join(dir_path, 'input.json')
	with open(data_file) as json_data:
		contentJSON = json.load(json_data)
	# with codecs.open('data.json', encoding='utf-8') as data_file:
	# 	contentJSON = json.loads(data_file.readlines())
	
	enrichContent(contentJSON)
	resout, reserr = capfd.readouterr()
	filename_expected = os.path.join(dir_path, 'expected.out')
	expected = open(filename_expected, "r").read()
	assert resout == expected
	# ch_result = check_result(resout)
	# assert ch_result == 1

def check_result(result):
	check = 0
	if type(result) == dict:
		if "identifier" in dict:
			check = 1
	return check