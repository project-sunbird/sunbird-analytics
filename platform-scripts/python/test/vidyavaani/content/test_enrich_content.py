from enrich_content import createDirectory, enrichContent
import pytest
import os
import json

root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_utils = os.path.join(python_dir, 'main', 'vidyavaani', 'utils')
sys.path.insert(0, src_code_utils)
from find_files import findFiles

dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'enrich_content')

def test_enrichContent():
	data_file = os.path.join(dir_path, 'input.csv')
	with open('data.json', encoding='utf-8') as data_file:
    contentJSON = json.loads(data_file.read())
	
	# enrichContent(contentJSON)
	pass