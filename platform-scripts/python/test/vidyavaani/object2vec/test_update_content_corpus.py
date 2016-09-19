import pytest
import os
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_utils = os.path.join(python_dir, 'main', 'vidyavaani', 'object2vec')
sys.path.insert(0, src_code_utils)
from enrich_content import updateContentCorpus, process_data, merge_strings, createDirectory

#test resources 
dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'update_content_corpus')

def test_updateContentCorpus():
	chcek = 0
	input_path = os.path.join(dir_path, 'input.json')
	with open(input_path, encoding='utf-8') as data_file:
    	input_text = json.loads(data_file.read())
	result = updateContentCorpus(input_text)
	if 'tag' in result and 'text' in result:
		check = 1
	assert check == 1

def test_process_data():

	pass

def test_merge_strings():
	pass

def test_createDirectory():
	pass