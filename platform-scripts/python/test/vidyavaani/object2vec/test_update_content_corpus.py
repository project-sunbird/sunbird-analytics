import pytest
import os
import sys
import json
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code = os.path.join(python_dir, 'main', 'vidyavaani', 'object2vec')
sys.path.insert(0, src_code)
from update_content_corpus_functions import updateContentCorpus, process_data, merge_strings, createDirectory, uniqfy_list

#test resources 
dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'update_content_corpus')

def test_merge_strings_pos():
	input_dict =  {'first': {'alternative': [{'transcript': "select this"}, {'transcript': 'not this one'}]}}
	result = merge_strings(input_dict)
	expected = {'first': 'select this'}
	assert result == expected

def test_merge_strings_neg():
	input_dict =  {'first': ''}
	result = merge_strings(input_dict)
	expected = input_dict
	assert result == expected

def test_process_data_pos():
	pass

def test_process_data_neg():
	pass

@pytest.mark.skip(reason="need to check the actual input and output")
def test_updateContentCorpus_pos():
	# input_file = os.path.join(dir_path, 'pos', 'input.txt')
	# input_text = open(input_file, 'r')
	# input_text = input_text.read()
	# print input_text
	# result = updateContentCorpus(input_text)
	# expected = {'text': 'Ek gajar Ki Keemat kya hai', 'tags': ''}
	# assert result == expected
	pass

def test_updateContentCorpus_neg():
	input_file = os.path.join(dir_path, 'neg', 'input.txt')
	input_text = open(input_file, 'r')
	input_text = input_text.read()
	result = updateContentCorpus(input_text)
	expected = 1
	assert result == expected


def test_uniqfy_list_pos():
	sample_list = ['1', '2', '3', '1', '4', '0']
	result = uniqfy_list(sample_list)
	expected = ['1', '2', '3', '4', '0']
	assert result == expected

def test_uniqfy_list_neg():
	pass