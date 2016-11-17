import pytest
import os
import sys
import codecs

root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code = os.path.join(python_dir, 'main', 'vidyavaani', 'object2vec')
sys.path.insert(0, src_code)
from corpus_to_vec_functions import *

#test resources 
root = os.path.dirname(os.path.abspath(__file__))
dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'corpus_to_vec')


def test_process_text_pos():
	filename = os.path.join(dir_path, 'process_text', 'pos', 'en-text')
	with codecs.open(filename, 'r', encoding='utf-8') as f:
		lines = f.readlines()
	f.close()
	result = process_text(lines, 'en-text')
	expected = ['sample','story','check','version','upgrade','side','loaded','genie']
	assert result == expected

def test_process_text_neg():
	lines = []
	result = process_text(lines, 'en-text')
	expected = []
	assert result == expected	

def test_process_file_pos():
	filename = os.path.join(dir_path, 'process_file', 'pos', 'en-text')
	result = process_file(filename, 'en-text')
	expected = ['sample','story','check','version','upgrade','side','loaded','genie']
	assert result == expected

def test_process_file_neg():
	filename = ''
	result = process_file(filename, 'en-text')
	expected = []
	assert result == expected

def test_load_documents_pos():
# 	# expected_file = os.path.join(dir_path, 'load_documents', 'pos', 'expected.txt')
# 	# with codecs.open(filename, 'r', encoding='utf-8') as f:
# 	# 	lines = f.read()
# 	# f.close()
# 	path1 = os.path.join(dir_path, 'load_documents', 'pos')
# 	file1 = os.path.join(path1, 'en1')
# 	file2 = os.path.join(path1, 'en2')
# 	filenames = [file1, file2]
# 	result = load_documents(filenames, 'en-text')
# 	expected = [TaggedDocument(words=[u'sample', u'story', u'check', u'version', u'upgrade', u'side', u'loaded', u'genie'],
# 				tags=['/Users/ajitbarik/Ilimi/github/Learning-Platform-Analytics/platform-scripts/python/test/vidyavaani/test_resources/corpus_to_vec/load_documents/pos/en1']),
# 				TaggedDocument(words=[u'bk', u'test'],
# 				tags=['/Users/ajitbarik/Ilimi/github/Learning-Platform-Analytics/platform-scripts/python/test/vidyavaani/test_resources/corpus_to_vec/load_documents/pos/en2'])]
# 	assert result == expected
	pass

def test_load_documents_neg():
	pass

# def test_train_model_pvdm_pos():
# 	test_path = os.path.join(dir_path, 'train_model_pvdm', 'pos')
# 	model = train_model_pvdm(test_path, 'en-text')
# 	assert model

def test_train_model_pvdm_neg():
	test_path = os.path.join(dir_path, 'train_model_pvdm', 'neg')
	model = train_model_pvdm(test_path, 'en-text')
	assert model == 0

# def test_train_model_pvdbow_pos():
# 	test_path = os.path.join(dir_path, 'train_model_pvdm', 'pos')
# 	model = train_model_pvdm(test_path, 'en-text')
# 	assert model

def test_train_model_pvdbow_neg():
	test_path = os.path.join(dir_path, 'train_model_pvdbow', 'neg')
	model = train_model_pvdbow(test_path, 'en-text')
	assert model == 0

def test_uniqfy_list_pos():
	sample_list = ['1', '2', '3', '1', '4', '0']
	result = uniqfy_list(sample_list)
	expected = ['1', '2', '3', '4', '0']
	assert result == expected

def test_uniqfy_list_neg():
	pass

def test_get_all_lang_pos():
	test_path = os.path.join(dir_path, 'get_all_lang', 'pos')
	models_lang = get_all_lang(test_path, "-text")
	expected = ['en-text', 'hi-text']
	assert models_lang == expected

def test_get_all_lang_neg():
	test_path = os.path.join(dir_path, 'get_all_lang', 'neg')
	models_lang = get_all_lang(test_path, "tags")
	expected = []
	assert models_lang == expected