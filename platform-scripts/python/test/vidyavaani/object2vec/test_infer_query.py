import os
import pytest
import gensim as gs
import sys
import ast
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code = os.path.join(python_dir, 'main', 'vidyavaani', 'object2vec')
sys.path.insert(0, src_code)
from infer_query_functions import *

#test resources 
root = os.path.dirname(os.path.abspath(__file__))
dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'infer_query')

def test_infer_query_pos_1():
	inferFlag = 'true'
	op_dir = os.path.join(dir_path, 'pos', 'content_corpus')
	model_loc = os.path.join(dir_path, 'pos', 'model')
	q_vec = infer_query(inferFlag, model_loc, op_dir)
	assert q_vec

def test_infer_query_pos_2():
	inferFlag = 'true'
	op_dir = os.path.join(dir_path, 'pos', 'content_corpus')
	model_loc = os.path.join(dir_path, 'pos', 'model')
	q_vec = infer_query(inferFlag, model_loc, op_dir)
	q_vec = ast.literal_eval(q_vec)
	number_vec = len(q_vec["content_vectors"])
	assert number_vec == 4
	# pass

def test_infer_query_neg():
	# inferFlag = 'True'
	# op_dir = os.path.join(dir_path, 'neg', 'content_corpus')
	# model_loc = os.path.join(dir_path, 'neg', 'model')
	# infer_query(inferFlag, model_loc, op_dir)
	# assert q_vec
	pass

def test_get_vector_dimension():
	dim = get_vector_dimension()
	assert dim == 50


def test_get_all_lang():
	directory = os.path.join(dir_path, 'pos', 'content_corpus')
	lang = get_all_lang(directory, ('tags', 'text'))
	expected_lang = ['en-text', 'tags']
	assert lang == expected_lang


