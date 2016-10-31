import os
import pytest
import gensim as gs

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
	inferFlag = 'True'
	op_dir = os.path.join(dir_path, 'pos', 'content_corpus')
	model_loc = os.path.join(dir_path, 'pos', 'model')
	infer_query(inferFlag, model_loc, op_dir)
	assert q_vec

def test_infer_query_pos_1():
	inferFlag = 'True'
	op_dir = os.path.join(dir_path, 'pos', 'content_corpus')
	model_loc = os.path.join(dir_path, 'pos', 'model')
	infer_query(inferFlag, model_loc, op_dir)
	assert q_vec

def test_infer_query_neg():
	inferFlag = 'True'
	op_dir = os.path.join(dir_path, 'neg', 'content_corpus')
	model_loc = os.path.join(dir_path, 'neg', 'model')
	infer_query(inferFlag, model_loc, op_dir)
	assert q_vec
	pass

def test_get_vector_dimension():
	pass
