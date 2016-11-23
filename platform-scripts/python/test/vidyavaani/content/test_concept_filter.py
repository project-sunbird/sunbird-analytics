
import pytest
import os 
import sys

root = os.path.dirname(os.path.abspath(__file__))
# print root
def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code = os.path.join(python_dir, 'main', 'vidyavaani', 'content')
sys.path.insert(0, src_code)
from concept_filter import *

#test resources 
root = os.path.dirname(os.path.abspath(__file__))
dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'concept_filter')

def read_concepts(case_path):
	# print case_path
	with codecs.open(os.path.join(case_path, 'conceptList.txt'), 'r', encoding='utf-8') as f:
		conceptList = f.readlines()
		conceptList = conceptList[0].split(',')
	return conceptList

def test_filter_assessment_data_case1():
	#C562 present in conceptList file
	case1_path = os.path.join(dir_path, 'case1')
	# print case1_path
	# print root
	conceptList = read_concepts(case1_path)
	result = filter_assessment_data(case1_path, conceptList)
	expected = [u'C562']
	assert result == expected

def test_filter_assessment_data_case2():
	#C562 not present in conceptList file
	case2_path = os.path.join(dir_path, 'case2')
	conceptList = read_concepts(case2_path)
	result = filter_assessment_data(case2_path, conceptList)
	expected = []
	assert result == expected