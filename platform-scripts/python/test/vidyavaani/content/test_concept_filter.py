from concept_filter import *
import pytest
import os 

def read_concepts(dir_path):
	with codecs.open(os.path.join(dir_path, 'conceptList.txt'), 'r', encoding='utf-8') as f:
		conceptList = f.readlines()
		conceptList = conceptList[0].split(',')
	return conceptList

def test_filter_assessment_data_case1():
	#C562 present in conceptList file
	dir_path = '/Users/ajitbarik/Ilimi/testing/nose/Data/concept_filter/case1'
	conceptList = read_concepts(dir_path)
	result = filter_assessment_data(dir_path, conceptList)
	expected = [u'C562']
	assert result == expected

def test_filter_assessment_data_case2():
	#C562 not present in conceptList file
	dir_path = '/Users/ajitbarik/Ilimi/testing/nose/Data/concept_filter/case2'
	conceptList = read_concepts(dir_path)
	result = filter_assessment_data(dir_path, conceptList)
	expected = []
	assert result == expected