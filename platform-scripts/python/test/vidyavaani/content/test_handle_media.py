import pytest
import os
import sys
import ast
import codecs
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_content = os.path.join(python_dir, 'main', 'vidyavaani', 'content')
sys.path.insert(0, src_code_content)

from handle_media import *
root = os.path.dirname(os.path.abspath(__file__))
handle_media_resources = os.path.join(rec_dir(root,1), 'test_resources', 'handle_media')

def test_pos_get_text():
	ecml_file = os.path.join(handle_media_resources, 'get_text', 'pos', 'index.ecml')
	print ecml_file
	result = get_text(ecml_file)
	assert result


def test_neg_get_text():
	ecml_file = os.path.join(handle_media_resources, 'get_text', 'neg', 'index.ecml')
	result = get_text(ecml_file)
	assert result == ''


def test_pos_count_MP3_length_directory():
	mp3_1 = os.path.join(handle_media_resources, 'count_MP3_length_directory', 'pos', 'test1.mp3')
	mp3_2 = os.path.join(handle_media_resources, 'count_MP3_length_directory', 'pos', 'test2.mp3')
	mp3_list = [mp3_1, mp3_2]
	result, ls = count_MP3_length_directory(mp3_list)
	expected = 13.1386
	assert result == expected 


def test_neg_count_MP3_length_directory():
	mp3_list = []
	result, ls = count_MP3_length_directory(mp3_list)
	assert result ==  0

def test_pos_count_file_type_directory():
	file_dir = os.path.join(handle_media_resources, 'count_file_type_directory', 'pos')
	result = count_file_type_directory(file_dir, ['png'])
	expected = {'png': 2}
	assert result == expected

def test_neg_count_file_type_directory():
	file_dir = os.path.join(handle_media_resources, 'count_file_type_directory', 'pos')
	result = count_file_type_directory(file_dir, ['pngvdg'])
	expected = {'pngvdg': 0}
	assert result == expected

@pytest.mark.skip(reason="WIP")
def test_pos_speech_recogniser():
	mp3_1 = os.path.join(handle_media_resources, 'speech_recogniser', 'pos', 'test1.mp3')
	mp3_2 = os.path.join(handle_media_resources, 'speech_recogniser', 'pos', 'test2.mp3')
	mp3_list = [mp3_1, mp3_2]
	result,ls = speech_recogniser(mp3_list)
	expected_file = os.path.join(handle_media_resources, 'speech_recogniser', 'pos', 'expected.txt')
	with codecs.open(expected_file, 'r', encoding='utf-8') as f:
		lines = f.read()
	f.close()
	expected_dict = ast.literal_eval(lines)
	assert cmp(expected_dict, result) == 0

def test_pos_camel_case_split():
	string = 'WhatIsGoingOn'
	result = camel_case_split(string)
	expected = ['What', 'Is', 'Going', 'On']
	assert result == expected

def test_pos_imageNames():
	file_dir = os.path.join(handle_media_resources, 'imageNames', 'pos')
	result = imageNames(file_dir)
	expected = 4
	assert len(result) == expected


def test_neg_imageNames():
	file_dir = os.path.join(handle_media_resources, 'imageNames', 'neg')
	result = imageNames(file_dir)
	expected = 0
	assert len(result) == expected

# @pytest.mark.skip(reason="no way of currently testing this")
def test_pos_add_confidence():
	input_file = os.path.join(handle_media_resources, 'add_confidence', 'pos', 'input.txt')
	f = open(input_file, 'r')
	input_dict = ast.literal_eval(f.read())
	f.close()
	output_file = os.path.join(handle_media_resources, 'add_confidence', 'pos', 'output.txt')
	f = open(output_file, 'r')
	output_dict = ast.literal_eval(f.read())
	f.close()
	result = add_confidence(input_dict)
	assert result == output_dict

def test_neg_add_confidence():
	result = add_confidence({})
	assert result == {}