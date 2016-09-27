import pytest
import os
import sys
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_content = os.path.join(python_dir, 'main', 'vidyavaani', 'content')
sys.path.insert(0, src_code_content)

from handle_media import *
handle_media_resources = os.path.join(rec_dir(root,1), 'test_resources', 'handle_media')

def test_pos_get_text():
	ecml_file = os.path.join(handle_media_resources, 'get_text', 'pos', 'index.ecml')
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
	# expected = 
	# assert result == expected 
	pass

def test_neg_count_MP3_length_directory():
	mp3_1 = os.path.join(handle_media_resources, 'count_MP3_length_directory', 'neg', 'test1.mp3')
	mp3_2 = os.path.join(handle_media_resources, 'count_MP3_length_directory', 'neg', 'test2.mp3')
	mp3_list = [mp3_1, mp3_2]
	result, ls = count_MP3_length_directory(mp3_list)
	assert result ==  0

def test_pos_count_file_type_directory():
	file_dir = os.path.join(handle_media_resources, 'count_file_type_directory', 'pos')
	result = count_file_type_directory(file_dir, 'png')
	# expected = 
	# assert result == expected
	pass

def test_neg_count_file_type_directory():
	file_dir = os.path.join(handle_media_resources, 'count_file_type_directory', 'pos')
	result = count_file_type_directory(file_dir, 'pngvdg')
	expected = {}
	assert result == expected


def test_pos_speech_recogniser():
	mp3_1 = os.path.join(handle_media_resources, 'count_MP3_length_directory', 'neg', 'test1.mp3')
	mp3_2 = os.path.join(handle_media_resources, 'count_MP3_length_directory', 'neg', 'test2.mp3')
	mp3_list = [mp3_1, mp3_2]
	result= speech_recogniser(mp3_list)
	assert result ==  0

def test_pos_camel_case_split():
	string = 'WhatIsGoingOn'
	result = camel_case_split(string)
	expected = ['What', 'Is', 'Going', 'On']
	assert result == expected

def test_pos_imageNames():
	file_dir = os.path.join(handle_media_resources, 'imageNames', 'pos')
	result = imageNames(file_dir)
	# expected = 
	# assert result == expected
	pass

def test_neg_imageNames():
	file_dir = os.path.join(handle_media_resources, 'imageNames', 'neg')
	result = imageNames(file_dir)
	expected = []
	assert result == expected

def test_pos_add_confidence():
	pass

def test_neg_add_confidence():
	pass