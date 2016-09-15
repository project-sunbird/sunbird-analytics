import pytest
import os 
from get_lowest_key_value import encodeName, f, objpath, flattenDict

def test_encodeName_string():
	string_to_test = 'ilimi'
	result = encodeName(string_to_test)
	expected = 'ilimi'
	assert result == expected

def test_encodeName_int():
	string_to_test = 1234
	result = encodeName(string_to_test)
	expected = '1234'
	assert result == expected

def test_encodeName_unicode():
	string_to_test = u'\u091c\u0940\u0935\u0928'
	result = encodeName(string_to_test)
	expected = '\xe0\xa4\x9c\xe0\xa5\x80\xe0\xa4\xb5\xe0\xa4\xa8'
	assert result == expected

	