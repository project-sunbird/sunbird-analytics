from concept_filter import *
import os 

root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code_content = os.path.join(python_dir, 'main', 'vidyavaani', 'content')
sys.path.insert(0, src_code_content)
from enrich_content_functions import createDirectory, enrichContent
root = os.path.dirname(os.path.abspath(__file__))

def test_createDirectory():
	# function doesn't return anything
	pass

def test_getConcepts():
	# function doesn't return anything
	pass