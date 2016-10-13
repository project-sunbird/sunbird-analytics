import pytest
import os
import sys
import json
from subprocess import Popen, PIPE
from tempfile import SpooledTemporaryFile as tempfile
import shlex
root = os.path.dirname(os.path.abspath(__file__))

def rec_dir(path, times):
    if times > 0:
        path = rec_dir(os.path.split(path)[0], times-1)
    return path

python_dir = rec_dir(root,3)
src_code = os.path.join(python_dir, 'main', 'vidyavaani', 'object2vec', 'update_content_corpus.py')
# sys.path.insert(0, src_code_utils)
# from update_content_corpus import updateContentCorpus, process_data, merge_strings, createDirectory

#test resources 
dir_path = os.path.join(rec_dir(root,1), 'test_resources', 'update_content_corpus')

# def test_updateContentCorpus():
# 	chcek = 0
# 	input_path = os.path.join(dir_path, 'input.json')

# 	with open(input_path) as data_file:
# 		input_text = json.loads(data_file.read())

# 	result = updateContentCorpus(input_text)
# 	if 'tag' in result and 'text' in result:
# 		check = 1
# 	assert check == 1

def test_update_content_corpus():

	# myinput = open(os.path.join(dir_path, 'input.in'))
	myoutput = open(os.path.join(dir_path, 'output.out'), 'w')
	myinput = {"languageCode": "en", "mediaCount": {"gif": 0, "ogg": 0, "png": 12, "mp3": 1, "jpg": 0}, "contentType": "Worksheet", "description": "PRW Item Template", "language": ["English"], "items": ["{}"], "imageTags": ["icon validate", "greenbox", "icon home", "icon submit", "ekstep placeholder blue eye", "icon hint", "speech bubble", "yellowbox", "icon reload", "background", "icon next", "icon previous"], "mp3Transcription": {"tmp/domain_38527/assets/q1_1460133785285": {"alternative": [{"confidence": 0.86655092, "transcript": "Ek gajar Ki Keemat kya hai"}, {"transcript": "gajar Ki Keemat kya hai"}, {"transcript": "IIT gajar Ki Keemat kya hai"}, {"transcript": "Ik gajar Ki Keemat kya hai"}, {"transcript": "EC gajar Ki Keemat kya hai"}], "final": true}}, "mp3Length": 2.914104308390023, "concepts": [], "owner": "EkStep", "identifier": "domain_38527", "data": ["{}"], "developer": "EkStep"}
	f = tempfile()
	f.write(myinput)
	f.seek(0)
	# command_line = 'echo %s | python %s' %(myinput,src_code)
	# args = shlex.split(command_line)
	# p = subprocess.Popen(args)
	p = subprocess.Popen(["python",src_code],stdin=f, stdout=PIPE)
	f.close()
	# p.wait()
	# myoutput.flush()
	pass