# Author: Aditya Arora, adityaarora@ekstepplus.org

import json
import os
import codecs
from find_files import findFiles
from get_lowest_key_value import *

#Extract json file values to list
def extract_json(json_filenames):
	json_files={}
	bugs=[]
	for filename in json_filenames:
#		.json.bk files are noise: They refer to the files already present in the template
		if(filename.endswith('.json.bk')):
			continue
		try:
			with codecs.open(filename,"r",encoding="utf-8") as f:
				json_data = json.load(f)
			json_files[filename]=flattenDict(json_data)
		except:
			bugs.append(filename)
		#Can return bugs for buggy json if needed
		f.close()
	return json_files
