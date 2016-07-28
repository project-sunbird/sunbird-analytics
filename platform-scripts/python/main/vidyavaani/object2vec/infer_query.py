import os
import sys
import gensim as gs
import logging #Log the data given
import numpy as np
import ConfigParser
import json
import ast#remove
import langdetect
langdetect.DetectorFactory.seed=0

root=os.path.dirname(os.path.abspath(__file__))
utils=os.path.join((os.path.split(root)[0]),'utils')
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in 
from find_files import *
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')

#inputs
# std_input = sys.stdin
std_input = sys.stdin.readline()
# std_input = json.loads(std_input)
std_input = ast.literal_eval(std_input)#remove
contentID = std_input['contentId']
docs = std_input['document']
inferFlag = std_input['infer_all']
corpus_loc = std_input['corpus_loc']
model_loc = std_input['model']

def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

def uniqfy_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def get_all_lang(directory,string):
	lst_lang = [name
	             for root, dirs, files in os.walk(directory)
	             for name in files
	             if name.endswith((string))]
	lst_lang = uniqfy_list(lst_lang)
	return lst_lang

#geting paths from config file
config = ConfigParser.RawConfigParser()
config.read(config_file)

#check if paths existss
if not os.path.exists(corpus_loc):
	logging.info('Corpus folder do not exist')
	os.makedirs(corpus_loc)
#Set up logging
logging.basicConfig(filename=os.path.join(corpus_loc,'inferQuery.log'),level=logging.DEBUG)
logging.info('Corpus to Vectors')

all_vector = {}
if inferFlag == 'true':
	op_dir = corpus_loc
	lst_folder = get_immediate_subdirectories(op_dir)
	lst_folder.remove('model')
	for folder in lst_folder:
		vector_dict ={}
		content_folder = os.path.join(op_dir,folder)
		lst_lang = get_all_lang(content_folder,('tags','text'))
		for lang in lst_lang:
			file_path = os.path.join(content_folder,lang)
			if not os.path.exists(file_path):
				logging.info('%s not found'%(file_path))
				continue;
			txt = open(file_path)
			query = txt.read()	
			model_path = os.path.join(model_loc,lang)
			if not os.path.exists(model_path):
				logging.info('%s model not found, using default model'%(lang))
				model_path = os.path.join(model_loc,'en-text')
				if not os.path.exists(model_path):
					logging.info('default model not found, skipping vector this language')
					continue;	
			model=gs.models.doc2vec.Doc2Vec.load(model_path)
			q_vec=model.infer_vector(query)
			# q_vec=model.infer_vector(query.split(' '),alpha=0.1, min_alpha=0.0001, steps=5)
			vector_list = np.array(q_vec).tolist()
			vector_dict[lang] = vector_list
		all_vector[folder] = vector_dict
	print all_vector
else:
	for key in docs.keys():
		vector_dict ={}
		if not key == 'tags':
			model = '%s-text'%(key)
		query = docs[key]
		model_path = os.path.join(model_loc,model)
		if not os.path.exists(model_path):
			logging.info('%s model not found, using default model'%(model))
			model_path = os.path.join(model_loc,'en-text')
			if not os.path.exists(model_path):
				logging.info('default model not found, skipping vector this language')
				continue;	
		gensim_model=gs.models.doc2vec.Doc2Vec.load(model_path)
		q_vec=gensim_model.infer_vector(query)
		vector_list = np.array(q_vec).tolist()
		vector_dict[model] = vector_list
	all_vector[contentID] = vector_dict
	print all_vector

#Infer search string from model
#https://github.com/RaRe-Technologies/gensim/blob/develop/gensim/models/doc2vec.py#L499
def inference(query,model):
	model.sg=1#https://github.com/RaRe-Technologies/gensim/blob/develop/gensim/models/doc2vec.py#L721
	q_vec=model.infer_vector(query.split(' '),alpha=0.1, min_alpha=0.0001, steps=5)
	distance=0
	predicted_idx=0
	for idx in range(len(model.docvecs)):
		dist=np.dot(gs.matutils.unitvec(model.docvecs[idx]), gs.matutils.unitvec(q_vec))
		if(dist>distance):
			distance=dist
			predicted_idx=idx
	# print(distance)
	return model.docvecs.index_to_doctag(predicted_idx)
