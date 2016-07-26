# Author: Aditya Arora, adityaarora@ekstepplus.org

import gensim as gs
import os
import numpy as np
import codecs
import re
import numpy as np
import argparse #Accept commandline arguments
import logging #Log the data given
import sys
from nltk.corpus import stopwords
import ConfigParser

#Using utility functions
root=os.path.dirname(os.path.abspath(__file__))

utils=os.path.join(os.path.split(root)[0],'utils')
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in 
#To find files with a particular substring
from find_files import findFiles

#get file path from config file 
config = ConfigParser.RawConfigParser()
config.read(config_file)
op_dir = config.get('FilePath','corpus_path')

#get parameters from config file
language_model=config.get('Training','language_model')
tags_model=config.get('Training','tags_model')

#pvdm params
pvdm_size=int(config.get('pvdm','size'))
pvdm_min_count=int(config.get('pvdm','min_count'))
pvdm_window=int(config.get('pvdm','window'))
pvdm_negative=int(config.get('pvdm','negative'))
pvdm_workers=int(config.get('pvdm','workers'))
# pvdm_sample=int(config.get('pvdm','sample'))

#pvdbow params
pvdbow_size=int(config.get('pvdbow','size'))
pvdbow_min_count=int(config.get('pvdbow','min_count'))
pvdbow_window=int(config.get('pvdbow','window'))
pvdbow_negative=int(config.get('pvdbow','negative'))
pvdbow_workers=int(config.get('pvdbow','workers'))
# pvdbow_sample=int(config.get('pvdbow','sample'))
pvdbow_dm=int(config.get('pvdbow','dm'))
#Set up logging
logging.basicConfig(filename=os.path.join(op_dir,'corpus2Vec.log'),level=logging.DEBUG)
logging.info('Corpus to Vectors')

stopword = set(stopwords.words("english"))

def process_text(lines,language):#Text processing
	word_list=[]
	for line in lines:
		if(language=='en-text' or language=='id-text'):#Any language using ASCII characters goes here
			line=re.sub("[^a-zA-Z]", " ",line)
			for word in line.split(' '):
				if word not in stopword and len(word)>1:
					word_list.append(word.lower())
		else:#Any language using unicode characters goes here
			line=' '.join([i for i in line if ord(i)>127 or ord(i)==32])
			#Remove ASCII Characters since the language is not english(This will remove punctuation as well)
			for word in line.split(' '):
					word_list.append(word.lower())
	return word_list

def process_file(filename,language):#File processing
	try:
		with codecs.open(filename,'r',encoding='utf-8') as f:
			lines = f.readlines()
		f.close()
		return process_text(lines,language)
	except:
		return

def load_documents(filenames,language):#Creating TaggedDocuments
	doc=[]
	for filename in filenames:
		word_list=process_file(filename,language)
		print word_list
		if(word_list!=None):
			doc.append(gs.models.doc2vec.TaggedDocument(words=word_list,tags=[filename]))  
		else:
			print(filename+" failed to load")
	return doc

def train_model_pvdm(directory,language):#en-English,id-Hindi
	doc=load_documents(findFiles(directory,[language]),language)
	if doc == []:
		return 0
	model=gs.models.doc2vec.Doc2Vec(doc, size=pvdm_size, min_count=pvdm_min_count, window=pvdm_window, negative=pvdm_negative, workers=pvdm_workers, sample=1e-5)
	return model

def train_model_pvdbow(directory,language):
	if language == ['tags']:
		doc=load_documents(findFiles(directory,['tag']),"en")
	else:
		doc=load_documents(findFiles(directory,[language]),language)
	if doc == []:
		return 0
	model=gs.models.doc2vec.Doc2Vec(doc, size=pvdbow_size, min_count=pvdbow_min_count, window=pvdbow_window, negative=pvdbow_negative, workers=pvdbow_workers, sample=1e-5, dm=pvdbow_dm) #Apply PV-DBOW
	return model

def uniqfy_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


models_lang = [name
             for root, dirs, files in os.walk(op_dir)
             for name in files
             if name.endswith(("-text"))]

models_lang = uniqfy_list(models_lang)
# print models_lang

models_tags = [name
             for root, dirs, files in os.walk(op_dir)
             for name in files
             if name.endswith(("tags"))]

models_tags = uniqfy_list(models_tags)
# print models_tags

# models=['en-text','id-text','tag']
for f in models_lang:
	if(os.path.isfile(os.path.join(op_dir,'model',f))):
		os.remove(os.path.join(op_dir,'model',f))

if not os.path.exists(os.path.join(op_dir,'model')):
	os.makedirs(os.path.join(op_dir,'model'))

for model in models_lang:
	function = 'train_model_%s'%language_model
	if(model=='en-text' or model=='id-text'):
		gensim_model = eval(function)(op_dir,model)
		if not gensim_model == 0:
			gensim_model.save(os.path.join(op_dir,'model',model))
		else:
			logging.info('%s data not present'%(model))

# function = 'train_model_%s'%tags_model
# model_tags=eval(function)(op_dir,models_tags)
# if not model_tags == 0:
# 	model_tags.save(os.path.join(op_dir,'model','tag'))
# else:
# 	logging.info('tags data not present')


