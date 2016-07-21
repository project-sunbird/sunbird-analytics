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
<<<<<<< HEAD
utils=os.path.join((os.path.split(root)[0]),'utils')
=======
utils=os.path.join(os.path.split(root)[0],'utils')
>>>>>>> fd58a9ab8eb0b941a1f5e2a68253bdafab402144
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in 
#To find files with a particular substring
from find_files import findFiles

#Define commandline arguments
# parser = argparse.ArgumentParser()
# parser.add_argument('--ld',help='This is the operating directory',default=os.path.join(root,'Corpus'))

#Read arguments given
# args = parser.parse_args()
config = ConfigParser.RawConfigParser()
config.read(config_file)
op_dir = config.get('FilePath','corpus_path')
# op_dir=args.ld
#Set up logging
logging.basicConfig(filename=os.path.join(op_dir,'corpus2Vec.log'),level=logging.DEBUG)
logging.info('Corpus to Vectors')

stopword = set(stopwords.words("english"))

def process_text(lines,language):#Text processing
	word_list=[]
	for line in lines:
		if(language=='en' or language=='id'):#Any language using ASCII characters goes here
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
		if(word_list!=None):
			doc.append(gs.models.doc2vec.TaggedDocument(words=word_list,tags=[filename]))  
		else:
			print(filename+" failed to load")
	return doc

def train_model_pvdm(directory,language):#en-English,id-Hindi
	doc=load_documents(findFiles(directory,['%s-text'%(language)]),language)
	print(type(doc))
	print doc
	if doc == []:
		return 0
	model=gs.models.doc2vec.Doc2Vec(doc, size=50, min_count=1, window=8, negative=10, workers=4, sample=1e-5)
	return model

def train_model_pvdbow(directory):
	doc=load_documents(findFiles(directory,['tag']),"en")
	if doc == []:
		return 0
	model=gs.models.doc2vec.Doc2Vec(doc, size=50, min_count=3, window=8, negative=10, workers=4, sample=1e-5, dm=0) #Apply PV-DBOW
	return model

models=['en-text','id-text','tag']
for f in models:
	if(os.path.isfile(os.path.join(op_dir,'model',f))):
		os.remove(os.path.join(op_dir,'model',f))

if not os.path.exists(os.path.join(op_dir,'model')):
	os.makedirs(os.path.join(op_dir,'model'))

en_model_text=train_model_pvdm(op_dir,'en')
if not en_model_text == 0:
	en_model_text.save(os.path.join(op_dir,'model','en-text'))
else:
	logging.info('english data not present')

hi_model_text=train_model_pvdm(op_dir,'id')
if not hi_model_text == 0:
	hi_model_text.save(os.path.join(op_dir,'model','id-text'))
else:
	logging.info('hindi data not present')

model_tags=train_model_pvdbow(op_dir)
if not model_tags == 0:
	model_tags.save(os.path.join(op_dir,'model','tag'))
else:
	logging.info('tags data not present')


