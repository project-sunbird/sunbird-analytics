import os
import sys
import gensim as gs
import argparse #Accept commandline arguments
import logging #Log the data given
import numpy as np
import ConfigParser
import langdetect
langdetect.DetectorFactory.seed=0

root=os.path.dirname(os.path.abspath(__file__))
utils=os.path.join((os.path.split(root)[0]),'utils')
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')


def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

#getiing paths from config file
config = ConfigParser.RawConfigParser()
config.read(config_file)
op_dir = config.get('FilePath', 'corpus_path')
log_dir = config.get('FilePath','log_path')

lst_folder = get_immediate_subdirectories(op_dir)
lst_folder.remove('model')

#Set up logging
logging.basicConfig(filename=os.path.join(log_dir,'corpus2Vec.log'),level=logging.DEBUG)
logging.info('Corpus to Vectors')

for folder in lst_folder:
	content_folder = os.path.join(op_dir,folder)
	file_path = os.path.join(content_folder,'id-text')
	if not os.path.exists(file_path):
		print 0	
		sys.exit()
	txt = open(file_path)
	query = txt.read()	
	language = langdetect.detect(query)
	model_path = os.path.join(op_dir,'model','%s-text'%(language))
	if not os.path.exists(model_path):
		print 0
		sys.exit()
	model=gs.models.doc2vec.Doc2Vec.load(model_path)
	# tag_model=gs.models.doc2vec.Doc2Vec.load(os.path.join(op_dir,'tag'))
	q_vec=model.infer_vector(query)
	vector_list = np.array(q_vec).tolist()
	print vector_list

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

# language = langdetect.detect(query)
# path = os.path.join(op_dir,'model','%s-text'%(language))
# if not os.path.exists(path):
# 	print "False"

# en_model=gs.models.doc2vec.Doc2Vec.load(os.path.join(op_dir,'en-text'))
# model=gs.models.doc2vec.Doc2Vec.load(path)
# tag_model=gs.models.doc2vec.Doc2Vec.load(os.path.join(op_dir,'tag'))
# q_vec=model.infer_vector(query)
# vector_list = np.array(q_vec).tolist()
# print vector_list
# print("English",inference(query,en_model))
#print("Hindi",inference(query,hi_model))
# print("Tag",inference(query,tag_model))

