import os
import gensim as gs
import argparse #Accept commandline arguments
import logging #Log the data given
import numpy as np
root=os.path.dirname(os.path.abspath(__file__))

#Define commandline arguments
parser = argparse.ArgumentParser()
parser.add_argument('q',help='This is the search string (" "separated)')
parser.add_argument('--ld',help='This is the operating directory',default=os.path.join(root,'Corpus'))

#Read arguments given
args = parser.parse_args()
op_dir=args.ld
query=args.q
#Set up logging
logging.basicConfig(filename=os.path.join(op_dir,'corpus2Vec.log'),level=logging.DEBUG)
logging.info('Corpus to Vectors')

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
	print(distance)
	return model.docvecs.index_to_doctag(predicted_idx)

en_model=gs.models.doc2vec.Doc2Vec.load(os.path.join(op_dir,'en-text'))
hi_model=gs.models.doc2vec.Doc2Vec.load(os.path.join(op_dir,'id-text'))
tag_model=gs.models.doc2vec.Doc2Vec.load(os.path.join(op_dir,'tag'))

print("English",inference(query,en_model))
print("Hindi",inference(query,hi_model))
print("Tag",inference(query,tag_model))

