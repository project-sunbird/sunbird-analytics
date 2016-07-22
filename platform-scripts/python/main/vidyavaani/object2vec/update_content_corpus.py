import json
import codecs
import os
import argparse #Accept commandline arguments
import logging #Log the data given
import re
import langdetect
import sys
import ConfigParser
langdetect.DetectorFactory.seed=0

from math import fabs
from operator import itemgetter

root=os.path.dirname(os.path.abspath(__file__))
utils=os.path.join(os.path.split(root)[0],'utils')
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in 
import find_files
import get_lowest_key_value
root=os.path.dirname(os.path.abspath(__file__))

#Define commandline arguments
# parser = argparse.ArgumentParser()
# parser.add_argument('--json',help='This is the json directory',default=os.path.join(os.path.join(root,'content2EnrichedJson'),'Data'))
# parser.add_argument('--corpus',help='This is the corpus directory',default=os.path.join(os.path.join(root,'corpus2Vec'),'Corpus'))

#getiing paths from config file
config = ConfigParser.RawConfigParser()
config.read(config_file)
corpus_dir = config.get('FilePath','corpus_path')
log_dir = config.get('FilePath','log_path')
#Read arguments given
# args = parser.parse_args()
# json_dir=args.json
# corpus_dir=args.corpus

#Set up logging
logfile_name = os.path.join(log_dir,'update_content_corpus.log')
logging.basicConfig(filename=logfile_name,level=logging.DEBUG)
logging.info('update_content_corpus')

"""def find_word(ls,word):
	index=[]
	for i in range(len(ls)):
		if(word==ls[i][0]):
			index.append(i)
	return index"""

#This function is used to get a final string from a list of alternatives
def merge_strings(transcription):
	keys=transcription.keys()
	final_transcription={}
	for key in keys:
		if(len(transcription[key])==0):
			final_transcription[key]=''
		else:
			final_transcription[key]=transcription[key]['alternative'][0]['transcript'].lower()
	return final_transcription
#The following commented code returns the same result as the highest confidence string but was computed as the merging of all 5 strings and thus merging this way is redundant when trying to get the final string. However this may be useful in case we have to predict areas of inconfidence. For this purpose should return the string at the stage where the print string command has mentioned that this is the step where inconfidence still exists and has not been removed by augmenting with top result
"""	window_size=2#This allows for duplicate words in the same speech recognition. It allows for variability in the positions accross different choices but ensuring that a duplicate word is still accepted as a new alternative
	keys=transcription.keys()
	final_transcription=[]
	for key in keys:
		print(key)
		max_length=0
		ls=[]
		vocab=[]
		for alternative in (transcription[key]['alternative']):
			word_list=alternative['transcript'].lower().split(' ')				
			max_length=max(len(word_list),max_length)				
			ls.append(word_list)
			for word in word_list:
				idx=word_list.index(word)
				index=find_word(vocab,word)
				if len(index)>0:
					added=False
					for ind in index:
						if(fabs(vocab[ind][1]-idx)<window_size):
							vocab[ind][1]=max(vocab[ind][1],idx)
							vocab[ind][2]+=1
							added=True
							break
					if(not added):
						vocab.append([word,idx,1])				
				else:
					vocab.append([word,idx,1])
		string=['' for i in range(max_length)]
		for word in vocab:
			if(word[2]>=4 and string[word[1]]==''):
				string[word[1]]=word[0]
		print(' '.join(string))#Print here to get areas of inconfidence
		for idx in range(min(len(string),len(ls[0]))):
			if(string[idx]=='' and ls[0][idx] not in string[max(idx-window_size,0):min(idx+window_size,len(ls[0]))]):
				string[idx]=ls[0][idx]
		string=[word for word in string if word!='']
		print(' '.join(string))
		print(' '.join(ls[0]))
		final_transcription.append(' '.join(string))
	return final_transcription"""

#Process json to get text
def process_data(json_dictionary):
	regex=re.compile('[^0-9]')
	processed={}
	for key in json_dictionary.keys():
		filename=key.split(',')[-2].split('/')[-1]
		item_number=regex.sub('',key.split(',')[0])
		if(item_number!=''):
			data=(int(item_number),'\n'.join(json_dictionary[key]))
			if(filename in processed):
				processed[filename].append(data)
			else:
				processed[filename]=[data]
	for k in processed.keys():
		processed[k]=sorted(processed[k],key=itemgetter(0))
		processed[k]='\n'.join([unicode(item[1]) for item in processed[k]])
	return(processed)

def uniqfy_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

if not os.path.isdir(corpus_dir):
	os.makedirs(corpus_dir)

#jsonFiles=findFiles.findFiles(json_dir,['.json'])
lst_language = []
jsonFiles = sys.stdin
for data in jsonFiles:
	json_data = json.loads(data)
	identifier = json_data['identifier']
	max_tag_length=5
	path=os.path.join(corpus_dir,identifier)
	if not os.path.isdir(path):
		os.makedirs(path)

	tags=[concept for concept in json_data['concepts']]
	#Data	
	x=set()
	data_list=json.loads(''.join(json_data['data']),encoding='utf-8')
	# data_list=json.loads(''.join(data['data']),encoding='utf-8')
	for key in data_list.keys():
		x.add(''.join(process_data(get_lowest_key_value.flattenDict(data_list[key])).values()))
	string='\n'.join(list(x))
	#mp3
	mp3_string=''
	dat=merge_strings(json_data['mp3Transcription']).values()
	for item in dat:
		if(len(item.split(' '))>max_tag_length):
			mp3_string+='\n'+item
		else:
			tags.append(item)
	text=True
	if(len(string)>0 and len(mp3_string)>0):#There exist both stories and mp3 transcription
		#Detect language of string		
		string_language=langdetect.detect(string)
		mp3_language=langdetect.detect(mp3_string)
		lst_language.append(string_language)
		lst_language.append(mp3_language)
		if(string_language==mp3_language):#Both same langauges
			string+='\n'+mp3_string
			with codecs.open(os.path.join(path,'%s-text'%(string_language)),'w',encoding='utf-8') as f:
				f.write(string)
			f.close()
		else:#If different languages, then create separate files
			with codecs.open(os.path.join(path,'%s-text'%(string_language)),'w',encoding='utf-8') as f:
				f.write(string)
			f.close()
			with codecs.open(os.path.join(path,'%s-text'%(mp3_language)),'w',encoding='utf-8') as f:
				f.write(mp3_string)
			f.close()
	elif(len(string)>0):#Only stories
		string_language=langdetect.detect(string)
		with codecs.open(os.path.join(path,'%s-text'%(string_language)),'w',encoding='utf-8') as f:
			f.write(string)
		f.close()
	elif(len(mp3_string)>0):#Only mp3 transcription
		mp3_language=langdetect.detect(mp3_string)
		with codecs.open(os.path.join(path,'%s-text'%(mp3_language)),'w',encoding='utf-8') as f:
			f.write(mp3_string)
		f.close()
	else:
		text=False
	tags_data=True
	if(len(tags)>0):#Non zero tags
		with codecs.open(os.path.join(path,'tags'),'w',encoding='utf-8') as f:
			f.write(','.join(tags))
		f.close()
	else:
		tags_data=False
	if(not text and not tags_data):#No metadata
		logging.info('Fail:%s'%(identifier))
		print("False")
	else:
		print("True")

lst_language = uniqfy_list(lst_language)
#print lst_language
# text_file = open(os.path.join(corpus_dir,'language.txt'), "w")
# text_file.write(lst_language)
# text_file.close()

