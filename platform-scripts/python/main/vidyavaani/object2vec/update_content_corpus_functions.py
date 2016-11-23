import json
#import simplejson as json
import codecs
import os
import argparse  # Accept commandline arguments
import logging  # Log the data given
import re
import langdetect
import sys
import ConfigParser
import traceback

langdetect.DetectorFactory.seed = 0

from math import fabs
from operator import itemgetter

root = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join(os.path.split(root)[0], 'utils')
resource = os.path.join((os.path.split(root)[0]), 'resources')
config_file = os.path.join(resource, 'config.properties')
# Insert at front of list ensuring that our util is executed first in
sys.path.insert(0, utils)
import find_files
import get_lowest_key_value
from get_lowest_key_value import flattenDict
from get_all_values import getAllValues

root = os.path.dirname(os.path.abspath(__file__))

# getiing paths from config file
config = ConfigParser.SafeConfigParser()
config.read(config_file)
corpus_dir = config.get('FilePath', 'corpus_path')
log_dir = config.get('FilePath', 'log_path')

# adding the log_dir to root
#log_dir = os.path.join(root,log_dir)
#corpus_dir = os.path.join(root,corpus_dir)

# defualt language code (for creating corpus)
DEFAULT_LANG_CODE = 'en'

def createDirectory(dir):
    try:
        if not os.path.exists(dir):
            os.makedirs(dir)
    except OSError, e:
        if e.errno != 17:
            traceback.print_exc()
            sys.exit(1)
    except:
        traceback.print_exc()
        msg = 'Not able to find/create log and/or tmp dir'
        logging.warn(msg)
        sys.exit(1)

# check if paths exists
createDirectory(corpus_dir)
createDirectory(log_dir)
createDirectory(corpus_dir)

max_tag_length = 5

# Set up logging
logfile_name = os.path.join(log_dir, 'update_content_corpus.log')
logging.basicConfig(filename=logfile_name, level=logging.DEBUG)
logging.info('update_content_corpus')

"""def find_word(ls,word):
	index=[]
	for i in range(len(ls)):
		if(word==ls[i][0]):
			index.append(i)
	return index"""

# This function is used to get a final string from a list of alternatives


def merge_strings(transcription):
    keys = transcription.keys()
    final_transcription = {}
    for key in keys:
        if key == 'confidence':
            continue
        if(len(transcription[key]) == 0):
            final_transcription[key] = ''
        else:
            final_transcription[key] = transcription[key][
                'alternative'][0]['transcript'].lower()
    return final_transcription
# The following commented code returns the same result as the highest
# confidence string but was computed as the merging of all 5 strings and
# thus merging this way is redundant when trying to get the final string.
# However this may be useful in case we have to predict areas of
# inconfidence. For this purpose should return the string at the stage
# where the print string command has mentioned that this is the step where
# inconfidence still exists and has not been removed by augmenting with
# top result
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

# Process json to get text


def process_data(json_dictionary):
    regex = re.compile('[^0-9]')
    processed = {}
    for key in json_dictionary.keys():
        filename = key.split(',')[-2].split('/')[-1]
        item_number = regex.sub('', key.split(',')[0])
        if(item_number != ''):
            data = (int(item_number), ''.join(json_dictionary[key]))
            if(filename in processed):
                processed[filename].append(data)
            else:
                processed[filename] = [data]
    for k in processed.keys():
        processed[k] = sorted(processed[k], key=itemgetter(0))
        processed[k] = ''.join([unicode(item[1]) for item in processed[k]])
    return(processed)


def uniqfy_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

# jsonFiles=findFiles.findFiles(json_dir,['.json'])
# jsonFiles = sys.stdin
# print contentPayload

# append everything except text, and description into tags"
# tags could be in multiple languages


def updateContentCorpus(reqJson):

    try:
        contentPayload = json.loads(reqJson)
    except:
        msg = 'Not able to read input json stream'
        logging.info(msg)
        return(1)
    print contentPayload
    # get the key info
    try:
        mustHavekeysFromContentModel = contentPayload[
            'mustHavekeysFromContentModel']  # array
        goodToHaveKeysFromContentModel = contentPayload[
            'goodToHaveKeysFromContentModel']
        enrichedKeysFromML = contentPayload['enrichedKeysFromML']  # array
    except:
        traceback.print_exc()
        msg = 'Not able to get minimum required fields from enriched content module'
        logging.info(msg)
        return(1)
    try:
        identifier = contentPayload['identifier']
        # print identifier
    except:
        traceback.print_exc()
        msg = 'Not able to identify the Content'
        logging.info(msg)
        return(1)
    try:
        # create corpus dir
        path = os.path.join(corpus_dir, identifier)
        if not os.path.isdir(path):
            os.makedirs(path)
    except:
        traceback.print_exc()
        msg = 'Could not find/create corpus_dir'
        logging.info(msg)
        return(1)

    # get the minimal output
    text = []
    if contentPayload.has_key('text'):
        val = contentPayload['text']
        if val:
            text = contentPayload['text']
    tags = []
    if contentPayload.has_key('tags'):
        val = contentPayload['tags']
        if val:
            tags = contentPayload['tags']

    # add "description" key to tex
    if contentPayload.has_key('description'):
        val = contentPayload['description']
        if val:
            # string is not empty
            text.append(val)

    # add concept to tags
    try:
        if contentPayload.has_key('concepts'):
            conceptList = [concept for concept in contentPayload['concepts']]
            if conceptList:
                tags.extend(conceptList)
    except:
        msg = 'Could not add concept to tags'
        logging.info(msg)

    # add everything under good2have keys and put it under tags
    try:
        for key in goodToHaveKeysFromContentModel:
            # print key
            if contentPayload.has_key(key):
                val = getAllValues(contentPayload[key])
                if val:
                    # print val
                    tags.extend(val)
    except:
        msg = 'Could not add grade, language etc. to tags'
        logging.info(msg)

    # handle data (text) json carefully
    try:
        x = set()
        if contentPayload.has_key('data'):
            val = contentPayload['data']
            if val:
                data_list = json.loads(
                    ''.join(contentPayload['data']), encoding='utf-8')
                # data_list=json.loads(''.join(data['data']),encoding='utf-8')
                for key in data_list.keys():
                    x.add(''.join(process_data(
                        get_lowest_key_value.flattenDict(data_list[key])).values()))
                # add string to text
            text.extend(list(x))
    except:
        msg = 'Could not add derived text to text doc'
        logging.info(msg)

    try:
        # process speech data
        # mp3_string=''
        if contentPayload.has_key('mp3Transcription'):
            val = contentPayload['mp3Transcription']
            if val:
                dat = merge_strings(
                    contentPayload['mp3Transcription']).values()
                for item in dat:

                    if(len(item.split(' ')) > max_tag_length):
                        text.extend(item)
                    else:
                        tags.extend(item)
    except:
        msg = 'Could not add mp3 transcription to either text or tag'
        logging.info(msg)

    try:
        # process image label data
        # mp3_string=''
        if contentPayload.has_key('imageTags'):
            val = contentPayload['imageTags']
            if val:
                dat = list(merge_strings(contentPayload['imageTags']).values())
                tags.extend(dat)
    except:
        msg = 'Could not add image labels to either text or tag'
        logging.info(msg)

    # taking the language defined in json instead of detetecting (WIP)
    # only one-lang per content (can change later)
    if contentPayload.has_key('languageCode'):
        lang_code = contentPayload['languageCode']
    else:
        lang_code = DEFAULT_LANG_CODE

    # write text document
    corpus_dict = {}
    # print path
    with codecs.open(os.path.join(path, '%s-text' % (lang_code)), 'w', encoding='utf-8') as f:
        unwrapped_text = '.'.join(text)
        f.write(unwrapped_text)
    corpus_dict[lang_code] = unwrapped_text
    f.close()

    with codecs.open(os.path.join(path, 'tags'), 'w', encoding='utf-8') as f:
        unwrapped_tags = ','.join(tags)
        f.write(unwrapped_tags)
    corpus_dict['tags'] = unwrapped_tags
    f.close()

    # print corpus_dic
    print(json.dumps(corpus_dict))

    # string_language=json_data['languageCode']
    # mp3_language=json_data['languageCode']

    # if(len(string)>0 and len(mp3_string)>0):#There exist both stories and mp3 transcription
    # 	#Detect language of string
    # 	# string_language=langdetect.detect(string)
    # 	# mp3_language=langdetect.detect(mp3_string)
    # 	if(string_language==mp3_language):#Both same langauges
    # 		string+=mp3_string
    # 		with codecs.open(os.path.join(path,'%s-text'%(string_language)),'w',encoding='utf-8') as f:
    # 			f.write(string)
    # 			corpus_dict[string_language] = string
    # 		f.close()
    # 	else:#If different languages, then create separate files
    # 		with codecs.open(os.path.join(path,'%s-text'%(string_language)),'w',encoding='utf-8') as f:
    # 			f.write(string)
    # 			corpus_dict[string_language] = string
    # 		f.close()
    # 		with codecs.open(os.path.join(path,'%s-text'%(mp3_language)),'w',encoding='utf-8') as f:
    # 			f.write(mp3_string)
    # 			corpus_dict[string_language] = mp3_string
    # 		f.close()
    # elif(len(string)>0):#Only stories
    # 	# string_language=langdetect.detect(string)
    # 	with codecs.open(os.path.join(path,'%s-text'%(string_language)),'w',encoding='utf-8') as f:
    # 		f.write(string)
    # 		corpus_dict[string_language] = string
    # 	f.close()
    # elif(len(mp3_string)>0):#Only mp3 transcription
    # 	# mp3_language=langdetect.detect(mp3_string)
    # 	with codecs.open(os.path.join(path,'%s-text'%(mp3_language)),'w',encoding='utf-8') as f:
    # 		f.write(mp3_string)
    # 		corpus_dict[string_language] = mp3_string
    # 	f.close()
    # else:
    # 	text=False
    # tags_data=True
    # if(len(tags)>0):#Non zero tags
    # 	with codecs.open(os.path.join(path,'tags'),'w',encoding='utf-8') as f:
    # 		f.write(','.join(tags))
    # 		corpus_dict[string_language] = ','.join(tags)
    # 	f.close()
    # else:
    # 	tags_data=False
    # if(not text and not tags_data):#No metadata
    # 	logging.info('Fail:%s'%(identifier))
    # 	print("False")
    # else:
    # 	print(json.dumps(corpus_dict))