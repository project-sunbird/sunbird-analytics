# Author: Aditya Arora, adityaarora@ekstepplus.org

import gensim as gs
import os
import numpy as np
import codecs
import re
import numpy as np
import logging  # Log the data given
import sys
import ast
from nltk.corpus import stopwords
import ConfigParser

# Using utility functions
root = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join(os.path.split(root)[0], 'utils')
resource = os.path.join((os.path.split(root)[0]), 'resources')
config_file = os.path.join(resource, 'config.properties')
# Insert at front of list ensuring that our util is executed first in
sys.path.insert(0, utils)
# To find files with a particular substring
from find_files import findFiles

# get file path from config file
config = ConfigParser.SafeConfigParser()
config.read(config_file)

op_dir = config.get('FilePath', 'corpus_path')
log_dir = config.get('FilePath', 'log_path')
model_loc = config.get('FilePath', 'model_path')


# get parameters from config file
language_model = config.get('Training', 'language_model')
tags_model = config.get('Training', 'tags_model')

# pvdm params
pvdm_size = int(config.get('pvdm', 'size'))
pvdm_min_count = int(config.get('pvdm', 'min_count'))
pvdm_window = int(config.get('pvdm', 'window'))
pvdm_negative = int(config.get('pvdm', 'negative'))
pvdm_workers = int(config.get('pvdm', 'workers'))
pvdm_sample = float(config.get('pvdm', 'sample'))

# pvdbow params
pvdbow_size = int(config.get('pvdbow', 'size'))
pvdbow_min_count = int(config.get('pvdbow', 'min_count'))
pvdbow_window = int(config.get('pvdbow', 'window'))
pvdbow_negative = int(config.get('pvdbow', 'negative'))
pvdbow_workers = int(config.get('pvdbow', 'workers'))
pvdbow_sample = float(config.get('pvdbow', 'sample'))
pvdbow_dm = int(config.get('pvdbow', 'dm'))

# Set up logging
logfile_name = os.path.join(log_dir, 'corpus_to_vec.log')
logging.basicConfig(filename=logfile_name, level=logging.INFO)
logging.info('Corpus to Vectors')

# check if paths existss
if not os.path.exists(op_dir):
    logging.info('Corpus folder do not exist')
    os.makedirs(op_dir)

if not os.path.exists(model_loc):
    logging.info('Model folder do not exist')
    os.makedirs(model_loc)    


# get parameters from config file


stopword = set(stopwords.words("english"))


def process_text(lines, language):  # Text processing
    word_list = []
    for line in lines:
        if(language == 'en-text'):  # Any language using ASCII characters goes here
            line = re.sub("[^a-zA-Z]", " ", line)
            for word in line.split(' '):
                if word not in stopword and len(word) > 1:
                    word_list.append(word.lower())
        else:  # Any language using unicode characters goes here
            # line=' '.join([i for i in line if ord(i)>127 or ord(i)==32])
            # Remove ASCII Characters since the language is not english(This
            # will remove punctuation as well)
            for word in line.split(' '):
                word_list.append(word.lower())
    return word_list


def process_file(filename, language):  # File processing
    try:
        with codecs.open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        f.close()
        return process_text(lines, language)
    except:
        return


def load_documents(filenames, language):  # Creating TaggedDocuments
    doc = []
    for filename in filenames:
        word_list = process_file(filename, language)
        # if(word_list!=None):
        if word_list:
            doc.append(gs.models.doc2vec.TaggedDocument(
                words=word_list, tags=[filename]))
        else:
            logging.info(filename + " failed to load in load_documents")
    return doc


def train_model_pvdm(directory, language):
    if language == ['tags']:
        doc = load_documents(findFiles(directory, ['tag']), "en-text")
    else:
        doc = load_documents(findFiles(directory, [language]), language)
    if not doc:
        return 0
    model = gs.models.doc2vec.Doc2Vec(doc, size=pvdm_size, min_count=pvdm_min_count,
                                      window=pvdm_window, negative=pvdm_negative, workers=pvdm_workers, sample=pvdm_sample)
    return model


def train_model_pvdbow(directory, language):
    if language == ['tags']:
        doc = load_documents(findFiles(directory, ['tag']), "en-text")
    else:
        doc = load_documents(findFiles(directory, [language]), language)
    if not doc:
        return 0
    model = gs.models.doc2vec.Doc2Vec(doc, size=pvdbow_size, min_count=pvdbow_min_count, window=pvdbow_window,
                                      negative=pvdbow_negative, workers=pvdbow_workers, sample=pvdbow_sample, dm=pvdbow_dm)  # Apply PV-DBOW
    return model


def uniqfy_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def get_all_lang(directory, string):
    lst_lang = [name
                for root, dirs, files in os.walk(directory)
                for name in files
                if name.endswith((string))]
    lst_lang = uniqfy_list(lst_lang)
    return lst_lang

models_lang = get_all_lang(op_dir, "-text")
models_tags = get_all_lang(op_dir, "tags")

# remove existing models
for f in models_lang:
    if(os.path.isfile(os.path.join(model_loc, f))):
        os.remove(os.path.join(model_loc, f))

# building model for language
if models_lang:
    for model in models_lang:
        function = 'train_model_%s' % language_model
        gensim_model = eval(function)(op_dir, model)
        if not gensim_model == 0:
            gensim_model.save(os.path.join(model_loc, model))
        else:
            logging.info('%s data not present' % (model))

# building model for tags
if models_tags:
    function = 'train_model_%s' % tags_model
    model_tags = eval(function)(op_dir, models_tags)
    if not model_tags == 0:
        model_tags.save(os.path.join(model_loc, 'tags'))
    else:
        logging.info('tags data not present')

print model_loc
