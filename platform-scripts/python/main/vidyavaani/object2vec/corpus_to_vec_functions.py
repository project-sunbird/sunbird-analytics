
import gensim as gs
import os
import numpy as np
import codecs
import re
import logging  # Log the data given
import sys
import ast
import ConfigParser
from nltk.corpus import stopwords
stopword = set(stopwords.words("english"))

#for LDA
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer
from gensim import corpora, models, similarities

root = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join(os.path.split(root)[0], 'utils')
resource = os.path.join((os.path.split(root)[0]), 'resources')
config_file = os.path.join(resource, 'config.properties')
# Insert at front of list ensuring that our util is executed first in
sys.path.insert(0, utils)
# To find files with a particular substring
from find_files import findFiles

config = ConfigParser.SafeConfigParser()
config.read(config_file)

# op_dir = config.get('FilePath', 'corpus_path')
# log_dir = config.get('FilePath', 'log_path')
# model_loc = config.get('FilePath', 'model_path')


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

# LDA params
lda_topics = int(config.get('LDA', 'topics'))
lda_passes = int(config.get('LDA', 'passes'))

def process_text(lines, language):  # Text processing
    word_list = []
    for line in lines:
        if(language == 'en-text'):  # Any language using ASCII characters goes here
            line = re.sub("[^a-zA-Z]", " ", line)
            for word in line.split(' '):
                if word not in stopword and len(word) > 1:
                    word_list.append(word.lower())
        elif(language == 'tags'):
            pre_query = line.split(",")
            word_list = []
            for str_word in pre_query:
                word_list.append("".join(str_word.split()).lower())
                word_list = uniqfy_list(word_list)  
        else:
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
        return []


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

def load_documents_LDA(filenames, language)
    doc = []
    flag = 0
    for filename in filenames:
        word_list = process_file(filename, language)
        if word_list:
            doc.append(word_list)
            flag = 1
        else:
            logging.info(filename + " failed to load in load_documents")
    if flag:
        flattened_doc = [val for sublist in doc for val in sublist]
    else:
        flattened_doc = doc
    return flattened_doc


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

def train_model_LDA(directory, language):
    if language == ['tags']:
        texts = load_documents_LDA(findFiles(directory, ['tag']), "en-text")
    else:
        texts = load_documents_LDA(findFiles(directory, [language]), language)
    if not doc:
        return 0    
    # turn our tokenized documents into a id <-> term dictionary
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]

    # generate LDA model
    ldamodel = gs.models.ldamodel.LdaModel(corpus, num_topics=lda_topics, id2word = dictionary, passes=lda_passes)
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