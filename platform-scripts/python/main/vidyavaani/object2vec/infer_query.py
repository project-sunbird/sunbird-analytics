import os
import sys
import logging  # Log the data given
import ConfigParser
import json

root = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join((os.path.split(root)[0]), 'utils')
# Insert at front of list ensuring that our util is executed first in
sys.path.insert(0, utils)
from find_files import *
resource = os.path.join((os.path.split(root)[0]), 'resources')
config_file = os.path.join(resource, 'config.properties')

# inputs
#std_input = json.loads(sys.stdin.read())
std_input = sys.stdin.readline()
# print std_input

std_input = json.loads(std_input)

inferFlag = std_input['infer_all']
op_dir = std_input['corpus_loc']
model_loc = std_input['model']

# geting paths from config file
config = ConfigParser.SafeConfigParser()
config.read(config_file)

# op_dir = config.get('FilePath', 'corpus_path')
log_dir = config.get('FilePath', 'log_path')
use_LDA = config.get('Training', 'use_LDA')

if not os.path.exists(model_loc):
    logging.info('model folder do not exist')

# Set up logging
infer_log_file = os.path.join(log_dir, 'inferQuery.log')

# commented out (as giving errors)
# test and remove the comments below
logging.basicConfig(filename=infer_log_file, level=logging.DEBUG)
logging.info('Corpus to Vectors')
if use_LDA == 'false':
	infer_query()
else:
	infer_query_LDA()