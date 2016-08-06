# Author: Aditya Arora, adityaarora@ekstepplus.org

import requests
import codecs
import os
import json
# Pass as a commandline argument later on
root = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join(os.path.split(root)[0], 'utils')
import sys
# Insert at front of list ensuring that our util is executed first in from
# getAllValues import *
sys.path.insert(0, utils)
from get_all_values import getAllValues

# This finds the concepts from the assessment data
def filter_assessment_data(directory, conceptList):
    concList = set([])
    item_list = []
    with codecs.open(os.path.join(directory, 'items.json'), 'r', encoding='utf-8') as f:
        items = f.read()
        if items:
            item_list = json.loads(items)
    f.close()
    if item_list:
        for key in item_list.keys():
            vals = getAllValues(item_list[key])
            for item in vals:
                if(item in conceptList):
                    concList.add(item)
    return list(concList)