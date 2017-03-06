import requests
import pandas as pd
import numpy as np
import json
import time
import warnings
import logging
import ConfigParser
import os
import sys
import ast
from json_load import *
environment = sys.argv[1]

# getting paths from config file
config = ConfigParser.SafeConfigParser()
config.read('config.properties')
# environment = config.get('Environment', 'environ')
environment_path = config.get('Environment_Path', environment)

warnings.filterwarnings("ignore")
logging.basicConfig(filename='concept_coverage_across_content.log',
                    format='%(levelname)s:%(message)s', level=logging.INFO)


def get_concept_df():
    url_numeracy = environment_path+"/learning/v2/domains/numeracy/concepts"
    url_literacy = environment_path+"/learning/v2/domains/literacy/concepts"
    resp_num = requests.get(url_numeracy).json()
    resp_lit = requests.get(url_literacy).json()

    concepts_list = resp_num["result"]["concepts"]
    concept_num_df = pd.DataFrame(concepts_list)
    concepts_list = resp_lit["result"]["concepts"]
    concept_lit_df = pd.DataFrame(concepts_list)

    concept_df = pd.concat([concept_num_df, concept_lit_df])
    concept_df = concept_df.reset_index()
    concept_df.is_copy = False
    return concept_df


def get_parent(x, concept_df):
    d = concept_df.parent[concept_df.identifier == x].iloc[0][0]
    parent = d['identifier']
    o_type = d['objectType']
    return [parent, o_type]


def get_grade(x):
    if isNaN(x):
        return 0
    else:
        try:
            grade = int(x[0].strip("Grade ").encode('ascii'))
            return grade
        except:
            return 0


# to check if num is NaN
def isNaN(num):
    return num != num


def get_depth(x, concept_df):
    depth = 1
    o_type = ''
    while o_type != 'Dimension':
        try:
            x, o_type = get_parent(x, concept_df)
            depth += 1
        except:
            break
    return depth


def get_content_df():
    url = environment_path+"/learning/v2/content/list"
    payload = "{\"request\": {\"search\": {\"status\": [\"Live\"],\"limit\": 2000}}}"
    headers = {
        'content-type': "application/json",
        'user-id': "mahesh",
        'cache-control': "no-cache",
        'postman-token': "d0fafff9-911a-9a91-2016-cfc4714cf543"
    }
    resp = requests.request("POST", url, data=payload, headers=headers).json()
    content_list = resp["result"]["content"]
    content_df = pd.DataFrame(content_list)
    return content_df


def get_concept(i, content_df):
    lst_concept = []
    try:
        list_of_concepts = content_df.concepts[i]
        for concept_dict in list_of_concepts:
            lst_concept.append(concept_dict['identifier'])
    except:
        lst_concept = []
    return lst_concept


def add_concept_details(concept_df):
    concept_df['Depth'] = concept_df['identifier'].apply(lambda x: get_depth(x, concept_df))
    lst_all_concept = concept_df['identifier'].tolist()
    concept_df['DescLen'] = concept_df['description'].map(lambda x: 0 if isNaN(x) else len(x))
    concept_df['grade'] = concept_df['gradeLevel'].apply(lambda x: get_grade(x))
    return concept_df, lst_all_concept


def create_content_usage_df(content_df, lst_all_concept):
    data = np.zeros((content_df.shape[0], len(lst_all_concept) + 1), dtype=np.int16)
    # create binary df
    df_micro = pd.DataFrame(data)
    df_micro.columns = ['contentID'] + lst_all_concept
    return df_micro


def add_concept_usage_count(content_df, df_micro, lst_all_concept):
    for i in range(content_df.shape[0]):
        concept = get_concept(i, content_df)
        df_micro['contentID'].iloc[i] = content_df['identifier'].iloc[i]
        for c in concept:
            if c in lst_all_concept:
                df_micro[c].iloc[i] = 1
                # df_micro.iloc[i, c] = 1
    return df_micro


def create_sorted_df(df_micro):
    df_micro_used = pd.DataFrame(df_micro[df_micro.columns.difference(['contentID'])].sum(axis=0))
    df_micro_used.columns = ['times_used']
    df_micro_used['micro_concept'] = df_micro_used.index
    df_micro_used.index = range(df_micro_used.shape[0])
    df_micro_used = df_micro_used[['micro_concept', 'times_used']]

    df_micro_used_sorted = df_micro_used.sort_values(['times_used'], ascending=[False])
    df_micro_used_sorted = df_micro_used_sorted.reset_index()
    df_micro_used_sorted = df_micro_used_sorted[['micro_concept', 'times_used']]
    df_micro_used.to_csv('fixing.csv')
    # df_micro_used_sorted['Details'] = ''
    # df_micro_used_sorted['Description'] = ''
    # df_micro_used_sorted['domain'] = ''
    # df_micro_used_sorted['depth'] = ''
    return df_micro_used_sorted


# add details
def get_contentIDs(x, df_micro):
    lst_content = ""
    for i in range(df_micro.shape[0]):
        if df_micro[x].iloc[i] == 1:
            lst_content = lst_content + "," + df_micro['contentID'].iloc[i]
    return lst_content[1:]


def add_details_to_sorted_df(df_micro_used_sorted, df_micro, concept_df):
    for j in range(df_micro_used_sorted.shape[0]):
        lst_content = get_contentIDs(df_micro_used_sorted['micro_concept'].iloc[j], df_micro)
        df_micro_used_sorted['Details'].iloc[j] = lst_content

    for i in range(df_micro_used_sorted.shape[0]):
        cID = df_micro_used_sorted.micro_concept.iloc[i]
        df_micro_used_sorted.Description.iloc[i] = concept_df.loc[concept_df['identifier'] == cID, 'description'].iloc[0]
        df_micro_used_sorted.domain.iloc[i] = concept_df.loc[concept_df['identifier'] == cID, 'subject'].iloc[0]
        df_micro_used_sorted.depth.iloc[i] = concept_df.loc[concept_df['identifier'] == cID, 'Depth'].iloc[0]
    return df_micro_used_sorted


# save dataframe in text file
def save_dataframe(df, filename):
    list_records = json_loads_byteified(df.to_json(orient='records'))
    if not os.path.exists('metrics'):
        os.makedirs('metrics')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'metrics', filename)
    outfile = open(file_path, "w")
    print >> outfile, "\n".join(str(i) for i in list_records)
    outfile.close()


def main():
    concept_df = get_concept_df()
    logging.info('concept model retrieved')
    concept_df, lst_all_concept = add_concept_details(concept_df)
    content_df = get_content_df()
    logging.info('content model retrieved')
    df_micro = create_content_usage_df(content_df, lst_all_concept)
    # df_micro.to_csv('fixing.csv')
    df_micro = add_concept_usage_count(content_df, df_micro, lst_all_concept)
    df_micro_used_sorted = create_sorted_df(df_micro)
    df_micro_used_sorted.columns = ['concept_id', 'contents']
    # df_micro_used_sorted = add_details_to_sorted_df(df_micro_used_sorted, df_micro, concept_df)
    filename = time.strftime("%Y-%m-%d") + '_concept_coverage_across_content.json'
    save_dataframe(df_micro_used_sorted, filename)
    logging.info('saved to json')
    print "Generated metrics of concept coverage across contents..."

main()