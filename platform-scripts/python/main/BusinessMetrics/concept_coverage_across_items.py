

import requests
import pandas as pd
import json
import time
import warnings
import logging
import ast
import ConfigParser
import os
import sys
environment = sys.argv[1]

# getting paths from config file
config = ConfigParser.SafeConfigParser()
config.read('config.properties')
# environment = config.get('Environment', 'environ')
environment_path = config.get('Environment_Path', environment)

warnings.filterwarnings("ignore")
logging.basicConfig(filename='concept_coverage_across_items.log',
                    format='%(levelname)s:%(message)s', level=logging.INFO)


# to check if num is NaN
def isNaN(num):
    # print(num)
    return num != num


def get_microConcepts(x):
    # print type(x)
    if x:
        if type(x) is unicode:
            x = ast.literal_eval(x)
            if x:
                x = x[0]
                mc = x["identifier"]
            else:
                mc = 'NaN'
        else:
            x = x[0]
            mc = x["identifier"]
    else:
        mc = 'NaN'
    return mc


def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found, return the string unchanged.
    """
    if (s[0] == s[-1]) and s.startswith(('"', "'")):
        return s[1:-1]
    return s


def get_domain(x, concept_df):
    try:
        index = concept_df.loc[concept_df['identifier'] == x].index.tolist()[0]
        # print index
        domain = concept_df['subject'].iloc[index]
        # print domain
    except:
        domain = ''
    return domain


def get_desc(x, concept_df):
    try:
        index = concept_df.loc[concept_df['identifier'] == x].index.tolist()[0]
        # print index
        desc = concept_df['description'].iloc[index]
        # print desc
    except:
        desc = ''
    return desc


def get_itemIDs(x, df_item):
    lst_item = df_item.loc[df_item['micro_concept'] == x].identifier.tolist()
    return ", ".join(lst_item)


def get_item_df():
    url = environment_path+"/learning/v1/assessmentitem/search"

    payload = "{\n  \"request\": {\n    \"startPosition\": 0,\n    \"resultSize\": 11270\n  }\n}"
    headers = {
        'content-type': "application/json",
        'user-id': "ilimi",
        'cache-control': "no-cache",
        'postman-token': "01d02b22-9e73-5968-3daf-63a177e1e20d"
        }

    response = requests.request("POST", url, data=payload, headers=headers)
    resp = response.json()
    try:
        itemList = resp["result"]["assessment_items"]
        df_item = pd.DataFrame(itemList)
        return df_item
    except:
        return resp['responseCode']


def add_concept_info(df_item):

    df_item['micro_concept'] = df_item['concepts'].apply(lambda x: x if isNaN(x) else get_microConcepts(x))

    array_mc = df_item.micro_concept.unique()
    df_mc = pd.DataFrame({'micro_concept': array_mc,
                          'number': 0})

    for i in range((df_item.shape)[0]):
        for j in range((df_mc.shape)[0]):
            if df_mc['micro_concept'].iloc[j] == df_item['micro_concept'].iloc[i]:
                df_mc.loc[j, 'number'] += 1

    df_mc_sorted = df_mc.sort_values(['number'], ascending=[False])
    df_mc_sorted = df_mc_sorted.reset_index()
    df_mc_sorted = df_mc_sorted[['micro_concept', 'number']]
    return df_mc_sorted


def get_concept_df():
    urlNumeracy = environment_path+"/learning/v2/domains/numeracy/concepts"
    urlLiteracy = environment_path+"/learning/v2/domains/literacy/concepts"
    respNum = requests.get(urlNumeracy).json()
    respLit = requests.get(urlLiteracy).json()

    conceptsList = respNum["result"]["concepts"]
    concept_Num_DF = pd.DataFrame(conceptsList)
    conceptsList = respLit["result"]["concepts"]
    concept_Lit_DF = pd.DataFrame(conceptsList)

    concept_df = pd.concat([concept_Num_DF, concept_Lit_DF])
    concept_df = concept_df.reset_index()
    concept_df.is_copy = False
    return concept_df


def add_concept_details(df_mc_sorted, concept_df, df_item):

    df_mc_sorted['Domain'] = ''
    df_mc_sorted['description'] = ''
    df_mc_sorted['item_ids'] = ''

    df_mc_sorted['Domain'] = df_mc_sorted['micro_concept'].apply(lambda x: get_domain(x, concept_df))
    df_mc_sorted['description'] = df_mc_sorted['micro_concept'].apply(lambda x: get_desc(x, concept_df))
    df_mc_sorted['item_ids'] = df_mc_sorted['micro_concept'].apply(lambda x: get_itemIDs(x, df_item))

    return df_mc_sorted


# save dataframe in text file
def save_dataframe(df, filename):
    list_records = json.loads(df.to_json(orient='records'))
    if not os.path.exists('metrics'):
        os.makedirs('metrics')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'metrics', filename)
    outfile = open(file_path, "w")
    print >> outfile, "\n".join(str(i) for i in list_records)
    outfile.close()


def main():
    df_item = get_item_df()
    if type(df_item) == str or type(df_item) == unicode:
        logging.error(df_item)
        return
    logging.info('df item retrieved')
    df_mc_sorted = add_concept_info(df_item)
    logging.info('concepts added')
    df_mc_sorted.columns = ['concept_id', 'items']
    # concept_df = get_concept_df()
    # logging.info('concept model retrieved')
    # df_mc_sorted = add_concept_details(df_mc_sorted, concept_df, df_item)
    # logging.info('concept details added')
    filename = time.strftime("%Y-%m-%d") + '_concept_coverage_across_items.json'
    save_dataframe(df_mc_sorted, filename)
    logging.info('saved to json')

main()


