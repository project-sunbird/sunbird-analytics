

import requests
import pandas as pd
import json
import time
import warnings
import logging
import ConfigParser
import os
import sys
import ast
environment = sys.argv[1]
root = os.path.dirname(os.path.abspath(__file__))
# changing working directory
os.chdir(root)

# getting paths from config file
config = ConfigParser.SafeConfigParser()
config.read('config.properties')
environment_path = config.get('Environment_Path', environment)
log_folder = config.get('logfile', 'log_dir')
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
log_path = os.path.join(log_folder, 'concept_coverage_across_items.log')
warnings.filterwarnings("ignore")
logging.basicConfig(filename=log_path,
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
    # url = environment_path+"/learning/v1/assessmentitem/search"

    # payload = "{\n  \"request\": {\n    \"startPosition\": 0,\n    \"resultSize\": 11270\n  }\n}"
    # headers = {
    #     'content-type': "application/json",
    #     'user-id': "ilimi",
    #     'cache-control': "no-cache",
    #     'postman-token': "01d02b22-9e73-5968-3daf-63a177e1e20d"
    #     }
    #
    # response = requests.request("POST", url, data=payload, headers=headers)
    # resp = response.json()
    # try:
    #     itemList = resp["result"]["assessment_items"]
    #     df_item = pd.DataFrame(itemList)
    #     return df_item
    # except:
    #     return resp['responseCode']
    url = environment_path+"/search/v2/search"

    payload = "{\r\n    \"request\": {\r\n        \"filters\":{\r\n            \"objectType\": [\"AssessmentItem\"],\r\n            \"status\": []\r\n        },\r\n        \"limit\": 10000\r\n    }\r\n}"
    headers = {
        'content-type': "application/json",
        'user-id': "ilimi",
        'accept-encoding': "UTF-8",
        'accept-charset': "UTF-8",
        'cache-control': "no-cache",
        'postman-token': "8f3ceabe-f633-8cbb-020c-13dcd3b52b6d"
    }
    try:
        response = requests.request("POST", url, data=payload, headers=headers).json()
        itemlist = response["result"]["items"]
        df_item = pd.DataFrame(itemlist)
        return df_item
    except:
        return 'unable to retrieve items'


def add_concept_info(df_item):

    # df_item['micro_concept'] = df_item['concepts'].apply(lambda x: x if isNaN(x) else get_microConcepts(x))
    # print df_item['micro_concept'].apply(lambda x: 0 if isNaN(x) else len(x))
    flatten = lambda l: [item for sublist in l for item in sublist]
    # array_mc = df_item.micro_concept.unique()
    cleanedList = [x for x in list(df_item.concepts) if not isNaN(x)]
    array_mc = list(set(flatten(cleanedList)))
    df_mc = pd.DataFrame({'micro_concept': array_mc,
                          'number': 0})

    for i in range((df_item.shape)[0]):
        for j in range((df_mc.shape)[0]):
            if not isNaN(df_item['concepts'].iloc[i]):
                if df_mc['micro_concept'].iloc[j] in df_item['concepts'].iloc[i]:
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

def get_concept_df_new():
    url = environment_path+"/search/v2/search"

    payload = "{\r\n    \"request\": {\r\n        \"filters\":{\r\n            \"objectType\": [\"Concept\"],\r\n            \"status\": [\"Live\",\"Retired\",\"Draft\"]\r\n        },\r\n        \"limit\": 1000\r\n    }\r\n}"
    headers = {
        'content-type': "application/json",
        'user-id': "ilimi",
        'accept-encoding': "UTF-8",
        'accept-charset': "UTF-8",
        'cache-control': "no-cache",
        'postman-token': "3e89d6cb-f2fe-9319-4bec-242c49443a7e"
    }

    response = requests.request("POST", url, data=payload, headers=headers).json()
    conceptlist = response["result"]["concepts"]
    conceptDF = pd.DataFrame(conceptlist)
    return conceptDF


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
    # list_records = json_loads_byteified(df.to_json(orient='records'))
    list_records = df.to_json(orient='records')[1:-1].replace('},{', '}\n{')
    if not os.path.exists('metrics'):
        os.makedirs('metrics')
    file_path = os.path.join(root, 'metrics', filename)
    # outfile = open(file_path, "w")
    # print >> outfile, "\n".join(str(i) for i in list_records)
    # outfile.close()
    with open(file_path, 'w') as f:
        f.write(list_records)


def main():
    df_item = get_item_df()
    if type(df_item) == str or type(df_item) == unicode:
        logging.error(df_item)
        exit()
    logging.info('df item retrieved')
    try:
        df_mc_sorted = add_concept_info(df_item)
    except:
        logging.error('unable to add concept info')
        exit()
    logging.info('concepts added')
    df_mc_sorted.columns = ['concept_id', 'items']
    # # concept_df = get_concept_df()
    # # logging.info('concept model retrieved')
    # # df_mc_sorted = add_concept_details(df_mc_sorted, concept_df, df_item)
    # # logging.info('concept details added')
    filename = time.strftime("%Y-%m-%d") + '_concept_coverage_across_items.json'
    save_dataframe(df_mc_sorted, filename)
    logging.info('saved to json')

main()


