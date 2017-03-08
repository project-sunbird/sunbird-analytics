"""this script calculates the usage of template in content.
"""

__version__ = '1.0'
__author__ = 'Ajit Barik'

import requests
import json
import logging
import pandas as pd
import warnings
import time
import ConfigParser
import os
import sys
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
log_path = os.path.join(log_folder, 'template_usage_by_content.log')
warnings.filterwarnings("ignore")
logging.basicConfig(filename=log_path,
                    format='%(levelname)s:%(message)s', level=logging.INFO)

# to get unique list
def uniqfy_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


# to get template id
def get_template_id(url):
    querystring = {"fields": "template"}
    payload = ""
    headers = {
        'content-type': "application/json",
        'user-id': "ilimi",
        'accept-encoding': "UTF-8",
        'accept-charset': "UTF-8",
        'cache-control': "no-cache",
        'postman-token': "71cb7af5-8a80-cbd8-7088-a30207752d87"
        }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring).json()
    try:
        template_id = response['result']['content']['template']
    except:
        template_id = ''
    return template_id


# to get content df
def get_content_df():
    # get all the content's
    url = environment_path+"/learning/v2/content/list"
    payload = "{\n\"request\": { \n\"search\": " \
              "{\n\"status\": [\"Live\"],\n\"contentType\": [\"Collection\", \"Worksheet\", \"Story\", \"Game\"]," \
              "\n\"limit\": 2000\n}\n  }\n}"
    headers = {
       'content-type': "application/json",
       'user-id': "mahesh",
       'cache-control': "no-cache",
       'postman-token': "d0fafff9-911a-9a91-2016-cfc4714cf543"
       }
    resp = requests.request("POST", url, data=payload, headers=headers).json()

    content_list = resp["result"]["content"]
    content_df = pd.DataFrame(content_list)

    content_df['template_id'] = ''
    return content_df


# to get template info
def get_template_info(content_df):
    url = environment_path+"/learning/v2/content/search"
    payload = "{\n  \"request\": { \n\"search\": {\n\"contentType\": [\"Template\"]," \
              "\n\"fields\": [\"name\", \"downloadUrl\", \"code\", " \
              "\"mediaType\", \"status\",\"templateType\"]\n}\n  }\n}"
    headers = {
        'content-type': "application/json",
        'user-id': "mahesh",
        'cache-control': "no-cache",
        'postman-token': "8bf218f2-5965-4b8c-aa14-7e5c457ffe93"
        }
    template_response = requests.request("POST", url, data=payload, headers=headers).json()
    template_dict_list = template_response['result']['content']

    template_dict = {}
    for templateDict in template_dict_list:
        template_id = templateDict['identifier']
        template_name = templateDict['name']
        template_dict[template_id] = template_name

    for i in range(len(content_df.identifier)):
        identifier = content_df.identifier.iloc[i]
        base_url = "https://api.ekstep.in/learning/v2/content/"
        url = base_url + identifier
        template = get_template_id(url)
        content_df.loc[i, 'template_id'] = template
        # content_df.template_id.iloc[i] = template

    return [content_df, template_dict]


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
    try:
        content_df = get_content_df()
        logging.info('content model retrieved')
        content_df, template_dict = get_template_info(content_df)
        logging.info('template info retrieved')
    except:
        logging.error('unable to retrieve content and template model')
        exit()

    lst_template = uniqfy_list(list(content_df.template_id))  # list of templates
    df_template = pd.DataFrame(
        {'Template_id': lst_template, 'Template_Name': '', 'Number_of_content': '', 'content_list': ''})
    logging.info('empty df created')

    for i in range(len(df_template.index)):
        template = df_template.Template_id.iloc[i]
        no = content_df[content_df.template_id == template].shape[0]
        df_template.Number_of_content.iloc[i] = no
        content_list = list(content_df.identifier[content_df.template_id == template])
        df_template.content_list.iloc[i] = str(','.join(content_list))
        try:
            df_template.Template_Name.iloc[i] = template_dict[template]
        except:
            df_template.Template_Name.iloc[i] = 'Not Available'
    logging.info('df_template filled')
    df_template = df_template[['Template_id', 'Template_Name', 'Number_of_content', 'content_list']]
    df_template_sorted = df_template.sort_values(['Number_of_content'], ascending=[False])
    df_template_sorted = df_template_sorted.reset_index()
    # df_template_sorted = df_template_sorted[['Template_id', 'Template_Name', 'Number_of_content', 'content_list']]
    df_template_sorted = df_template_sorted[['Template_id', 'Number_of_content']]
    df_template_sorted.columns = ['template_id', 'contents']
    # df_template_sorted.to_csv('template_usage_by_content.csv')
    filename = time.strftime("%Y-%m-%d") + '_template_usage_by_content.json'
    save_dataframe(df_template_sorted, filename)

main()
