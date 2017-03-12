"""this script calculates the usage of template in items.
"""

__version__ = '1.0'
__author__ = 'Ajit Barik'
import requests
import pandas as pd
import warnings
import json
import logging
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
log_path = os.path.join(log_folder, 'template_usage_by_items.log')

warnings.filterwarnings("ignore")
logging.basicConfig(filename=log_path,
                    format='%(levelname)s:%(message)s', level=logging.INFO)


url = environment_path+"/learning/v2/content/search"
payload = "{\n\"request\": { \n\"search\": " \
          "{\n\"contentType\": [\"Template\"],\n\"fields\":" \
          " [\"name\", \"downloadUrl\", \"code\", \"mediaType\", \"status\",\"templateType\"]\n}\n  }\n}"
headers = {
    'content-type': "application/json",
    'user-id': "mahesh",
    'cache-control': "no-cache",
    'postman-token': "8bf218f2-5965-4b8c-aa14-7e5c457ffe93"
    }
try:
    response = requests.request("POST", url, data=payload, headers=headers).json()
    n = len(response['result']['content'])
    template_id = response['result']['content'][0]['identifier']
except:
    logging.error('no response from api call')
    exit()

url = environment_path+"/search/v2/search"

payload = "{\r\n    \"request\": {\r\n        \"filters\":{\r\n            \"objectType\": [\"AssessmentItem\"],\r\n            \"template_id\": [\"domain_7355\"],\r\n            \"status\": []\r\n        },\r\n        \"limit\": 1\r\n    }\r\n}"
headers = {
    'content-type': "application/json",
    'user-id': "ilimi",
    'accept-encoding': "UTF-8",
    'accept-charset': "UTF-8",
    'cache-control': "no-cache",
    'postman-token': "5b234f26-ea8b-2372-7c50-30a42d6cfa78"
    }

payload_suffix = "\"],\r\n            \"status\": []\r\n        },\r\n        \"limit\": 1\r\n    }\r\n}"
payload_prefix = "{\"request\": {\r\n        \"filters\":{\r\n            \"objectType\": [\"AssessmentItem\"],\r\n            \"template_id\": [\""

dictList = response['result']['content']
n=len(dictList)
x=[]

lst_template_id = []
lst_template_name = []
lst_item = []
lst_itemName = []
for templateDict in dictList:
    template_id = templateDict['identifier']
    template_name = templateDict['name']
    payload = payload_prefix + template_id + payload_suffix
    # itemDict = responseItem['result']['assessment_items']
    try:
        responseItem = requests.request("POST", url, data=payload, headers=headers).json()
        count = responseItem['result']['count']
    except:
        count = 0
        #     n_item = len(itemDict)
    n_item = count
    # n_item = len(itemDict)
    lst_template_id.append(template_id)
    lst_template_name.append(template_name)
    lst_item.append(n_item)
    lst_temp = ""
    i = 0
    # for item in itemDict:
    #     item_id = item['identifier']
    #     if i == 0:
    #         lst_temp = item_id
    #     else:
    #         lst_temp = lst_temp + " , " + item_id
    #     i += 1
    # lst_itemName.append(lst_temp)

logging.info('getting item info')
url = environment_path+"/search/v2/search"

payload = "{\r\n    \"request\": {\r\n        \"filters\":{\r\n            \"objectType\": [\"AssessmentItem\"]," \
          "\r\n            \"status\": []\r\n        },\r\n        \"facets\": [\"template_id\"]," \
          "\r\n        \"limit\":0\r\n    }\r\n}"
headers = {
    'content-type': "application/json",
    'user-id': "ilimi",
    'accept-encoding': "UTF-8",
    'accept-charset': "UTF-8",
    'cache-control': "no-cache",
    'postman-token': "83285590-f013-3bec-6ca6-ecb18ef046f7"
    }
try:
    response = requests.request("POST", url, data=payload, headers=headers).json()
    template_dict = response['result']['facets'][0]['values']
except:
    logging.error('no response from api call')
    exit()

template_dict_final = {}
for i in range(len(template_dict)):
    name = template_dict[i]['name']
    count = template_dict[i]['count']
    template_dict_final[name] = count

df_template = pd.DataFrame({'template_id': lst_template_id, 'Template_Name': lst_template_name, 'items': lst_item})
df_template = df_template[['template_id', 'Template_Name', 'items']]

for key, value in template_dict_final.iteritems():
    failed = []
    try:
        df_template['items'][df_template['template_id'] == key] = value
    except:
        failed.append(1)

df_template_sorted = df_template.sort_values(['items'], ascending=[False])
df_template_sorted = df_template_sorted.reset_index()
df_template_sorted = df_template_sorted[['template_id', 'Template_Name', 'items']]

df_detail = pd.DataFrame({'template_id': lst_template_id, 'Template_Name': lst_template_name,
                          'items': lst_item})
df_detail = df_detail[['template_id', 'Template_Name', 'items']]
df_detail_sorted = df_detail.sort_values(['items'], ascending=[False])
df_detail_sorted = df_detail_sorted.reset_index()
# df_detail_sorted = df_detail_sorted[['Template_id', 'Template_Name', 'Item ids', 'Number of Items']]
df_detail_sorted = df_detail_sorted[['template_id', 'items']]
file_name = time.strftime("%Y-%m-%d") + '_template_usage_by_items.json'


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

save_dataframe(df_detail_sorted, file_name)
logging.info('saved to json')
