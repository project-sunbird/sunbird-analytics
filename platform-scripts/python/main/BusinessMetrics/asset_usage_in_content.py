import requests
import pandas as pd
import numpy as np
import json
import time
import warnings
import logging
import re
import ConfigParser
import os
import sys
import ast
from json_load import *
environment = sys.argv[1]

# getiing paths from config file
config = ConfigParser.SafeConfigParser()
config.read('config.properties')
# environment = config.get('Environment', 'environ')
environment_path = config.get('Environment_Path', environment)

warnings.filterwarnings("ignore")
logging.basicConfig(filename='asset_usage_in_content.log',
                    format='%(levelname)s:%(message)s', level=logging.INFO)


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


def get_content_dict(content_df):
    dict_content_body = {}
    # to get the body of all the contents
    for i in range(content_df.shape[0]):
        contentID = content_df.identifier.iloc[i]
    #     print contentID
        url = environment_path+"/learning/v2/content/" + contentID
    #     print url
        querystring = {"fields":"body"}
        payload = "-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\n\r\n-----011000010111000001101001--"
        headers = {
            'content-type': "multipart/form-data; boundary=---011000010111000001101001",
            'user-id': "rayuluv",
            'cache-control': "no-cache",
            'postman-token': "0892cf6d-004a-3c20-1f18-194340f5dc7f"
            }

        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
        resp = response.json()
        try:
            body = resp["result"]["content"]["body"]
            dict_content_body[contentID] = body
        except:
            continue
    return dict_content_body


def count_assets(contentDF, dict_content_body):
    # creating assets dataframe
    data = np.zeros(((contentDF.shape)[0], 4), dtype=np.int16)
    df_assets = pd.DataFrame(data)
    df_assets.columns = ['contentID', 'image', 'audio', 'animation']
    # counting no of audio and images in each content
    i = 0
    for key, value in dict_content_body.iteritems():
        df_assets.contentID.iloc[i] = key
        image = value.count('.jpg')
        image += value.count('.png')
        image += value.count('.jpeg')
        animation = value.count('.gif')
        audio = value.count('.mp3')
        # df_assets.image.iloc[i] = image
        df_assets[i, 'image'] = image
        # df_assets.audio.iloc[i] = audio
        df_assets[i, 'audio'] = audio
        # df_assets.animation.iloc[i] = animation
        df_assets[i, 'animation'] = animation
        i += 1
    return df_assets


def sort_df_assets(df_assets, contentDF):
    # sorting wrt no of images
    df_assets_sorted = df_assets.sort_values(['image', 'audio'], ascending=[False, False])
    df_assets_sorted = df_assets_sorted.reset_index()
    df_assets_sorted = df_assets_sorted[['contentID', 'image', 'audio', 'animation']]

    # removing rows with contentID == 0
    df_assets_sorted = df_assets_sorted[df_assets_sorted.contentID != 0]
    df_assets_sorted = df_assets_sorted.reset_index()

    # adding title column
    df_assets_sorted['title'] = ''
    df_assets_sorted['description'] = ''

    for i in range(df_assets_sorted.shape[0]):
        cID = df_assets_sorted.contentID.iloc[i]
        df_assets_sorted.title.iloc[i] = contentDF.loc[contentDF['identifier'] == cID, 'name'].iloc[0]
        df_assets_sorted.description.iloc[i] = contentDF.loc[contentDF['identifier'] == cID, 'description'].iloc[0]
    return df_assets_sorted


def get_fileName(dict_content_body, str_match):
    lst_name = []
    for key, value in dict_content_body.iteritems():
        lst_loc = [m.start() for m in re.finditer(str_match, value)]
        for loc in lst_loc:
            i = loc
            name = ''
            while i > 0:
                if value[i] == '/':
                    name = value[i+1:loc]+str_match
                    break;
                elif value[i] == ':':
                    break;
                i -= 1
            if name:
                lst_name.append(name)
    return lst_name


def uniqfy_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def get_type(x):
    if isinstance(x, int ):
        return 'NA'
    else:
        if '.jpg' in x or '.png' in x:
            return 'image'
        else:
            return 'audio'


def get_assets_details(dict_content_body):
    lst_jpgName = uniqfy_list(get_fileName(dict_content_body, '.jpg'))
    lst_pngName = uniqfy_list(get_fileName(dict_content_body, '.png'))
    lst_mp3Name = uniqfy_list(get_fileName(dict_content_body, '.mp3'))

    lst_all = lst_jpgName+lst_pngName+lst_mp3Name
    data = np.zeros((len(lst_all), 3), dtype=np.int16)
    df_assets_2 = pd.DataFrame(data)
    df_assets_2.columns = ['asset', 'number', 'type']
    # counting no of times assets has been used
    j = 0
    for asset in lst_all:
        i = 0
        df_assets_2.asset.iloc[j] = asset
        for key, value in dict_content_body.iteritems():
            if asset in value:
                df_assets_2.number.iloc[j] += 1
            i += 1
        j += 1

    df_assets_2['type'] = df_assets_2['asset'].apply(lambda x: get_type(x))
    df_assets_2_sorted = df_assets_2.sort_values(['number'], ascending=[False])
    df_assets_2_sorted = df_assets_2_sorted.reset_index()
    # df_assets_2_sorted = df_assets_2_sorted[['asset','number','type']]
    df_assets_2_sorted = df_assets_2_sorted[['asset', 'number']]
    df_assets_2_sorted = df_assets_2_sorted[df_assets_2_sorted.asset != 0]
    return df_assets_2_sorted


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
    content_df = get_content_df()
    dict_content_body = get_content_dict(content_df)
    # df_assets = count_assets(content_df, dict_content_body)
    # df_assets_sorted = sort_df_assets(df_assets, content_df)
    # df_assets_sorted.to_csv('df_assets.csv')
    df_assets_2_sorted = get_assets_details(dict_content_body)
    df_assets_2_sorted.columns = ['asset_id', 'contents']
    filename = time.strftime("%Y-%m-%d") + '_asset_usage_in_content.json'
    save_dataframe(df_assets_2_sorted, filename)
    logging.info('saved to json')
    print "Generated metrics of asset usage in contents..."

main()

