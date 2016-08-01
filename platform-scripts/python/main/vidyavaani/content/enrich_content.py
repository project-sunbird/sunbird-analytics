# Author: Aditya Arora, adityaarora@ekstepplus.org

import os
import argparse #Accept commandline arguments
import logging #Log the data given
import sys
import requests
import ConfigParser
import json

#Pass as a commandline argument later on
root=os.path.dirname(os.path.abspath(__file__))
utils=os.path.join((os.path.split(root)[0]),'utils')
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in 
root=os.path.dirname(os.path.abspath(__file__))

#getiing paths from config file
config = ConfigParser.RawConfigParser()
config.read(config_file)
op_dir = config.get('FilePath', 'temp_path')
log_dir = config.get('FilePath','log_path')

#check if paths exists
if not os.path.exists(op_dir):
	os.makedirs(op_dir)

if not os.path.exists(log_dir):
	os.makedirs(log_dir)

std_input = sys.stdin.readline()
std_input = json.loads(std_input)
#define the identifier
url = std_input['content_url']
base_url = std_input['base_url']
identifier = os.path.split(url)[-1]
#Define the path of the folder containg all data entries
path=os.path.join(op_dir,identifier)
#Set up logging
logfile_name = os.path.join(log_dir,'%s.log'%(identifier))
logging.basicConfig(filename=logfile_name,level=logging.DEBUG)
logging.info('Name:%s'%(identifier))

#########Download Content
#Download metadata
r={}
try:
	r=requests.get(url)
	r=json.loads(r.text)
except:
	logging.info('Exception:Unable to access metadata')
	sys.exit()
#Get metadata as dictionary
obj={}
try:
	obj=r['result']['content']
except:
	logging.info('Exception:Something is seriously wrong with the structure of response from metadata')
	sys.exit()
#Get download URL
if 'downloadUrl' in obj:
	downUrl=obj['downloadUrl']
	logging.info('downloadUrl%s'%(downUrl))
else:
	logging.info('Exception:No downloadUrl')
	sys.exit()
#To download zipfiles
from download_zip_file import downloadZipFile
if(downloadZipFile(downUrl,os.path.join(op_dir,'temp'+identifier))):
	logging.info('Content Downloaded')
else:
	logging.info('Exception:Unable to download content')
	sys.exit()
#Import and use downloadContent
from download_content import *
unzip_files(os.path.join(op_dir,'temp'+identifier))
logging.info('Files unzipped')
copy_main_folders(op_dir,identifier)
logging.info('Folders shifted')
add_manifest(obj,os.path.join(op_dir,identifier))
logging.info('Manifest added')
logging.info('Pre-Processing Complete')

#########Handle Media
media_type=['mp3','ogg','png','gif','jpg']
from handle_media import *
count=count_file_type_directory(path,media_type)
logging.info('Counted all media file types')
mp3Files=findFiles(path,['mp3'])
(mp3Length,bad_mp3)=count_MP3_length_directory(mp3Files)
logging.info('Counted mp3 length of all files except for these: %s'%(','.join(bad_mp3)))
mp3Files=findFiles(path,['mp3'])
(transcribed,bad_mp3)=speech_recogniser(mp3Files)
logging.info('Transcribed all mp3 files except for these: %s'%(','.join(bad_mp3)))
transcribed = add_confidence(transcribed)
logging.info('Default confidence score added for empty confidence score')
images=imageNames(path)
logging.info('Added image filenames')
mediaStats={'mediaCount':count,'mp3Length':mp3Length,'mp3Transcription':transcribed,'imageTags':images}
with codecs.open(os.path.join(path,'mediaStats.json'),'w',encoding='utf-8') as f:
	json.dump(mediaStats, f, sort_keys=True, indent=4)
f.close()
logging.info('Assets handled')

#########Parse Json
from parse_json import *
#Parse all jsons in a particular subdirectory of an identifier ['assets','data','items'] and save as ___.json Eg-assets.json
for subdir in ['data','items']:
	json_files=findFiles(os.path.join(path,subdir),['.json'])
	with codecs.open(os.path.join(path,'%s.json'%(subdir)),'w',encoding='utf-8') as f:
		json.dump(extract_json(json_files), f, sort_keys=True, indent=4)
	f.close()
	logging.info('%s JSON files handled'%(subdir))
#########Get Concepts
#Download concept list if necessary
if (not os.path.isfile(os.path.join(op_dir,'conceptList.txt'))):
	from get_concepts import *
	getConcepts(base_url)
#Load concept list
with codecs.open(os.path.join(op_dir,'conceptList.txt'),'r',encoding='utf-8') as f:
	conceptList=f.readlines()
conceptList=conceptList[0].split(',')
#Filter to get concepts
from concept_filter import *
filter_assessment_data(path,conceptList)

#########Relevant Content Keys (relevant pieces of content metadata)
keys=['ageGroup','contentType','gradeLevel','languageCode','language','developer','publisher','author',
	  'illustrators','genre','subject','Subject','domain','popularity','filter','owner','concepts',
	  'objectsUsed','keywords','tags','description','text']

#########Convert to EnrichedJSON
content={}
content['identifier']=identifier
logging.info('Converting to enriched JSON: %s'%(op_dir))
#Read key value pairs from manifest.json
with codecs.open(os.path.join(path,'manifest.json'),'r',encoding='utf-8') as f:
	dct=json.load(f)
f.close()
for key in keys:
	if key in dct:
		content[key]=dct[key]
#Read media statistics from mediaStats.json
with codecs.open(os.path.join(path,'mediaStats.json'),'r',encoding='utf-8') as f:
	dct=json.load(f)
f.close()
for key in dct.keys():
	content[key]=dct[key]
logging.info('Media stats added')
#Read json values from assets,data and items
for i in ['data','items']:
	with codecs.open(os.path.join(path,'%s.json'%(i)),'r',encoding='utf-8') as f:
		content[i]=f.readlines()
	f.close()
logging.info('Data and items values added')
with codecs.open(os.path.join(path,'concepts.txt'),'r',encoding='utf-8') as f:
	content['concepts']=f.readlines()
f.close()
logging.info('Concepts Read')
print(json.dumps(content))
shutil.rmtree(path)
