# Author: Aditya Arora, adityaarora@ekstepplus.org

import os
import argparse #Accept commandline arguments
import logging #Log the data given
import sys
import requests
#Pass as a commandline argument later on
root=os.path.dirname(os.path.abspath(__file__))
utils=os.path.join(os.path.split(os.path.split(root)[0])[0],'Utils')
import sys
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in 
root=os.path.dirname(os.path.abspath(__file__))
tap=['download','media','json','concepts','end']

#########Setting Up The Area
#Define commandline arguments
parser = argparse.ArgumentParser()
parser.add_argument('baseurl',type=unicode,help='This the base url for retrieving metadata about a content')
parser.add_argument('identifier',help='This is the identifier of the piece of content',default=os.path.join(root,'Data'))
parser.add_argument('--ld',help='This is the operating directory',default=os.path.join(root,'Data'))
parser.add_argument('--ts',help='This is the stage of tapping. Choices:%s'%(','.join(tap)),default='end',choices=tap)

#Read arguments given
args = parser.parse_args()
op_dir=args.ld
identifier=args.identifier
url=args.baseurl+'/'+identifier
tap_stage=args.ts
#Define the path of the folder containg all data entries
path=os.path.join(op_dir,identifier)
#Set up logging
logging.basicConfig(filename=os.path.join(op_dir,'%s.log'%(args.identifier)),level=logging.DEBUG)
logging.info('Name:%s'%(identifier))

#########Download Content
#Download metadata
r={}
try:
	r=requests.get(url).json()
except:
	logging.info('Exception:Unable to access metadata')
	sys.exit()
#Get metadata as dictionary
obj={}
try:
	obj=r['result']['content']
except:
	log.info('Exception:Something is seriously wrong with the structure of response from metadata')
	sys.exit()
#Get download URL
if 'downloadUrl' in obj:
	downUrl=obj['downloadUrl']
	logging.info('downloadUrl%s'%(downUrl))
else:
	logging.info('Exception:No downloadUrl')
	sys.exit()
#To download zipfiles
from downloadZipFile import downloadZipFile
if(downloadZipFile(downUrl,os.path.join(op_dir,'temp'+identifier))):
	logging.info('Content Downloaded')
else:
	logging.info('Exception:Unable to download content')
	sys.exit()
#Import and use downloadContent
from downloadContent import *
unzip_files(os.path.join(op_dir,'temp'+identifier))
logging.info('Files unzipped')
copy_main_folders(op_dir,identifier)
logging.info('Folders shifted')
add_manifest(obj,os.path.join(op_dir,identifier))
logging.info('Manifest added')
logging.info('Pre-Processing Complete')
if(tap_stage=='download'):
	sys.exit()	

#########Handle Media
media_type=['mp3','ogg','png','gif','jpg']
from handleMedia import *
count=count_file_type_directory(path,media_type)
logging.info('Counted all media file types')
mp3Files=findFiles(path,['mp3'])
(mp3Length,bad_mp3)=count_MP3_length_directory(mp3Files)
logging.info('Counted mp3 length of all files except for these: %s'%(','.join(bad_mp3)))
mp3Files=findFiles(path,['mp3'])
(transcribed,bad_mp3)=speech_recogniser(mp3Files)
logging.info('Transcribed all mp3 files except for these: %s'%(','.join(bad_mp3)))
images=imageNames(path)
logging.info('Added image filenames')
mediaStats={'mediaCount':count,'mp3Length':mp3Length,'mp3Transcription':transcribed,'imageTags':images}
with codecs.open(os.path.join(path,'mediaStats.json'),'w',encoding='utf-8') as f:
	json.dump(mediaStats, f, sort_keys=True, indent=4)
f.close()
logging.info('Assets handled')
if(tap_stage=='media'):
	sys.exit()

#########Parse Json
from parseJson import *
#Parse all jsons in a particular subdirectory of an identifier ['assets','data','items'] and save as ___.json Eg-assets.json
for subdir in ['data','items']:
	json_files=findFiles(os.path.join(path,subdir),['.json'])
	with codecs.open(os.path.join(path,'%s.json'%(subdir)),'w',encoding='utf-8') as f:
		json.dump(extract_json(json_files), f, sort_keys=True, indent=4)
	f.close()
	logging.info('%s JSON files handled'%(subdir))
if(tap_stage=='json'):
	sys.exit()

#########Get Concepts
#Download concept list if necessary
if (not os.path.isfile(os.path.join(root,'conceptList.txt'))):
	from getConcepts import *
	getConcepts()
#Load concept list
with codecs.open(os.path.join(root,'conceptList.txt'),'r',encoding='utf-8') as f:
	conceptList=f.readlines()
conceptList=conceptList[0].split(',')
#Filter to get concepts
from conceptFilter import *
filter_assessment_data(path,conceptList)
if(tap_stage=='concepts'):
	sys.exit()

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
#Write enriched JSON to root
with codecs.open(os.path.join(op_dir,'%s.json'%(identifier)),'w',encoding='utf-8') as f:
	json.dump(content, f, sort_keys=True, indent=4)
f.close()
logging.info('Enriched JSON Saved')
shutil.rmtree(path)
