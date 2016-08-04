# Author(s): Aditya Arora, adityaarora@ekstepplus.org
# 		   : Soma Dhavala soma@ilimi.in
# 		   : Ajit Barik ajitbk@ilimi.in


import os
import argparse #Accept commandline arguments
import logging #Log the data given
import sys
import requests
import ConfigParser
import json

# new additions
import atexit
import validators
import codecs
import traceback

#Pass as a commandline argument later on
root=os.path.dirname(os.path.abspath(__file__))
utils=os.path.join((os.path.split(root)[0]),'utils')
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in 
root=os.path.dirname(os.path.abspath(__file__))

from download_zip_file import downloadZipFile
from download_content import *
from get_lowest_key_value import flattenDict
from handle_media import *
from parse_json import *
from get_concepts import *
from concept_filter import *

# getiing paths from config file
config = ConfigParser.RawConfigParser()
config.read(config_file)

op_dir = config.get('FilePath', 'temp_path')
log_dir = config.get('FilePath','log_path')
logfile_name = os.path.join(log_dir,'enrich_content.log')
logging.basicConfig(filename=logfile_name,level=logging.INFO)
logging.info('### Enriching content ###')
# get a relative path 
#op_dir = os.path.join(root,op_dir)
#log_dir = os.path.join(root,log_dir)

#print 'op_dir' + op_dir
#print 'log_dir' + log_dir

# must've-fields as part of the content meta-data. empty values are fine
mustHavekeysFromContentModel=['identifier','concepts','tags','description','text']
goodToHaveKeysFromContentModel=['ageGroup','contentType','gradeLevel','languageCode','language','developer','publisher','author',
	  'illustrators','genre','subject','Subject','domain','popularity','filter','owner',
	  'objectsUsed','keywords','status']
enrichedKeysFromML = ['mediaCount','mp3Length','mp3Transcription','imageTags']

# define output file to write to (stdout)
out = sys.stdout
#msg = 'test write to out'
#out.write(msg)

## this is specific to this module (not a part of content model)
# content['exitStatus']=-1

# #logging.info('Converting to enriched JSON: %s'%(op_dir))
# # this function does write the json to stdout upon exit
# def writePayLoadToStreamOnExit():
#     print "Exiting the Content Enriching Module"
#     print(json.dumps(content))
# atexit.register(writePayLoadToStreamOnExit())

#check if paths exists
try: 
	if not os.path.exists(op_dir):
		os.makedirs(op_dir)

	if not os.path.exists(log_dir):
		os.makedirs(log_dir)
except:
	traceback.print_exc()
	msg = 'Not able to find/create log and/or tmp dir'
	logging.info(msg)
	sys.exit(1)

def get_content_metadata(URL, contentPayload):
	# response object
	obj = {}
	# form the query
	try:
		reqKeys = mustHavekeysFromContentModel + goodToHaveKeysFromContentModel
		query=URL+'?fields='+','.join(reqKeys)
		response=requests.get(query)
		response=json.loads(response.text.decode('UTF-8'))
		obj = response['result']['content']
		for key in obj.keys():
			if key not in "collections":
				val = obj[key]
				#print("Key:%s" + str(type(val))) %key
				if(isinstance(val, basestring)):
					# if a string, simply add it
					val =val		
				elif(isinstance(val, list)):
					if all(isinstance(x, dict) for x in val):
						val = [d['identifier'] for d in val]
					else:
						# unwrap a list, convert it into a gaint string
						val = ', '.join(val)
				elif(isinstance(val, dict)):
					val = flattenDict(val)										
				else:
					#print type(val)
					# works only if it a dictionary
					val = str(val)
				contentPayload[key]=val
	except:
		traceback.print_exc()
		# nothign to do, pass as is
		logging.info('Exception: Unable to access metadata')
		logging.info('just sending data as-is')
		contentPayload = contentPayload
		obj=obj
	return contentPayload,obj

def enrichContent(reqJson):
	# create the default payload
	contentPayload={} # dictionary object
	contentPayload['identifier']='' # string
	contentPayload['concepts']=[] # array
	contentPayload['tags']=[] # array
	contentPayload['description']='' # string
	contentPayload['text']=[] # array
	contentPayload['goodToHaveKeysFromContentModel']=goodToHaveKeysFromContentModel # array
	contentPayload['mustHavekeysFromContentModel']=mustHavekeysFromContentModel # array
	contentPayload['enrichedKeysFromML']=enrichedKeysFromML # array

	# check if the input is a valid URL
	try:
		#print std_input
		std_input = json.loads(reqJson)
	except:
		traceback.print_exc()
		msg = 'Exception: Not able to read json input stream'
		logging.info(msg)
		#print(msg)
		sys.exit(1)

	# do basic input checking
	if not std_input.has_key('content_url'):
		msg = 'No Content URL provided'
		logging.info(msg)
		sys.exit(1)
	URL = std_input['content_url']

	if not std_input.has_key('base_url'):
		msg = 'No base URL provided'
		logging.info(msg)
		sys.exit(1)
	BASE_URL = std_input['base_url']

	if not validators.url(URL):
		msg = 'Not a valid URL'
		logging.info(msg)
		sys.exit(1)

	if not std_input.has_key('enrichMedia'):
		msg = 'enrichMedia option not provided. Turning it off'
		logging.info(msg)
		ENRICH_MEDIA = 'False'
	else:
		ENRICH_MEDIA = std_input['enrichMedia']=='True'

	# content id
	identifier = os.path.split(URL)[-1]
	contentPayload['identifier']=identifier
	#downloaded_path = os.path.join(op_dir,identifier)

	# set up logging
	path=os.path.join(op_dir,identifier)
	#print'path' + path
	logfile_name = os.path.join(log_dir,'%s.log'%(identifier))

	#print 'log file name' + logfile_name

	logging.info('Name:%s'%(identifier))


	# get minimal data available from the content-model and update
	# the output json bucket (content)

	# get baseline contentPayLoad
	contentPayload, obj = get_content_metadata(URL, contentPayload)

	#print 'processing 1'

	# Download Content
	try:
		#print 'processing 1.1'
		query=URL+'?fields=downloadUrl'
		r=requests.get(query)
		r=json.loads(r.text)
		downUrl=r['result']['content']['downloadUrl']
		logging.info('downloadUrl%s'%(downUrl))
	except:
		#print 'processing 1.2'
		logging.info('Exception: No downloadUrl')
		logging.info('Writing whatever is available from Content-Model')
		print(json.dumps(contentPayload))
		sys.exit(0)

	#print 'processing 2'
	# To download zipfiles
	try:
		#print 'processing 2.1'
		downloadZipFile(downUrl,os.path.join(op_dir,'temp'+identifier))
		logging.info('Content Downloaded')
	except:
		#print 'processing 2.2'
		logging.info('Exception:Unable to download content')
		logging.info('Writing whatever is available from Content-Model')
		print(json.dumps(contentPayload))
		sys.exit(0)

	#print 'processing 3'
	# Import and use downloadContent
	try:
		#print 'processing 3.1'
		unzip_files(os.path.join(op_dir,'temp'+identifier))
		logging.info('Files unzipped')
		copy_main_folders(op_dir,identifier)
		logging.info('Folders shifted')
		add_manifest(obj,os.path.join(op_dir,identifier))
		logging.info('Manifest added')
		logging.info('Pre-Processing Complete')
	except:
		#print 'processing 3.2'
		logging.info('Could not process the ecar files')
		logging.info('Writing whatever is available from Content-Model')
		print(json.dumps(contentPayload))
		sys.exit(0)

	#print 'processing 4'
	#########Handle Media if the flag is on
	if(ENRICH_MEDIA=='True'):
		try:
			#print 'processing 4.1'
			media_type=['mp3','ogg','png','gif','jpg']
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

			# optional (writing to a file)
			with codecs.open(os.path.join(path,'mediaStats.json'),'w',encoding='utf-8') as f:
				json.dumps(mediaStats, f, sort_keys=True, indent=4)
			f.close()
			logging.info('Assets handled')
			
			# enriching the contentModel
			logging.info('Enriching the Content Model based on asset processing')
			for key in enrichedKeysFromML:
				if not contentPayload[key]:
					# if this list is empty
					contentPayload[key]=mediaStats[key]
		except:
			logging.info('Did not process some or all Assets. Skipping this step')
		else:
			#print 'processing 4.2'
			logging.info('User chose not to not process Assets. Skipping this step')

	#print 'processing 5'
	#########Parse Json
	try:
		#print 'processing 5.1'
		#Parse all jsons in a particular subdirectory of an identifier ['assets','data','items'] and save as ___.json Eg-assets.json
		for subdir in ['data','items']:
			json_files=findFiles(os.path.join(path,subdir),['.json'])
			extracted_json = extract_json(json_files)
			
			contentPayload[subdir] = extracted_json
			
			# (optional) write to a file
			with codecs.open(os.path.join(path,'%s.json'%(subdir)),'w',encoding='utf-8') as f:
				json.dumps(extracted_json, f, sort_keys=True, indent=4)
			f.close()
			logging.info('%s JSON files handled and enriched'%(subdir))
	except:
		#print 'processing 5.2'
		logging.info('Unable to parse data and items folder. Skipping this step')

	#########Get Concepts
	#print 'processing 6'
	try:
		#print 'processing 6.1'
		#Download concept list if necessary
		if (not os.path.isfile(os.path.join(op_dir,'conceptList.txt'))):
			getConcepts(BASE_URL)
		
		#Load concept list
		with codecs.open(os.path.join(op_dir,'conceptList.txt'),'r',encoding='utf-8') as f:
			conceptList=f.readlines()
		conceptList=conceptList[0].split(',')
		#Filter to get Concepts
		
		enrichedConcepts = filter_assessment_data(path,conceptList);
		
		if not contentPayload['concepts']:
			# do a set addition
			contentPayload['concepts']=enrichedConcepts
		else:
			originalConcept=list(contentPayload['concepts'])
			contentPayload['concepts']=list(set(originalConcept+enrichedConcepts))
	except:
		traceback.print_exc()
		#print 'processing 6.2'
		logging.info('Unable to read and/or enrich concepts from domain model. Skipping this step')

	#print 'Done'

	# #########Relevant Content Keys (relevant pieces of content metadata)
	# keys=['ageGroup','contentType','gradeLevel','languageCode','language','developer','publisher','author',
	# 	  'illustrators','genre','subject','Subject','domain','popularity','filter','owner','concepts',
	# 	  'objectsUsed','keywords','tags','description','text']

	# #########Convert to EnrichedJSON
	# print 'processing 7'
	# logging.info('Enriching JSON: %s'%(op_dir))

	# the folling is not necessary, as basic content-meta-data is already populated
	# #Read key value pairs from manifest.json
	# with codecs.open(os.path.join(path,'manifest.json'),'r',encoding='utf-8') as f:
	# 	dct=json.load(f)
	# f.close()
	# for key in keys:

	# 	if key in dct:
	# 		content[key]=dct[key]
	#Read media statistics from mediaStats.json

	# # this block is moved into media_enrichment module
	# with codecs.open(os.path.join(path,'mediaStats.json'),'r',encoding='utf-8') as f:
	# 	dct=json.load(f)
	# f.close()
	# for key in dct.keys():
	# 	content[key]=dct[key]
	# logging.info('Media stats added')


	# # this block is moved into json_enrichment module
	# #Read json values from assets,data and items
	# for i in ['data','items']:
	# 	with codecs.open(os.path.join(path,'%s.json'%(i)),'r',encoding='utf-8') as f:
	# 		content[i]=f.readlines()
	# 	f.close()
	# logging.info('Data and items values added')

	# # this block is moved to where concepts are processed
	# with codecs.open(os.path.join(op_dir,'conceptList.txt'),'r',encoding='utf-8') as f:
	# 	content['concepts']=f.readlines()
	# f.close()
	# logging.info('Concepts Read')

	print(json.dumps(contentPayload))
	shutil.rmtree(path)

for line in sys.stdin:
	str_line = line.rstrip('\n')
	logging.info('Request JSON: %s', str_line)
	if str_line:
		enrichContent(str_line)
	else:
		print("Empty input received")