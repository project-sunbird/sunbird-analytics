# coding: utf-8

# In[1]:

import requests
import zipfile
import os
import shutil
import StringIO #In Python 3.x substitute StringIO with io
import argparse
import codecs
import json


# In[2]:

#This section parses arguments passed through it from command line
parser = argparse.ArgumentParser()
#Metadata url
parser.add_argument("--url",type=unicode,help="This the url where the list of metadata of all pieces of content are stored. Default:Sandbox content metadata url",default="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/content/list")
#Range of content to be downloaded
parser.add_argument("--range",help="This is the range of content to be downloaded from the list of metadata. Format --range='start,end'")
#If all content is to be downloaded
parser.add_argument("--all",help="This specifies that all the content from the given metadata url is to be downloaded",action="store_true",default=False)
#For debugging pass verbose
parser.add_argument("--verbose",help="This makes the output verbose allowing for debugging",action="store_true",default=False)

#Read arguments given
args = parser.parse_args()
verbose=args.verbose
all_content=args.all
url=args.url
download_range=args.range.split(",")
#Check that download range fits acceptable syntax
if download_range!=None:
	if(len(download_range)!=2):
		parser.error("Bad range. Range must be of form --range='start,end'")
#Check that of --all or --range only one or the other is given
if not args.all and args.range==None:
	parser.error("Must specify either --range or --all")
if args.all and args.range!=None:
	parser.error("Must specify only one of --range or --all")


# In[3]:

#Downloads the folder referred to by the url and saves it in the directory
#Returns True if the download was successful
def download_file(url,directory):
	assert type(url)==unicode
	assert type(directory)==unicode or type(directory)==str
	r=requests.get(url)
	z=zipfile.ZipFile(StringIO.StringIO(r.content))
#	 For python 3.x use the following line
#	 z=zipfile.ZipFile(io.BytesIO(r.content))
	z.extractall(directory)
	return r.ok


# In[4]:

#This function traverses a directory finding all files with a particular substring
#Returns a list of files found
def find_files(directory,substrings):
	assert type(directory)==unicode or type(directory)==str
	assert type(substrings)==list
	ls=[]
	for dirname, dirnames, filenames in os.walk(directory):
		for filename in filenames:
			string=os.path.join(dirname, filename)
			for substring in substrings:
				if(string.find(substring)>=0):
					ls.append(string)
	return ls


# In[5]:

#Extracts all zipfiles into the download directory of that piece of content and deletes the zip files after extraction
def unzip_files(directory):
	assert type(directory)==unicode or type(directory)==str
	zip_list=find_files(directory,[".zip"])
	for zip_file in zip_list:
		with zipfile.ZipFile(zip_file, "r") as z:
			z.extractall(directory)
		os.remove(zip_file)


# In[6]:

#Transfer the files in assets,data,items and the ecml files
def copy_main_folders(root,identifier):
	assert type(identifier)==unicode or type(identifier)==str
	assert type(root)==unicode or type(root)==str
	file_list=find_files(os.path.join(root,"temp"+identifier),['asset','data','item','ecml'])
	path=os.path.join(root,identifier)
	os.makedirs(path)
	os.makedirs(os.path.join(path,"assets"))
	os.makedirs(os.path.join(path,"data"))
	os.makedirs(os.path.join(path,"items"))
	for f in file_list:
		if(f.find("asset")>=0):
			shutil.copy(f,os.path.join(path,"assets"))
		elif(f.find("data")>=0):
			shutil.copy(f,os.path.join(path,"data"))
		elif(f.find("item")>=0):
			shutil.copy(f,os.path.join(path,"items"))
		else:
			shutil.copy(f,path)
	shutil.rmtree(os.path.join(root,"temp"+identifier))


# In[7]:

#Adds a manifest.json file for the given piece of content
def add_manifest(obj,directory):
	with codecs.open(os.path.join(directory,'manifest.json'),'w',encoding='utf8') as f:
		json.dump(obj, f, sort_keys=True, indent=4)
	f.close()


# In[8]:

#Accepts the metadata of a single piece of content and processes it so that the content is downloaded to root/identifier
#If successful it returns the identifier of the file downloaded else it returns None
def process_entity(obj,root,**kwargs):
	verbose=kwargs.get("verbose",False)
	#Check if the root directory exists else create it
	if not os.path.exists(root):
		os.makedirs(root)
	#Get Download URL and identifier
	try:
		identifier=obj["identifier"]
		url=obj["downloadUrl"]
	except:
		if(verbose):
			print("Either downloadURL or identifier not present")
		return None
	#Download the file from downloadURL
	if(not download_file(url,os.path.join(root,"temp"+identifier))):
		if(verbose):
			print("Unable to download file")
		return None
	unzip_files(os.path.join(root,"temp"+identifier))
	copy_main_folders(root,identifier)
	#Manifest is added for each piece of content separately simply because this format of manifest is clearer
	add_manifest(obj,os.path.join(root,identifier))
	if(verbose):
		print("Success "+identifier)
	return identifier


# In[9]:

root=os.path.dirname(os.path.abspath(__file__))
root_dir=os.path.join(root,"Data")
identifier_list=[]
r=requests.get(url).json()
if(all_content):
	download_range="0:%d"%(len(r['result']['contents']))
start=max(int(download_range[0]),0)
end=min(int(download_range[1]),len(r['result']['contents']))
for i in r['result']['contents'][start:end]:
	try:
		identifier_list.append(process_entity(i,root_dir,verbose=verbose))
		if(verbose):
			print("Downloaded",identifier_list)
	except KeyError:
		if(verbose):
			print("Something is seriously wrong with the structure of response from metadata")
	except:
		if(verbose):
			print("Something wierd")#TODO: Expand this with more test cases


# In[10]:

#Tests someone can run in order to understand and verify the code
##Test download_file()
#r=requests.get("http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/content/list").json()
#print(r.keys())
#print(r['result'].keys())
#print(type(r['result']['contents']))
#print(r['result']['contents'][10]['downloadUrl'])
#obj1=r['result']['contents'][0]
#download_file(obj1['downloadUrl'],"./Data/temp"+obj1['identifier'],verbose=True)
#obj2=r['result']['contents'][10]
#download_file(obj2['downloadUrl'],"./Data/temp"+obj2['identifier'],verbose=True)
##Test find_files()
#print(find_files("./Data/temp"+obj1['identifier'],["items"]))
#print(find_files("./Data/temp"+obj2['identifier'],[".zip"]))
##Test unzip_files()
#dir2="./Data/temp"+obj1['identifier']
#unzip_files(dir2)
#dir1="./Data/temp"+obj1['identifier']
#unzip_files(zl,dir1)
##Test copy_main_folders()
#root="./Data"
#identifier1=obj1['identifier']
#identifier2=obj2['identifier']
#copy_main_folders(root,identifier1)
#copy_main_folders(root,identifier2)
##Test process_entity()
#process_entity(obj1,root,verbose=True)
#process_entity(obj2,root,verbose=True)

