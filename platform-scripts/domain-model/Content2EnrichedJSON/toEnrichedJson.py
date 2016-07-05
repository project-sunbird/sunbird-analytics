# coding: utf-8

# In[1]:

import os
import json
import codecs
import shutil


# In[2]:

#Keys include all the relevant pieces of content metadata
keys=["ageGroup","contentType","gradeLevel","languageCode","language","developer","publisher","author",
	  "illustrators","genre","subject","Subject","domain","popularity","filter","owner","concepts",
	  "objectsUsed","keywords","tags","description","text"]

root=os.path.dirname(os.path.abspath(__file__))
root_dir=os.path.join(root,"Data")

#identifiers are simply the folder names in "./Data"
identifiers=[os.path.join(root_dir, name) for name in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, name))]

for identifier in identifiers:
	content={}
	path=os.path.join(root_dir,identifier)
	content["identifier"]=identifier
	#Read key value pairs from manifest.json	
	with codecs.open(os.path.join(path,"manifest.json"),"r",encoding="utf-8") as f:
		dct=json.load(f)
	f.close()
	for key in keys:
		if key in dct:
			content[key]=dct[key]
	#Read media statistics from mediaStats.json
	with codecs.open(os.path.join(path,"mediaStats.json"),"r",encoding="utf-8") as f:
		dct=json.load(f)
	f.close()
	for key in dct.keys():
		content[key]=dct[key]
	#Read json values from assets,data and items
	for i in ["assets","data","items","concepts"]:
		with codecs.open(os.path.join(path,"%s.txt"%(i)),"r",encoding="utf-8") as f:
			content[i]=f.readlines()
		f.close()
	#Write enriched JSON to root_dir("./Data")
	with codecs.open(os.path.join(root_dir,"%s.json"%(identifier)),"w",encoding="utf-8") as f:
		json.dump(content, f, sort_keys=True, indent=4)
	f.close()
	#shutil.rmtree(path)
