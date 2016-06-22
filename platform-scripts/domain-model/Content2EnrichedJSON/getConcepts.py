# coding: utf-8

# In[1]:

import requests
import codecs
import argparse

# In[2]:

#This section parses arguments passed through it from command line
parser = argparse.ArgumentParser()
#Metadata url
parser.add_argument("--url",type=unicode,help="This the url where the list of metadata of all concepts are stored. Default:Sandbox content metadata url",default="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map")
args = parser.parse_args()
#Parse the url
URL=args.url

# In[3]:

#Load concept list
try:
	resp=requests.get(URL).json()
	conceptList=[]
	for i in resp['result']['concepts']:
		for k in i.keys():
			try:
				conceptList.index(i['identifier'])
			except:
				conceptList.append(i['identifier'])
except:
	print("Bad internet")

# In[4]:

#Save concept list to file
with codecs.open("./concList.txt","w",encoding="utf-8") as f:
	f.write(",".join(conceptList))
f.close()
