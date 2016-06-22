# coding: utf-8

# In[1]:

import requests
import codecs
import os

# In[2]:

#Load concept list
root=os.path.dirname(os.path.abspath(__file__))
with codecs.open(os.path.join(root,'concList.txt'),'r',encoding='utf-8') as f:
	conceptList=f.readlines()
conceptList=conceptList[0].split(',')


# In[3]:

#This finds the concepts from the assessment data
def filter_assessment_data(directory):
	concList=set([])
	with codecs.open(os.path.join(directory,"items.txt"),"r",encoding="utf-8") as f:
		item_list=f.readlines()
	f.close()
	for items in item_list:
		item=items.split(",")
		for i in item:
			string=i.replace(' ','')[2:-1]
			if(string in conceptList):
				concList.add(string)
	with codecs.open(os.path.join(directory,"concepts.txt"),"w",encoding="utf-8") as f:
		f.write(",".join(concList))
	f.close()


# In[4]:

#Getting concept list
root=os.path.dirname(os.path.abspath(__file__))
root_dir=os.path.join(root,"Data")
identifiers=[os.path.join(root_dir, name) for name in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, name))]
for identifier in identifiers:
	filter_assessment_data(identifier)
