# coding: utf-8

# In[1]:

import json
import os
import codecs


# In[2]:

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


# In[3]:

#Get all unique values corresponding to all keys in a dictionary(including any enclosed sub dictionaries)
def get_all_values(obj):
	values=set()
	if(isinstance(obj,dict)):
		keys=obj.keys()
		for key in keys:
			val_list=get_all_values(obj[key])
			for value in val_list:
				values.add(value)
	elif(isinstance(obj,list)):
		for dct in obj:
			val_list=get_all_values(dct)
			for value in val_list:
				values.add(value)
	else:
		values=set([obj])
	return list(values)


# In[4]:

#Unrolls .json.bk files to plain json
def unroll_json_bk(file_names):
	for filename in file_names:
		try:
			with codecs.open(filename,'r',encoding='utf-8') as f:
				lines = f.readlines()
			f.close()
			x=[]
			bracket=0
			k=0
			for ln in lines:
				for char in ln:
					if char=='{':
						bracket+=1
					elif char=='}':
						bracket-=1
				if(bracket!=0):
					x.append(ln)
				else:
					x.append("}")
			with codecs.open("%s%d.json"%(filename[:-8],k),'w',encoding="utf-8") as f:
				f.write("\n".join(x))
			f.close()
			k+=1
			x=[]
			os.remove(filename)
		except:
			print(filename)


# In[5]

#Extract json file values to list
def extract_json(json_filenames):
	json_files=[]
	for filename in json_filenames:
		try:
			with codecs.open(filename,"r",encoding="utf-8") as f:
				json_data = json.load(f)
			json_files.append(get_all_values(json_data))
			f.close()
		except ValueError:
			f.close()
			os.remove(filename);
	return json_files


# In[6]

#Get immediate subdirectories of a particular directory
def get_immediate_subdir(root_dir):
	return [name for name in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, name))]


# In[7]

#Parse all jsons in a particular subdirectory of an identifier ['assets','data','items'] and save as ___.txt Eg-assets.txt
root=os.path.dirname(os.path.abspath(__file__))
root_dir=os.path.join(root,"Data")

identifiers=get_immediate_subdir(root_dir)
for identifier in identifiers:
	dirs=os.path.join(root_dir,identifier)
	for subdir in get_immediate_subdir(dirs):
		bk_files=find_files(os.path.join(dirs,subdir),[".json.bk"])
		unroll_json_bk(bk_files)
		json_files=find_files(os.path.join(dirs,subdir),[".json"])
		with codecs.open(os.path.join(dirs,"%s.txt"%(subdir)),"w",encoding="utf-8") as f:
			f.write(str(extract_json(json_files)).replace("[","\n").replace("]","\n"))
		f.close()
