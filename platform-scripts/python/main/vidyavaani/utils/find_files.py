# Author: Aditya Arora, adityaarora@ekstepplus.org

import os

#This function traverses a directory finding all files with a particular substring
#Returns a list of files found
def findFiles(directory,substrings):
	ls=[]
	if (type(directory) == unicode or type(directory) == str) and type(substrings) == list:
	# assert type(directory)==unicode or type(directory)==str
	# assert type(substrings)==list
		if os.path.isdir(directory):
			for dirname, dirnames, filenames in os.walk(directory):
				for filename in filenames:
					string=os.path.join(dirname, filename)
					for substring in substrings:
						if(string.find(substring)>=0):
							ls.append(string)
	return ls
