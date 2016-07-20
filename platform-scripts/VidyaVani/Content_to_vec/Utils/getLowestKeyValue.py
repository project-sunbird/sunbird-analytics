# Author: Aditya Arora, adityaarora@ekstepplus.org

import json
import codecs
import networkx as nx

G=nx.DiGraph()

def encodeName(obj):
	if(type(obj)!=unicode):
		return str(obj)
	else:
		return obj.encode('utf-8')
		#http://stackoverflow.com/questions/5096776/unicode-decodeutf-8-ignore-raising-unicodeencodeerror

def f(obj,parent):
	if(type(obj)==dict):
		for key in obj.keys():
			f(key,parent)
			f(obj[key],key)
	elif(type(obj)==list):
		for item in obj:
			f(item,parent)
	else:
		G.add_edge(encodeName(obj),encodeName(parent))

def objpath(node):#Path from node to root
	return ','.join(nx.shortest_path(G,node,'groot')[1:])

def flattenDict(obj):
	assert type(obj)==dict	
	f(obj,'groot')
	flattened_dict={}
	for node in G.nodes():
		if(len(list(nx.ancestors(G,node)))==0):
			try:
				flattened_dict[objpath(node)].append(node.decode('utf-8'))
			except:
				flattened_dict[objpath(node)]=[node.decode('utf-8')]
	return flattened_dict
