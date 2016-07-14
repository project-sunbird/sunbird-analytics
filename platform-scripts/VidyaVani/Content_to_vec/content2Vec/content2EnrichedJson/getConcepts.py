# Author: Aditya Arora, adityaarora@ekstepplus.org

import requests
import codecs

def getConcepts(URL='http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map'):
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
	with codecs.open("./conceptList.txt","w",encoding="utf-8") as f:
		f.write(",".join(conceptList))
	f.close()
