# Author: Aditya Arora, adityaarora@ekstepplus.org

import requests
import codecs
import ConfigParser
import os

root=os.path.dirname(os.path.abspath(__file__))
resource = os.path.join((os.path.split(root)[0]),'resources')
config_file = os.path.join(resource,'config.properties')
#getiing paths from config file
config = ConfigParser.RawConfigParser()
config.read(config_file)
op_dir = config.get('FilePath', 'temp_path')
conceptListFile = os.path.join(op_dir,'conceptList.txt')

def getConcepts(baseURL):
	
	url = baseURL + "/v2/domains/numeracy/concepts"
	try:
		resp=requests.get(url)
		resp=json.loads(resp.text)
		conceptList=[]
		for i in resp['result']['concepts']:
			try:
				conceptList.index(i['identifier'])
			except:
				conceptList.append(i['identifier'])
	except:	
		print("Bad internet")
	with codecs.open(conceptListFile,"a",encoding="utf-8") as f:
		f.write(",".join(conceptList))
	f.close()
