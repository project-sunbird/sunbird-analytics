# Author: Aditya Arora, adityaarora@ekstepplus.org

import subprocess
import requests
import argparse
import os

#Pass as a commandline argument later on
root=os.path.dirname(os.path.abspath(__file__))
ekstep='/'.join(root.split('/')[:-2])
import sys
sys.path.insert(0, os.path.join(ekstep,'Utils'))#Insert at front of list ensuring that our util is executed first in case of name clash
import findFiles


parser = argparse.ArgumentParser()
parser.add_argument('--ld',help='This is the operating directory',default=os.path.join(root,'Data'))
args = parser.parse_args()
op_dir=args.ld
if not os.path.exists(op_dir):
	os.makedirs(op_dir)

r=requests.get('http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/content/list').json()
total_identifiers=[obj['identifier'] for obj in r['result']['contents']]
file_list=findFiles.findFiles(op_dir,['.json'])
present_identifiers=[identifier[:-5].split('/')[-1] for identifier in file_list]
absent_identifiers=[identifier for identifier in total_identifiers if identifier not in present_identifiers]
root=os.path.dirname(os.path.abspath(__file__))

for response in r['result']['contents'][520:]:
	try:
		if(response['identifier'] not in absent_identifiers):
			continue
		subprocess.call(['python content2EnrichedJson.py \'http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/content\' \'%s\''%(response['identifier'])], shell=True)
	except:
		print('Ouch',response)
