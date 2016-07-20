# Author: Aditya Arora, adityaarora@ekstepplus.org

import requests
import zipfile
import StringIO

#Downloads the folder referred to by the url and saves it in the directory. Returns True if the download was successful
def downloadZipFile(url,directory):
	assert type(url)==unicode
	assert type(directory)==unicode or type(directory)==str
	r=requests.get(url)
	try:
		z=zipfile.ZipFile(StringIO.StringIO(r.content))
		z.extractall(directory)
		return r.ok
	except:
		return False
#	 For python 3.x use the following line
#	 z=zipfile.ZipFile(io.BytesIO(r.content))

