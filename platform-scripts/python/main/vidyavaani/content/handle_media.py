# Author: Aditya Arora, adityaarora@ekstepplus.org

import os
import mutagen.mp3 as mp3
from pydub import AudioSegment
import signal
import speech_recognition as sr
import json
import codecs
import re #For image names
#Pass as a commandline argument later on
root=os.path.dirname(os.path.abspath(__file__))
utils=os.path.join((os.path.split(root)[0]),'utils')
import sys
sys.path.insert(0, utils)#Insert at front of list ensuring that our util is executed first in #To find files with a particular substring
from find_files import findFiles

#This counts the length of mp3 files in a directory 
def count_MP3_length_directory(mp3_filelist):
	x=0
	ls=[]
	for mp3_file in mp3_filelist:	
		try:
			audio = mp3.MP3(mp3_file)
			x+=audio.info.length
		except mp3.HeaderNotFoundError:
			os.remove(mp3_file)
			ls.append(mp3_file)
	return x,ls

#This count all files in a directory grouped by type (in list typ)
def count_file_type_directory(directory,typ):
	x={}
	for i in typ:
		x[i]=0
	file_list=findFiles(directory,typ)
	for fl in file_list:
		try:
			x[fl.split('.')[-1]]+=1
		except:
		#In case filename has weird end type like ._oldpng (in org.ekstep.englishsecondlanguage and org.ekstep.esl1)
			{}
	return x


#Alarm to restrict conversion time
def handler(signum, frame):
	raise OSError("Couldn't open device!")

def speech_recogniser(mp3_file_names,language='en-IN'):
	#Language by default en-IN, pass appropriate language code
	#A list of some (not exhaustive) language codes is given here: http://www.lingoes.net/en/translator/langcode.htm
	ls=[]
	mp3_dct={}
	for mp3 in mp3_file_names:
		temp=mp3[:-4]		
		try:
			# Set the signal handler and a 5-second alarm
			signal.signal(signal.SIGALRM, handler)
			signal.alarm(20)
			#Convert mp3 to wav			
			AudioSegment.from_mp3('%s.mp3'%(temp)).export('%s.wav'%(temp), format="wav")
			AUDIO_FILE = '%s.wav'%(temp)
			#Use the audio file as the audio source
			r = sr.Recognizer()
			with sr.AudioFile(AUDIO_FILE) as source:
				audio = r.record(source) # read the entire audio file
			#Recognize speech using Google Speech Recognition
			mp3_dct[temp]=r.recognize_google(audio,show_all=True,language="en-IN")
			os.remove('%s.wav'%(temp))
			signal.alarm(0)#Alarm disabled
		except:
			#Bad mp3 files
			try:
				os.remove('%s.wav'%(temp))
			except:
				{}
			ls.append(mp3)
	return mp3_dct,ls


#http://stackoverflow.com/questions/29916065/how-to-do-camelcase-split-in-python
def camel_case_split(identifier):
	matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
	return [m.group(0) for m in matches]

#Get image names
def imageNames(directory):
	image_names=findFiles(directory,['png','gif','jpg'])
	image_names=[os.path.basename(image) for image in image_names]#Get filename from path
	image_names=[os.path.splitext(image)[0] for image in image_names]#Get filename without file type
#	image_names=[image[:-4] for image in image_names]#Possibly better since it can handle files with '.' in their name
	image_names=[' '.join(image.split('_')) for image in image_names]#Replace underscore('_') by space
	image_names=[' '.join(re.findall('[a-zA-Z]+', image)) for image in image_names]#Filter out numbers
	image_names=[' '.join(camel_case_split(image)) for image in image_names]#Split Camel Case
	image_names=[image.lower() for image in image_names]#Turn all text to lower case
	return(list(set(image_names)))#list(set(.)) removes identical values if any

#add confidence score if it dosen't exist
def add_confidence(mp3_dict):
	for key, value in mp3_dict.iteritems():
		# print key
		# print value
		if not value == []:
			dict1 = value.get('alternative')[0]
			if not dict1.has_key('confidence'):
				dict1['confidence'] = 0.8
	return mp3_dict
				
