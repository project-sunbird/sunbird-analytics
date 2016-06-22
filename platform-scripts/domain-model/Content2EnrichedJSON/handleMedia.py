# coding: utf-8

# In[1]:

import os
import mutagen.mp3 as mp3
from pydub import AudioSegment
import signal
import speech_recognition as sr
import json
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

#directory is the one to be explored. This counts the length of mp3 files in a directory 
#(Due to parsing of mp3 files to json files we no longer need this as much but there might exist some unparsed 
#files that failed to be converted)
def count_MP3_length_directory(mp3_filelist,**kwargs):
	verbose=kwargs.get("verbose",False)
	x=0
	try:
		for mp3_file in mp3_filelist:	
			audio = mp3.MP3(mp3_file)
			x+=audio.info.length
	except mp3.HeaderNotFoundError:
		os.remove(mp3_file)
		if(verbose):
			print("Media read error")
	return x


# In[4]:

# Count all Files in a Directory having any one of the Types in the list Typ (Accepts only a list object)
# directory is the one to be explored and typ is a list of the filetypes whose counts we want
def count_file_type_directory(directory,typ):
	x={}
	for i in typ:
		x[i]=0
	file_list=find_files(directory,typ)
	for fl in file_list:
		x[fl.split('.')[-1]]+=1
	return x


# In[5]:

def handler(signum, frame):
	raise OSError("Couldn't open device!")


# In[6]

# mp3_file_names is a list of paths of mp3 files on whom speech recognition is to be applied
def speech_recogniser(mp3_file_names,**kwargs):
	#Here verbose is true since I get impatient and I need some sign that the system is alive and running
	verbose=kwargs.get("verbose",True)
	#Language by default en-IN, pass appropriate language code
	#A list of some (not exhaustive) language codes is given here: 
	#http://www.lingoes.net/en/translator/langcode.htm
	language=kwargs.get("language","en-IN")
	#Convert mp3 to wav
	for mp3 in mp3_file_names:
		temp=mp3[:-4]		
		if(verbose):
			print(temp)
		try:
			# Set the signal handler and a 5-second alarm
			signal.signal(signal.SIGALRM, handler)
			signal.alarm(20)

			AudioSegment.from_mp3('%s.mp3'%(temp)).export('%s.wav'%(temp), format="wav")
			#subprocess.check_call(['ffmpeg', '-i', '%s.mp3'%(temp),'%s.wav'%(temp)])
			AUDIO_FILE = '%s.wav'%(temp)
			# use the audio file as the audio source
			r = sr.Recognizer()
			with sr.AudioFile(AUDIO_FILE) as source:
				audio = r.record(source) # read the entire audio file
			# recognize speech using Google Speech Recognition
			recog=r.recognize_google(audio,show_all=True,language="en-IN")
			f=open("%s.json"%(temp),"w")
			f.write(json.dumps(recog))
			f.close()
			os.remove('%s.wav'%(temp))
			signal.alarm(0)#Alarm disabled
			#To reduce memory usage delete mp3 files after speech recognition  has run its course
		except:
			if(verbose):
				print("Breaking Bad",temp)


# In[7]

#Get immediate subdirectories of a particular directory
def get_immediate_subdir(root_dir):
	return [name for name in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, name))]


# In[8]

#Parse all jsons in a particular subdirectory of an identifier ['assets','data','items'] and save as ___.txt Eg-assets.txt
root=os.path.dirname(os.path.abspath(__file__))
root_dir=os.path.join(root,"Data")
media_type=["mp3","ogg","png","gif","jpg"]

identifiers=get_immediate_subdir(root_dir)
for identifier in identifiers:
	path=os.path.join(root_dir,identifier)
	count=count_file_type_directory(path,media_type)
	mp3Files=find_files(path,["mp3"])
	mp3Length=count_MP3_length_directory(mp3Files)
	speech_recogniser(mp3Files)
	mediaStats={'mediaCount':count,'mp3Length':mp3Length}
	with codecs.open(os.path.join(path,'mediaStats.json'),'w',encoding='utf8') as f:
		json.dump(mediaStats, f, sort_keys=True, indent=4)
	f.close()
