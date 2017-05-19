# Author(s): Aditya Arora, adityaarora@ekstepplus.org
# 		   : Soma Dhavala soma@ilimi.in
# 		   : Ajit Barik ajitbk@ilimi.in
# 		   : Santhosh santhosh@ilimi.in


import os
import logging  # Log the data given
import sys
import ConfigParser
import types

# Pass as a commandline argument later on
root = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join((os.path.split(root)[0]), 'utils')
resource = os.path.join((os.path.split(root)[0]), 'resources')
config_file = os.path.join(resource, 'config.properties')
# Insert at front of list ensuring that our util is executed first in
sys.path.insert(0, utils)
root = os.path.dirname(os.path.abspath(__file__))

# from download_zip_file import downloadZipFile
from download_content import *
from get_lowest_key_value import flattenDict
from handle_media import *
from parse_json import *
from concept_filter import *

# getting paths from config file
config = ConfigParser.SafeConfigParser()
config.read(config_file)

op_dir = config.get('FilePath', 'temp_path')
log_dir = config.get('FilePath', 'log_path')
concepts_dir = config.get('FilePath', 'concepts_path')  # remove
download_file_prefix = config.get('FilePath', 'download_file_prefix')


# must've-fields as part of the content meta-data. empty values are fine
mustHavekeysFromContentModel = ['identifier',
                                'concepts', 'tags', 'description', 'text']
goodToHaveKeysFromContentModel = ['ageGroup', 'contentType', 'gradeLevel', 'languageCode', 'language', 'developer', 'publisher', 'author',
                                  'illustrators', 'genre', 'domain', 'popularity', 'filter', 'owner',
                                  'objectsUsed', 'keywords', 'status']
enrichedKeysFromML = ['mediaCount', 'mp3Length',
                      'mp3Transcription', 'imageTags']

# define output file to write to (stdout)
out = sys.stdout
#msg = 'test write to out'
# out.write(msg)

# this is specific to this module (not a part of content model)
# content['exitStatus']=-1

# #logging.info('Converting to enriched JSON: %s'%(op_dir))
# # this function does write the json to stdout upon exit
# def writePayLoadToStreamOnExit():
#    print "Exiting the Content Enriching Module"
#     print(json.dumps(content))
# atexit.register(writePayLoadToStreamOnExit()) 

def createDirectory(dir):
	try:
	    if not os.path.exists(dir):
	        os.makedirs(dir)
	except OSError, e:
	    if e.errno != 17:
	        traceback.print_exc()
	        sys.exit(1)
	except:
	    traceback.print_exc()
	    msg = 'Not able to find/create log and/or tmp dir'
	    logging.warn(msg)
	    sys.exit(1)

# Create all required directories
createDirectory(op_dir);
createDirectory(log_dir);
createDirectory(concepts_dir);

logfile_name = os.path.join(log_dir, 'enrich_content.log')
logging.basicConfig(filename=logfile_name, level=logging.INFO)
logging.info('### Enriching content ###')


def enrichContent(contentJson):
    #  create the default payload
    contentPayload = {}  # dictionary object
    contentPayload['identifier'] = ''  # string
    contentPayload['concepts'] = []  # array
    contentPayload['tags'] = []  # array
    contentPayload['description'] = ''  # string
    contentPayload['text'] = []  # array
    contentPayload[
        'goodToHaveKeysFromContentModel'] = goodToHaveKeysFromContentModel  # array
    contentPayload[
        'mustHavekeysFromContentModel'] = mustHavekeysFromContentModel  # array
    contentPayload['enrichedKeysFromML'] = enrichedKeysFromML  # array

    # check if the input is a valid URL
    try:
        # print std_input
        std_input = json.loads(contentJson)
    except:
        traceback.print_exc()
        msg = 'Exception: Not able to read json input stream'
        logging.warn(msg)
        print(contentJson)
        return 1

    # get minimal data available from the content-model and update
    # the output json bucket (content)
    NumberTypes = (types.IntType, types.LongType, types.FloatType, types.ComplexType)
    for key in std_input.keys():
    	val = std_input[key]
    	if isinstance(val, NumberTypes):
    		val = str(val)
        contentPayload[key] = val

    if not std_input.has_key('enrichMedia'):
        msg = 'enrichMedia option not provided. Turning it off'
        logging.warn(msg)
        ENRICH_MEDIA = 'False'
    else:
        ENRICH_MEDIA = std_input['enrichMedia'] == 'True'

    # content id
    identifier = contentPayload['identifier']

    # set up logging
    path = os.path.join(op_dir, identifier)
    logging.info('Name:%s' % (identifier))

    # Import and use downloadContent

    try:
    	downloaded_file = os.path.split(contentPayload['downloadUrl'])[-1]
        directory = os.path.join(op_dir, download_file_prefix + identifier)
        zip_file = os.path.join(op_dir, download_file_prefix + downloaded_file)
        with zipfile.ZipFile(zip_file, 'r') as z:
            z.extractall(directory)
        unzip_files(os.path.join(op_dir, download_file_prefix + identifier))
        logging.info('Unzipped file')
        copy_main_folders(op_dir, identifier, download_file_prefix + identifier)
        logging.info("files copied")
        add_manifest(std_input, os.path.join(op_dir, identifier))
        logging.info('%s:Pre-Processing Complete' % (identifier))
    except:
        logging.warn('Could not process the ecar files')
        logging.warn('Writing whatever is available from Content-Model')
        print(json.dumps(contentPayload))
        return 1

    # print 'processing 4'
    # Handle Media if the flag is on
    if(ENRICH_MEDIA == 'True'):
        try:
            # print 'processing 4.1'
            media_type = ['mp3', 'ogg', 'png', 'gif', 'jpg']
            count = count_file_type_directory(path, media_type)
            logging.info('Counted all media file types')
            mp3Files = findFiles(path, ['mp3'])
            (mp3Length, bad_mp3) = count_MP3_length_directory(mp3Files)
            logging.info(
                'Counted mp3 length of all files except for these: %s' % (','.join(bad_mp3)))
            mp3Files = findFiles(path, ['mp3'])
            (transcribed, bad_mp3) = speech_recogniser(mp3Files)
            msg = 'transcribed : '+transcribed
            logging.info(
                'Transcribed all mp3 files except for these: %s' % (','.join(bad_mp3)))
            # transcribed = add_confidence(transcribed)
            logging.info(msg)
            logging.info(
                'Default confidence score added for empty confidence score')
            images = imageNames(path)
            logging.info('Added image filenames')
            mediaStats = {'mediaCount': count, 'mp3Length': mp3Length,
                          'mp3Transcription': transcribed, 'imageTags': images}

            # optional (writing to a file)
            with codecs.open(os.path.join(path, 'mediaStats.json'), 'w', encoding='utf-8') as f:
                json.dumps(mediaStats, f, sort_keys=True, indent=4)
            f.close()
            logging.info('Assets handled')

            # enriching the contentModel
            logging.info(
                'Enriching the Content Model based on asset processing')
            for key in enrichedKeysFromML:
                if not contentPayload[key]:
                    # if this list is empty
                    contentPayload[key] = mediaStats[key]
        except:
            logging.warn(
                'Did not process some or all Assets. Skipping this step')
        else:
            # print 'processing 4.2'
            logging.warn(
                'User chose not to not process Assets. Skipping this step')

    # print 'processing 5'
    # Parse Json
    try:
        # print 'processing 5.1'
        # Parse all jsons in a particular subdirectory of an identifier
        # ['assets','data','items'] and save as ___.json Eg-assets.json
        for subdir in ['data', 'items']:
            json_files = findFiles(os.path.join(path, subdir), ['.json'])
            extracted_json = extract_json(json_files)

            contentPayload[subdir] = extracted_json

            # (optional) write to a file
            with codecs.open(os.path.join(path, '%s.json' % (subdir)), 'w', encoding='utf-8') as f:
                json.dumps(extracted_json, f, sort_keys=True, indent=4)
            f.close()
            logging.info('%s JSON files handled and enriched' % (subdir))
    except:
        # print 'processing 5.2'
        logging.warn(
            'Unable to parse data and items folder. Skipping this step')

    # # Get Concepts
    # # print 'processing 6'
    # try:
    #     # Load concept list
    #     with codecs.open(os.path.join(concepts_dir, 'conceptList.txt'), 'r', encoding='utf-8') as f:
    #         conceptList = f.readlines()
    #     conceptList = conceptList[0].split(',')
    #     # Filter to get Concepts
    #
    #     enrichedConcepts = filter_assessment_data(path, conceptList)
    #
    #     if not contentPayload['concepts']:
    #         # do a set addition
    #         contentPayload['concepts'] = enrichedConcepts
    #     else:
    #         originalConcept = list(contentPayload['concepts'])
    #         contentPayload['concepts'] = list(
    #             set(originalConcept + enrichedConcepts))
    # except:
    #     traceback.print_exc()
    #     # print 'processing 6.2'
    #     logging.warn(
    #         'Unable to read and/or enrich concepts from domain model. Skipping this step')
    #read ecml file

    try:
        ecml_file = os.path.join(os.path.join(op_dir, identifier),'index.ecml')
        ecml_text = get_text(ecml_file)
        logging.info("reading ecml for "+identifier)
        p_text= str(contentPayload['text'])
        all_text = ecml_text+p_text
        all_text_list = []
        all_text_list.append(all_text)
        contentPayload['text'] = all_text_list
    except:
        logging.warn('Unable to read ecml file. Skipping this step')
    print(json.dumps(contentPayload))
    # shutil.rmtree(path)

