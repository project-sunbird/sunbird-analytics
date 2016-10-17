import sys
from enrich_content_functions import *

for line in sys.stdin:
    str_line = line.rstrip('\n')
    logging.info('Request JSON: %s', str_line)
    if str_line:
        enrichContent(str_line)
    else:
        print("Empty input received")