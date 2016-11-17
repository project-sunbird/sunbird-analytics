import sys
from update_content_corpus_functions import *

for line in sys.stdin:
    str_line = line.rstrip('\n')
    if str_line:
        updateContentCorpus(line.rstrip('\n'))
    else:
        print("Empty input received")