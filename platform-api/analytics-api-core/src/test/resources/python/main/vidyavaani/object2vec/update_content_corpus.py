import sys

for line in sys.stdin:
    str_line = line.rstrip('\n')
    if str_line:
        print """{"en": "\\nek gajar ki keemat kya hai"}"""
    else:
        print("Empty input received")
