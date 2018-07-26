#!/usr/bin/env python
import traceback

try:
    openFile = open('notExistsFile.txt', 'r')
    fileContent = openFile.readlines()
except IOError as info:
    print 'File not Exists'
    print info
    traceback.print_exc()
    print 'continue'
except:
    print 'process exception'
else:
    print 'Reading the file'