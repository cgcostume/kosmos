import os
import glob
import re
# import exifread
import sys
# import datetime
import pytz
import hashlib

from tzlocal import get_localzone
from time import sleep
from collections import OrderedDict
from win32com.propsys import propsys, pscon


sheduled_files = { }

def update_progress():
	print('.', end = '', flush = True)


files_globex = [ '*.jpg', '*.JPG', '*.srw', '*.jpeg', '*.PNG', '*.png', '*.mp4', '*.mov', '*.avi' ]

for g in files_globex:

	print (g)

	for file_path in glob.glob('**/' + g, recursive = True):
		if not os.path.isfile(file_path):
			continue
			
		size = os.path.getsize(file_path)

		if not size in sheduled_files:
			sheduled_files[size] = []

		if not file_path in sheduled_files[size]:
			sheduled_files[size].append(file_path)

		update_progress()

globex = [ ]


print ('/')

def process_files(sheduled_files):

	scheduled_duplicates = { }

	ts_count = 0

	for key, file_paths in sheduled_files.items():

		if len(file_paths) < 2:
			continue

		hash_first = '';

		for index, file_path in enumerate(file_paths):

			hash_md5 = hashlib.md5()
			with open(file_path, 'rb') as file:
				for chunk in iter(lambda: file.read(4096), b''):
					hash_md5.update(chunk)

			if index == 0:
				hash_first = hash_md5.hexdigest()
				continue
			
			if hash_first == hash_md5.hexdigest():
				print('duplicates:', '"' + file_paths[0] + '"', '"' + file_path + '"')
  
	return scheduled_duplicates

	# os.rename(file_path, newname)


# def rename_files(scheduled_renames):
# 	
# 	for file_path, new_name in reversed(scheduled_renames.items()):
# 		if file_path == new_name:
# 			continue
# 
# 		print('rename', file_path, '->', new_name)
# 		os.rename(file_path, new_name)
# 		#sleep(0.1)


print()
scheduled_duplicates = process_files(sheduled_files)

# print()
# input('press [enter] to apply these changes')

# rename_files(scheduled_renames)
