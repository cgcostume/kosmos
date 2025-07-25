import os
import glob
import re
import exifread
import sys
import datetime

from time import sleep
from collections import OrderedDict


files = { }

def update_progress():
	print('.', end = '', flush = True)


exif_globex = [ '*.jpg', '*.srw', '*.jpeg' ]
date_taken_tag = 'EXIF DateTimeOriginal'
date_taken_tag_alternative = 'DateTimeOriginal'


for g in exif_globex:
	for file_path in glob.glob(g):
		if not os.path.isfile(file_path):
			continue
		
		file = open(file_path, 'rb')
		tags = exifread.process_file(file)
		if date_taken_tag in tags:
			date_taken = tags[date_taken_tag]
		elif date_taken_tag_alternative in tags:
			date_taken = tags[date_taken_tag_alternative]
		else:
			sys.exit('cannot read date_taken from exif: ' + str(tags))

		file.close()

		# print(date_taken)
		files[str(date_taken) + ' -- ' + file_path] = file_path

		update_progress()

globex = [ '*.mp4', '*.mov' ]

for g in globex:
	for file_path in glob.glob(g):
		if not os.path.isfile(file_path):
			continue

		date_created = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
		date_created = date_created.strftime('%Y:%m:%d %H:%M:%S')

		# print(file_path, date_created)
		files[str(date_created) + ' -- ' + file_path] = file_path

		update_progress()



sorted_files = OrderedDict(sorted(files.items()))
print ('/')

def process_files(sorted_files, dryrun):

	last_yyyy = ''
	last_mm   = ''
	last_dd   = ''

	day_count = 0

	for key, file_path in sorted_files.items():

		yyyy = key[ 0: 4]
		mm   = key[ 5: 7]
		dd   = key[ 8:10]

		if last_yyyy != '' and last_yyyy != yyyy:
			sys.exit('other year detected ' + file_path)
		if last_mm != '' and last_mm != mm:
		 	sys.exit('other month detected ' + file_path)

		if last_dd == '' or last_dd != dd:
			day_count = 0

		file_type = os.path.splitext(file_path)[1]
		new_name = (yyyy + mm + dd + '-' + str(day_count).zfill(4) + file_type).lower()

		if dryrun == False:
			print('rename', file_path, '->', new_name)
			os.rename(file_path, new_name)
			#sleep(0.1)
		else:
			print('dryrun', file_path, '->', new_name)

		last_yyyy = yyyy
		last_mm   = mm
		last_dd   = dd

		day_count += 1

	# os.rename(file_path, newname)

print()
process_files(sorted_files, True)

print()
input('press [enter] to apply these changes')
process_files(sorted_files, False)
