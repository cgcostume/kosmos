import argparse
import pathlib
from tqdm import tqdm

import os
import glob
import re
import sys

import exifread
import datetime
import pytz

from tzlocal import get_localzone
from time import sleep
from collections import OrderedDict
from win32com.propsys import propsys, pscon


image_globex = [ 'jpg', 'srw', 'jpeg', 'png' ]
video_globex = [ 'mp4', 'mov', 'avi' ]

parser = argparse.ArgumentParser(description='Renames all images and videos within the given directories to yyyymmdd_hhmmss.<ext>')
parser.add_argument('path', type = pathlib.Path, help = 'Directories to rename images and videos in.', default = r'.', nargs = '+')
parser.add_argument('-e', '--extension', help = 'Extension(s) to look out for (default: %(default)s).', default = image_globex + video_globex, nargs = '+')
parser.add_argument('-r', '--recursive', help = 'Include subdirectories', action = 'store_true')
args = parser.parse_args()


image_files = glob.glob('')

print(args.extension)

for path in args.path:
	for ext in args.extension:
		if ext in image_files:
			image_files.extend(glob.glob('*.' + ext, root_dir = path, recursive = args.recursive))

print(image_files)

date_taken_tags = [ 'EXIF DateTimeOriginal', 'DateTimeOriginal', 'EXIF DateTimeDigitized', 'DateTimeDigitized', 'EXIF DateTime', 'DateTime' ]
# for g in exif_globex:

# 	print (g)

# 	for file_path in glob.glob(g):
# 		if not os.path.isfile(file_path):
# 			continue
		
# 		file = open(file_path, 'rb')
# 		tags = exifread.process_file(file)
# 		date_taken = None
# 		for tag in date_taken_tags:
# 			if tag in tags:
# 				date_taken = tags[tag]
# 				break
		
# 		if date_taken == None:
# 			date_taken = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
# 			date_taken = date_taken.strftime('%Y:%m:%d %H:%M:%S')
# 			print('cannot read date_taken from exif, used file creation time instead: ' + str(file_path))

# 		file.close()

# 		# print(date_taken)
# 		files[str(date_taken) + ' -- ' + file_path] = file_path

# 		update_progress()

# globex = [ '*.mp4', '*.mov', '*.avi' ]

# for g in globex:
# 	for file_path in glob.glob(g):
# 		if not os.path.isfile(file_path):
# 			continue
# 		properties = propsys.SHGetPropertyStoreFromParsingName(os.path.abspath(file_path))
# 		date_created = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()

# 		if not isinstance(date_created, datetime.datetime):
# 			date_created = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
# 			date_created = date_created.strftime('%Y:%m:%d %H:%M:%S')	
# 		else:
# 			date_created = date_created.astimezone(get_localzone())

# 		# print(file_path, date_created)
# 		files[str(date_created) + ' -- ' + file_path] = file_path

# 		update_progress()



# sorted_files = OrderedDict(sorted(files.items()))

# print ('/')

# def process_files(sorted_files):

# 	scheduled_renames = { }

# 	last_yyyy = ''
# 	last_mm   = ''
# 	last_ts   = ''

# 	ts_count = 0

# 	for key, file_path in sorted_files.items():

# 		yyyy = key[ 0: 4]
# 		mm   = key[ 5: 7]
# 		dd   = key[ 8:10]
# 		H    = key[11:13]
# 		M    = key[14:16]
# 		S    = key[17:19]

# 		if last_yyyy != '' and last_yyyy != yyyy:
# 			sys.exit('other year detected ' + file_path)
# 		#if last_mm != '' and last_mm != mm:
# 		 #	sys.exit('other month detected ' + file_path)

# 		if last_ts == '' or last_ts != key:
# 			ts_count = 0

# 		file_type = os.path.splitext(file_path)[1]

# 		while True:		
# 			new_name = yyyy + mm + dd + '_' + H + M + S
# 			if ts_count > 0:
# 				new_name = new_name + '_' + str(ts_count).zfill(2)
# 			new_name = (new_name + file_type).lower()

# 			if new_name not in scheduled_renames.values():
# 				break
# 			ts_count += 1

# 		print('scheduled', file_path, '->', new_name)
# 		scheduled_renames[file_path] = new_name

# 		last_yyyy = yyyy
# 		last_mm   = mm
# 		last_ts   = key

# 		ts_count += 1

# 	return scheduled_renames

# 	# os.rename(file_path, newname)


# def rename_files(scheduled_renames):
	
# 	for file_path, new_name in reversed(scheduled_renames.items()):
# 		if file_path == new_name:
# 			continue

# 		print('rename', file_path, '->', new_name)
# 		os.rename(file_path, new_name)
# 		#sleep(0.1)


# print()
# scheduled_renames = process_files(sorted_files)

# print()
# input('press [enter] to apply these changes')

# rename_files(scheduled_renames)
