import argparse
import pathlib
from tqdm import tqdm

import os
import glob
import sys

import exifread
import datetime

from tzlocal import get_localzone
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

pbar = tqdm(leave = True)
pbar.reset(total = len(args.path) * len(args.extension))
pbar.set_description('1 of 3 | searching files ')


for path in args.path:
	
	for ext in args.extension:
		if ext.lower() in image_globex or ext.lower() in video_globex:
			glob_str = os.path.abspath(path) + ('/**/' if args.recursive else '/') + '*.' + ext
			image_files.extend(glob.glob(glob_str))
			pbar.update()


date_created_tags = [ 'EXIF DateTimeOriginal', 'DateTimeOriginal', 'EXIF DateTimeDigitized', 'DateTimeDigitized', 'EXIF DateTime', 'DateTime' ]

def getDateCreatedFromImage(file_path):
	
	file = open(file_path, 'rb')
	tags = exifread.process_file(file, details = False)
	date_created = None
	for tag in date_created_tags:
		if tag in tags:
			date_created = tags[tag]
			break
	
	if date_created == None:
		date_created = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
		date_created = date_created.strftime('%Y:%m:%d %H:%M:%S')

	file.close()

	return date_created


def getDateCreatedFromVideo(file_path):
	
	properties = propsys.SHGetPropertyStoreFromParsingName(os.path.abspath(file_path))
	date_created = properties.GetValue(pscon.PKEY_Media_DateEncoded).GetValue()

	if not isinstance(date_created, datetime.datetime):
		date_created = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
		date_created = date_created.strftime('%Y:%m:%d %H:%M:%S')	
	else:
		date_created = date_created.astimezone(get_localzone())

	return date_created


pbar.update()
pbar.close()

if len(image_files) == 0:
	sys.exit('No files have been found.')


pbar = tqdm(leave = True)
pbar.reset(total = len(image_files))
pbar.set_description('2 of 3 | reading dates   ')

files = { }

for file_path in image_files:

	if not os.path.isfile(file_path):
		pbar.update()
		continue

	file_ext = os.path.splitext(file_path)[-1][1:].lower()

	date_created = None
	if file_ext in image_globex:
		date_created = getDateCreatedFromImage(file_path)
	elif file_ext in video_globex:
		date_created = getDateCreatedFromVideo(file_path)
	else:
		pbar.update()
		continue

	files[str(date_created) + ' -- ' + file_path] = file_path
	pbar.update()


def scheduleFileRenames(sorted_files):

	scheduled_renames = OrderedDict()
	existing_names = [ ]

	last_yyyy = ''
	last_mm   = ''
	last_ts   = ''

	ts_count = 0

	for key, file_path in sorted_files.items():

		yyyy = key[ 0: 4]
		mm   = key[ 5: 7]
		dd   = key[ 8:10]
		H    = key[11:13]
		M    = key[14:16]
		S    = key[17:19]

		# if args.exit_other_year and last_yyyy != '' and last_yyyy != yyyy:
		# 	sys.exit('other year detected: ' + file_path)
		# if args.exit_other_month and last_mm != '' and last_mm != mm:
		#  	sys.exit('other month detected: ' + file_path)

		if last_ts == '' or last_ts != key:
			ts_count = 0

		file_ext = os.path.splitext(file_path)[-1].lower()

		while True:		
			new_name = yyyy + mm + dd + '_' + H + M + S
			if ts_count > 0:
				new_name = new_name + '_' + str(ts_count).zfill(2)
			new_name = (new_name + file_ext)

			if new_name not in existing_names:
				break
			ts_count += 1

		existing_names.append(new_name)

		# print('scheduled', file_path, '->', new_name)
		dir_name = os.path.dirname(file_path)
		current_name = os.path.basename(file_path)

		if current_name != new_name:
			scheduled_renames[file_path] = [ dir_name, current_name, new_name ]

		last_yyyy = yyyy
		last_mm   = mm
		last_ts   = key

		ts_count += 1

	return scheduled_renames

pbar.update()
pbar.close()

sorted_files = OrderedDict(sorted(files.items()))
scheduled_renames = scheduleFileRenames(sorted_files)
	
if len(scheduled_renames) == 0:
	sys.exit('No files scheduled for renaming.')


print('The following files have been scheduled for renaming:')

last_path = None
for file_path, attributes in reversed(scheduled_renames.items()):
	if last_path == None or last_path != attributes[0]:
		print('#', attributes[0])

	print(' ', attributes[1], '->', attributes[2])
	last_path = attributes[0]


print()
input('press [enter] to apply these changes')

print()
pbar = tqdm(leave = True)
pbar.reset(total = len(scheduled_renames))
pbar.set_description('3 of 3 | renaming files  ')

rescheduled_renames = OrderedDict()

for file_path, attributes in reversed(scheduled_renames.items()):
	new_name = os.path.join(attributes[0], attributes[2])
	if os.path.isfile(new_name):
		rescheduled_renames[file_path] = attributes
		continue

	os.rename(file_path, new_name)
	pbar.update()
	#sleep(0.1)

for file_path, attributes in reversed(rescheduled_renames.items()):
	new_name = os.path.join(attributes[0], attributes[2])
	os.rename(file_path, new_name)
	pbar.update()

pbar.update()
pbar.close()
