import argparse
import pathlib
from tqdm import tqdm

import os
import glob
import sys

import hashlib

from collections import OrderedDict


parser = argparse.ArgumentParser(description='Identifies duplicates within the given directories')
parser.add_argument('path', type = pathlib.Path, help = 'Directories to include in duplicate search.', default = r'.', nargs = '+')
parser.add_argument('-r', '--recursive', help = 'Include subdirectories', action = 'store_true')
args = parser.parse_args()


files = glob.glob('')

pbar = tqdm(leave = True)
pbar.reset(total = len(args.path))
pbar.set_description('1 of 3 | searching files  ')


for path in args.path:

	# glob_str = os.path.abspath(path) + ('/**/' if args.recursive else '/') + '*'
	glob_str = '[!.]*.*'
	files.extend(pathlib.Path(path).rglob(glob_str) if args.recursive else pathlib.Path(path).glob(glob_str))
	pbar.update()

pbar.update()
pbar.close()


if len(files) == 0:
	sys.exit('No files have been found.')


pbar = tqdm(leave = True)
pbar.reset(total = len(files))
pbar.set_description('2 of 3 | reading sizes    ')

files_by_size = { }
scheduled_files = OrderedDict()

for file_path in files:

	if not os.path.isfile(file_path):
		pbar.update()
		continue

	size = os.path.getsize(file_path)

	if not size in files_by_size:
		files_by_size[size] = file_path
		pbar.update()
		continue

	if not size in scheduled_files:
		scheduled_files[size] = [ files_by_size[size], file_path ]
	else:
		scheduled_files[size].append(file_path)

	pbar.update()


pbar.update()
pbar.close()


if len(scheduled_files) == 0:
	sys.exit('No files scheduled for duplicate checking.')


pbar = tqdm(leave = True)
pbar.reset(total = len(scheduled_files))
pbar.set_description('3 of 3 | computing hashes ')

duplicates = [ ]

for size, file_paths in scheduled_files.items():
	
	hash_first = ''

	for index, file_path in enumerate(file_paths):

		hash_md5 = hashlib.md5()
		with open(file_path, 'rb') as file:
			for chunk in iter(lambda: file.read(4096), b''):
				hash_md5.update(chunk)

		if index == 0:
			hash_first = hash_md5.hexdigest()
			continue
		
		if hash_first == hash_md5.hexdigest():
			duplicates.append([ file_paths[0], file_path ])

	pbar.update()


for file_paths in duplicates:
	print(file_paths)


# print()
# input('press [enter] to apply these changes')

# print()
# pbar = tqdm(leave = True)
# pbar.reset(total = len(scheduled_renames))
# pbar.set_description('3 of 3 | removing files  ')

# rescheduled_renames = OrderedDict()

# pbar.update()
# pbar.close()
