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


folders = glob.glob('')

pbar = tqdm(leave = True)
pbar.reset(total = len(args.path))
pbar.set_description('1 of 3 | searching folders  ')


for path in args.path:

	glob_str = os.path.abspath(path) + ('/**/' if args.recursive else '/') + '*'
	glob_str = f'{path}/**/*/'
	folders.extend(glob.glob(glob_str, recursive = args.recursive))
	pbar.update()

pbar.update()
pbar.close()


if len(folders) == 0:
	sys.exit('No folders have been found.')


pbar = tqdm(leave = True)
pbar.reset(total = len(folders))
pbar.set_description('2 of 3 | reading sizes    ')

folders_by_size = { }
scheduled_folders = OrderedDict()

for folder_path in folders:

	size = sum(file.stat().st_size for file in pathlib.Path(folder_path).rglob('*'))
	if size == 0:
		print(size, folder_path)

	pbar.update()


# 	print(folder_path, os.access(folder_path, os.R_OK))

# 	if os.access(folder_path, os.R_OK) == False:
# 		pbar.update()
# 		continue

# 	if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
# 		pbar.update()
# 		continue

# 	if not os.listdir(folder_path):
# 		pbar.update()
# 		continue

# 	size = os.path.getsize(folder_path)

# 	print("NULL", size, folder_path)

# 	if not size in folders_by_size:
# 		folders_by_size[size] = folder_path
# 		pbar.update()
# 		continue

# 	if not size in scheduled_folders:
# 		scheduled_folders[size] = [ folders_by_size[size], folder_path ]
# 	else:
# 		scheduled_folders[size].append(folder_path)

# 	pbar.update()


# pbar.update()
# pbar.close()


# if len(scheduled_folders) == 0:
# 	sys.exit('No folders scheduled for duplicate checking.')


# pbar = tqdm(leave = True)
# pbar.reset(total = len(scheduled_folders))
# pbar.set_description('3 of 3 | computing hashes ')

# duplicates = [ ]

# for size, folder_paths in scheduled_folders.items():
	
# 	hash_first = ''

# 	# for index, folder_path in enumerate(folder_paths):

# 		# hash_md5 = hashlib.md5()
# 		# with open(folder_path, 'rb') as file:
# 		# 	for chunk in iter(lambda: file.read(4096), b''):
# 		# 		hash_md5.update(chunk)

# 		# if index == 0:
# 		# 	hash_first = hash_md5.hexdigest()
# 		# 	continue
		
# 		# if hash_first == hash_md5.hexdigest():
# 		# 	duplicates.append([ folder_paths[0], folder_path ])

# 	pbar.update()


# for folder_paths in duplicates:
# 	print(folder_paths)


# # print()
# # input('press [enter] to apply these changes')

# # print()
# # pbar = tqdm(leave = True)
# # pbar.reset(total = len(scheduled_renames))
# # pbar.set_description('3 of 3 | removing folders  ')

# # rescheduled_renames = OrderedDict()

# # pbar.update()
# # pbar.close()
