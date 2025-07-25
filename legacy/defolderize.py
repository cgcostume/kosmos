import os
import glob
import re


# from PIL import Image
# def date_taken(path):
    # return Image.open(path)._getexif()[36867]

globex = [ '*.mp4', '*.jpg', '*.srw', '*.jpeg', '*.srw' ]
year   = '2015'
regex  = re.compile('.{3}-(\d\d)-(\d\d)')

for path in glob.glob('*'):
	
	if not os.path.isdir(path):
		continue

	for g in globex:
		for file_path in glob.glob(os.path.join(path, g)):
			if not os.path.isfile(file_path):
				continue

			dirname  = os.path.dirname (file_path)		
			basename = os.path.basename(file_path)

			matches  = regex.findall(dirname)
			month    = matches[0][0]
			day      = matches[0][1]
			
			# newname  = os.path.join(dirname, year + month + day + basename)
			newname  = year + month + day + basename

			print(file_path, ' -> ', newname)
			os.rename(file_path, newname)
