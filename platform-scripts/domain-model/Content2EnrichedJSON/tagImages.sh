#!/bin/bash




#src=/Library/Python/2.7/site-packages/tensorFlow/models/image/imagenet/classify_image.py

# tensorFlow imagenet src file
srcDir=$1

# asset directory 
imgDir=$2

echo "tensorFlow src code is $1"
echo "images to be classified are in the directory:  $2"
# store the current dir
shopt -s nullglob

cwd=`pwd`
cd $imDir

for f in *.sh
do
	echo "labeling the image in $f"

	#python $src --image_file $f > "$f.txt"
done
# change to caller's directory
cd $cwd