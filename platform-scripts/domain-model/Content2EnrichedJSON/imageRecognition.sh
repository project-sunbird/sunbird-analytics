#Run this to set up tensor flow
export TF_BINARY_URL=https://storage.googleapis.com/tensorflow/mac/tensorflow-0.9.0rc0-py2-none-any.whl
sudo pip install --upgrade $TF_BINARY_URL

#Run this to classify an image
#Add a for loop to loop over all image files in a directory
cd tensorflow/models/image/imagenet
python classify_image.py --image_file "$filename">"$filename.txt" 
