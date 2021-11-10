import os
import shutil
import numpy as np
import scipy.spatial
from collections import OrderedDict 

import boto3

# set the environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region_name = os.getenv('AWS_REGION_NAME')


# establish connection with s3 bucket
try:  
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id , 
    aws_secret_access_key=aws_secret_access_key, 
    region_name=aws_region_name)

    print('Connected to s3 bucket!')
except Exception as ex:
    print('\n\naws client error:', ex)
    exit('Failed to connect to s3 bucket, terminating.')


# this function accepts a string
# replaces newlines with ' ' empty space
# splits all lines on the basis of '.'
# returns a list of sentences which have >= to 50 characters

def payload_text_preprocess(text):
    text = text.replace('\n', ' ')
    text = text.split('.')
    text = [x for x in text if len(x) >=50]

    return text


def download_text_file_and_embeddings_from_s3_bucket(sess):
    os.mkdir('tmp/'+ sess)
    
    # download the corpus encodings for the given uuid file
    with open('tmp/'+sess+'/corpus_encode.npy', 'wb') as f:
        s3.download_fileobj('readneedobjects', 'v2/'+sess+'/corpus_encode.npy', f)

    print('\ndownloading encoded weights 👣')

    # download the text content of the given file
    # used for generating lines after running the cosine similiarity match after the clustering is done
    s3.download_file('readneedobjects', 'v2/'+sess+'/file.txt', 'tmp/'+sess+'/file.txt')
    
    print('files download complete!')


def load_text_file_and_embeddings(sess):

    try:
        with open('tmp/'+sess+'/file.txt', 'r') as file:
            file_string = file.read()
    except Exception as e:
        print('text and encoding files for '+sess+' not loaded because error:\n ', e)
        print('\nretrying to download file one more time')
        print('deleting previos corrupt downloaded files from folder ', 'tmp/', sess)
        
        if os.path.exists('tmp/'+sess):
            shutil.rmtree('tmp/'+sess)
        os.makedirs(dir)       

        download_text_file_and_embeddings_from_s3_bucket(sess)
        print('trying to load text file for', sess, ' once again')
        
        with open('tmp/'+sess+'/file.txt', 'r') as file:
            file_string = file.read()

        print('text files for '+sess+' loaded succesfully')

    else:
        print('text files for '+sess+' loaded succesfully')


    # make the text file ready for passing to encoder as a list of strings
    corpus = payload_text_preprocess(file_string)

    load_path = os.path.join('tmp', sess, 'corpus_encode.npy')

    # load corpus encoded values
    corpus_embeddings = np.load(load_path, allow_pickle=True)

    return corpus, corpus_embeddings



# return matching lines by using cosine similiariy 
# givev the corpus c, queries c, number of results
# required by passisng values to max_resutls, acc_thresh filters out
# lines that are below the passed confidence level

def cluster(c, q, max_results, acc_thresh=0.5):

    queries, query_embeddings = q
    corpus, corpus_embeddings = c

    closest_n = max_results

    similiar_results= []

    for query, query_embedding in zip(queries, query_embeddings):
        distances = scipy.spatial.distance.cdist([query_embedding], corpus_embeddings, "cosine")[0]

        # print(query_embedding.shape, corpus_embeddings.shape)

        results = zip(range(len(distances)), distances)
        results = sorted(results, key=lambda x: x[1])



        for idx, distance in results[0:closest_n]:
        

            if (1-distance) > acc_thresh:
                # print(corpus[idx].strip(), "(Score: %.4f)" % (1-distance))
                similiar_results.append({"line": corpus[idx].strip(), "score": "%.4f" % (1-distance), "index" : idx})

    return similiar_results

