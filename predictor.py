# this is an example for cortex release 0.21 and may not deploy correctly on other releases of cortex
import os
import shutil
import glob
import math
import numpy as np
import scipy.spatial
from sklearn.cluster import KMeans
from sentence_transformers import SentenceTransformer
from collections import OrderedDict 
from itertools import islice
import json

import boto3

import redis

from utils import helper_functions 


class PythonPredictor:

    def __init__(self, config):

        # download the information retrieval model trained on MS-MARCO dataset
        self.embedder = SentenceTransformer('distilroberta-base-msmarco-v2')
    
        
        # set the environment variables
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        # establish connection with s3 bucket
        
        try:  
            self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id , aws_secret_access_key=self.aws_secret_access_key)
            print('Connected to s3 bucket!')
        except Exception as ex:
            print('\n\naws client error:', ex)
            exit('Failed to connect to s3 bucket, terminating.')
        
        # create temp dir for storing embeddings 
        self.dir = 'tmp'

        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(self.dir)                                           


    def predict(self, payload):
        
        # extract values from the request payload
        
        # sess stores a file's uuid
        # a unique identifier to link to an uploaded file's text file, encodings and top words
        sess = payload["uuid"]
    
        query = payload["query"]

        max_results =  payload["maxResults"]
        
        acc_greater_than  = payload["accuracyGreaterThan"]
        
        # cache_bool_value = iredis_cache_mechanisms.check_if_request_to_be_cached(self, sess, query, max_results)
        
        #print('are we caching the values:', cache_bool_value)
        
        # check if the files for the corresponding file id are present on the local disk or not
        # return 0 if there's no folder present for the file
        sess_dir_find = glob.glob('tmp/'+sess)
        new_disk_sess = True if len(sess_dir_find)==0 else False

        if new_disk_sess:
            # create new cache disk session direct

            helper_functions.download_text_file_and_embeddings_from_s3_bucket(self, sess)

            corpus, corpus_embeddings = helper_functions.load_text_file_and_embeddings(self, sess)

        else:


            # accessing from already downloaded encodings and files from disk

            print('😉 got you\'ve covered, model alread encoded 🤘')

            corpus, corpus_embeddings = helper_functions.load_text_file_and_embeddings(self, sess)


        queries = [str(query)]
        

        query_embeddings = self.embedder.encode(queries)

        queries_and_embeddings=(queries, query_embeddings)
        corpus_and_embeddings=(corpus, corpus_embeddings)

        response = helper_functions.cluster(self, corpus_and_embeddings, queries_and_embeddings, max_results, acc_greater_than)
        
        return response
