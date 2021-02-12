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

from utils import helper_functions, redis_cache_mechanisms


class PythonPredictor:

    def __init__(self, config):

        # download the information retrieval model trained on MS-MARCO dataset
        self.embedder = SentenceTransformer('distilroberta-base-msmarco-v2')
        
        # set the environment variables
        self.redis_host = os.getenv('REDIS_HOST')
        self.redis_port = os.getenv('REDIS_PORT')
        self.redis_passkey = os.getenv('REDIS_PASSKEY')

        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        # establish connection with s3 bucket
        self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id , aws_secret_access_key=self.aws_secret_access_key)


        # establish connection to redis server to be used as data store persistence

        try:
            self.r = redis.StrictRedis(host=self.redis_host, port=self.redis_port, password=self.redis_passkey, decode_responses=True)
            
            self.r.ping()
            print('Connected to redis cache!')
        except Exception as ex:
            print('\n\nredis client error:', ex)
            exit('Failed to connect to redis, terminating.')

        
        self.dir = 'tmp'

        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(self.dir)                                           



    def predict(self, payload):
        
        # extract values from the request payload
        
        # sess stores a file's uuid
        # a unique identifier to link to an uploaded file's text file, encodings and top words
        sess = payload["uuid"]
     
        query = payload["text"]

        max_results =  payload["top"]
        
        acc_greater_than  = payload["accuracyGreaterThan"]
        
        cache_bool_value = redis_cache_mechanisms.check_if_request_to_be_cached(self, sess, query, max_results)
                
        if cache_bool_value:
            
            # as caching has to be done we request for 50 more lines and cache them
            # however we return the exact requested amount of lines to the client
            max_results+=50
            
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
            
            redis_cache_mechanisms.cache_response_to_redis(self, sess, query, response)

            response = OrderedDict(islice(response.items(), 0, payload['top']))
            
            return response

        else:

            # return from redis cache!

            print('file available in redis cache! 😇')

            response_cache = redis_cache_mechanisms.get_cache_data_from_redis(self, sess, query, max_results)
            
            return response_cache

