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

        self.demo_titles = ['benj', 'naval', 'think']

        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)
        os.makedirs(self.dir)                                           



    # this function accepts a string
    # replaces newlines with ' ' empty space
    # splits all lines on the basis of '.'
    # returns a list of sentences which have >= to 50 characters

    def payload_text_preprocess(self, text):
        text = text.replace('\n', ' ')
        text = text.split('.')
        text = [x for x in text if len(x) >=50]

        return text


    # return matching lines givev the corpus c, queries c, number of results
    # required by passisng values to max_resutls, acc_thresh filters out
    # lines that are below the passed confidence level

    def cluster(self, c, q, max_results, acc_thresh=0.5):

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
                    print(corpus[idx].strip(), "(Score: %.4f)" % (1-distance))
                    similiar_results.append([corpus[idx].strip(), "%.4f" % (1-distance)])

        return OrderedDict(similiar_results)




    def download_text_file_and_embeddings_from_s3_bucket(self, sess):
        os.mkdir('tmp/'+ sess)
        
        # download the corpus encodings for the given uuid file
        with open('tmp/'+sess+'/corpus_encode.npy', 'wb') as f:
            self.s3.download_fileobj('readneedobjects', 'v2/'+sess+'/corpus_encode.npy', f)


        print('downloading encoded weights 👣')


        # download the text content of the given file
        # used for generating lines after running the cosine similiarity match after the clustering is done
        self.s3.download_file('readneedobjects', 'v2/'+sess+'/text_content.txt', 'tmp/'+sess+'/text_content.txt')
        
        print('dowload complete!')



    def load_text_file_and_embeddings(self, sess):

        with open('tmp/'+sess+'/text_content.txt', 'r') as file:
            file_string = file.read()

        # make the text file ready for passing to encoder as a list of strings
        corpus = self.payload_text_preprocess(file_string)

        load_path = os.path.join('tmp', sess, 'corpus_encode.npy')

        # load corpus encoded values
        corpus_embeddings = np.load(load_path, allow_pickle=True)

        return corpus, corpus_embeddings
    

    # Checks if a request, given sess and query has to be cached or not.
    # The function returns a boolean returning True or false depending on
    # whether file id that is sess and query are present or not
    # if uuid and query are not cached in redis then return true
    # if requested number of lines is greater than what's cached, then return true

    def check_if_request_to_be_cached(self, sess, query, max_results):
        
        cahce_condition_chech_query_exist = self.r.sismember('uuid:'+sess+':queries', str(query))
        cache_condition_check_query_cardinality = self.r.zcard('match_lines_sorted_set:'+sess+':'+str(query))
        
        # cache_bool_value stores information if caching of the query and response has to be performed or not
        cache_bool_value = cahce_condition_chech_query_exist is False or max_results > cache_condition_check_query_cardinality
        
        return cache_bool_value


    
    def cache_response_to_redis(self, sess, query, response):
       
        print('hold on tight 🌠 caching query and response to redis')
        
        pipe = self.r.pipeline()
        
        pipe.sadd('files:uuids', sess)
        pipe.sadd('uuid:'+sess+':queries', str(query))
        
        pipe.zadd('match_lines_sorted_set:'+sess+':'+str(query), response, 'nx' )

        print('response and query cached 🌻')

        pipe.execute()


    def get_cache_data_from_redis(self, sess, query, max_results):

        redis_response = self.r.zrevrange('match_lines_sorted_set:'+sess+':'+str(query), 0, max_results, withscores=True)

        return json.dumps(OrderedDict(redis_response))


    def predict(self, payload):
        
        # extract values from the request payload
        
        # sess stores a file's uuid
        # a unique identifier to link to an uploaded file's text file, encodings and top words
        sess = payload["uuid"]
     
        query = payload["text"]

        max_results =  payload["top"]
        
        acc_greater_than  = payload["accuracyGreaterThan"]
        
        cache_bool_value = self.check_if_request_to_be_cached(sess, query, max_results)
                
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

                self.download_text_file_and_embeddings_from_s3_bucket(sess)

                corpus, corpus_embeddings = self.load_text_file_and_embeddings(sess)

            else:


                # accessing from already downloaded encodings and files from disk

                print('😉 got you\'ve covered, model alread encoded 🤘')

                corpus, corpus_embeddings = self.load_text_file_and_embeddings(sess)


            queries = [str(query)]
            

            query_embeddings = self.embedder.encode(queries)

            queries_and_embeddings=(queries, query_embeddings)
            corpus_and_embeddings=(corpus, corpus_embeddings)

            response = self.cluster(corpus_and_embeddings, queries_and_embeddings, max_results, acc_greater_than)
            
            self.cache_response_to_redis(sess, query, response)

            response = OrderedDict(islice(response.items(), 0, payload['top']))
            
            return response

        else:

            # return from redis cache!

            print('file available in redis cache! 😇')

            response_cache = self.get_cache_data_from_redis(sess, query, max_results)
            
            return response_cache

