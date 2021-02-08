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
    
        self.embedder = SentenceTransformer('distilroberta-base-msmarco-v2')

        self.redis_host = os.getenv('REDIS_HOST')
        self.redis_port = os.getenv('REDIS_PORT')
        self.redis_passkey = os.getenv('REDIS_PASSKEY')

        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
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
    
  


    def payload_text_preprocess(self, text):
        text = text.replace('\n', ' ')
        text = text.split('.')
        text = [x for x in text if len(x) >=50]

        return text



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



    def predict(self, payload):
        
        # payload treatment
        
 
        sess = payload["uuid"]
     
        query = payload["text"]

        max_results =  payload["top"]
        
        acc_greater_than  = payload["accuracyGreaterThan"]
        
        # if uuid and query, not cached in redis then download files/load from disk
        # or if requested number of lines is greater than what's cached, then update cahce
        
        if self.r.sismember('uuid:'+sess+':queries', str(query)) is False or max_results>self.r.zcard('match_lines_sorted_set:'+sess+':'+str(query)):
            
            max_results+=50
            sess_find = glob.glob('tmp/'+sess)
            new_session_bool = True if len(sess_find)==0 else False

            if new_session_bool:
                # create new cache disk session directory

                os.mkdir('tmp/'+ sess)

                with open('tmp/'+sess+'/corpus_encode.npy', 'wb') as f:
                    self.s3.download_fileobj('readneedobjects', 'v2/'+sess+'/corpus_encode.npy', f)


                print('downloading encoded weights 👣')



                self.s3.download_file('readneedobjects', 'v2/'+sess+'/text_content.txt', 'tmp/'+sess+'/text_content.txt')

                with open('tmp/'+sess+'/text_content.txt', 'r') as file:
                    file_list = file.read()



                corpus = self.payload_text_preprocess(file_list)



                load_path = os.path.join('tmp', sess, 'corpus_encode.npy')

                corpus_embeddings = np.load(load_path, allow_pickle=True)

                #print('loaded corpus:', corpus_embeddings)



            else:

                # disk cache here

                print('😉 got you\'ve covered, model alread encoded 🤘')

                with open('tmp/'+sess+'/text_content.txt', 'r') as file:
                    file_list = file.read()

                corpus = self.payload_text_preprocess(file_list)

                load_path = os.path.join('tmp', sess, 'corpus_encode.npy')

                corpus_embeddings = np.load(load_path, allow_pickle=True)

                #print('loaded corpus:', corpus_embeddings)

                # load corpus encoded values




            queries = [str(query)]
            

            query_embeddings = self.embedder.encode(queries)

            queries_and_embeddings=(queries, query_embeddings)
            corpus_and_embeddings=(corpus, corpus_embeddings)

            response = self.cluster(corpus_and_embeddings, queries_and_embeddings, max_results, acc_greater_than)
            
            pipe = self.r.pipeline()
            
            pipe.sadd('files:uuids', sess)
            pipe.sadd('uuid:'+sess+':queries', str(query))
            
            pipe.zadd('match_lines_sorted_set:'+sess+':'+str(query), response, 'nx' )
            

            pipe.execute()

            response = OrderedDict(islice(response.items(), 0, payload['top']))
            
            return response

        else:

            # return from redis cache!

            print('file available in redis cache! 😇')

            response_cache = self.r.zrevrange('match_lines_sorted_set:'+sess+':'+str(query), 0, max_results, withscores=True)

            return json.dumps(OrderedDict(response_cache))

