from typing import Optional

from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import os
import shutil
import glob
import math
import numpy as np

from sentence_transformers import SentenceTransformer
from collections import OrderedDict 
from itertools import islice
import json


from utils import helper_functions 
environment_type = os.getenv('SEARCH-HANDLER-ENV')
# init app
app = FastAPI()

# add cors origins rules 
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# download the information retrieval model trained on MS-MARCO dataset
if environment_type == "DEV":
    model_path= './models/distilroberta-base-msmarco-v2'
if environment_type == "PROD":
    model_path= 'distilroberta-base-msmarco-v2'
embedder = SentenceTransformer(model_path)

# create temp dir for storing embeddings 
dir = 'tmp'
                                 

class Payload(BaseModel):
    file_id: str
    query: str
    maxResults: int
    accuracyGreaterThan: float

@app.post("/search")
def predict( payload: Payload):
    # extract values from the request payload
    
    # sess stores a file's uuid
    # a unique identifier to link to an uploaded file's text file, encodings and top words

    sess = payload.file_id
    query = payload.query
    max_results =  payload.maxResults
    acc_greater_than  = payload.accuracyGreaterThan

    print(sess, query, max_results, acc_greater_than)
    
    # check if the files for the corresponding file id are present on the local disk or not
    # return 0 if there's no folder present for the file
    sess_dir_find = glob.glob('tmp/'+sess)
    new_disk_sess = True if len(sess_dir_find)==0 else False

    if new_disk_sess:
        # create new cache disk session direct
        helper_functions.download_text_file_and_embeddings_from_s3_bucket(sess)
        corpus, corpus_embeddings = helper_functions.load_text_file_and_embeddings(sess)

    else:
        # accessing from already downloaded encodings and files from disk
        print('😉 got you\'ve covered, model alread encoded 🤘')
        corpus, corpus_embeddings = helper_functions.load_text_file_and_embeddings(sess)


    queries = [str(query)]
    

    query_embeddings = embedder.encode(queries)

    queries_and_embeddings=(queries, query_embeddings)
    corpus_and_embeddings=(corpus, corpus_embeddings)

    response = helper_functions.cluster(corpus_and_embeddings, queries_and_embeddings, max_results, acc_greater_than)
    
    return response

@app.get("/")
async def root():
    return {"message": "Hello seekers"}