import os
import numpy as np
import scipy.spatial
from collections import OrderedDict 


# this function accepts a string
# replaces newlines with ' ' empty space
# splits all lines on the basis of '.'
# returns a list of sentences which have >= to 50 characters

def payload_text_preprocess(text):
    text = text.replace('\n', ' ')
    text = text.split('.')
    text = [x for x in text if len(x) >=50]

    return text



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
    print('wotihin load_text_file_and_embeddings')
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

