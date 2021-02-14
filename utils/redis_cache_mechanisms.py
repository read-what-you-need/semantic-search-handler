import json
from collections import OrderedDict 

import hashlib



def hash_it(string):
    '''
    sha1 hash creates a 40 character string encoded as hexadecimal.
    hexadecimal bit contains 2^4 characters
    so if we slice the sha1 output to first 9 bits.
    Ideally it should map 2^(4*9) number of strings. ~= 6.8*10^10 = 68 billion lines
    For practical cases, taking into consideration the birthday problem and other collision issues,
    A collision can be considered to take place roughly every 2^(4*n*0.5) where n is the number of bits
    '''

    # for our purposes we will keep first 11 bits for line ids, which can map atleast 4 million lines (2^(4*0.5*11))
    # because an average 1MB book contains 6000 strings
    # if we max out the book size to 50MB and containing 300,000 lines, we still have additional 2.7 Million lines to spare

    # for query ids also we will keep first 11 bits, which can map atleast 4 million query. Enough to start with

    return hashlib.sha1(string.encode('utf-8')).hexdigest()


def get_sliced_hash(string):

    # number of bits from sha1 hash to be used for line and query ids
    # chech hash_it function for more details on why we are slicing a part of sha1 hash
    HASH_SHA1_BITS_RETAIN = 11
    

    string_hash = hash_it(string)
    hash_id = string_hash[:HASH_SHA1_BITS_RETAIN]

    return hash_id




# Checks if a request, given sess and query has to be cached or not.
# The function returns a boolean returning True or false depending on
# whether file id that is sess and query are present or not
# if uuid and query are not cached in redis then return true
# if requested number of lines is greater than what's cached, then return true

def check_if_request_to_be_cached(self, sess, query, max_results):

    print('in check for cache or not')

    query_hash_id = get_sliced_hash(query)

    query_id_val = self.r.hget(sess+':query_to_id:'+query_hash_id[:2], query_hash_id[2::])

    # if check_query_exist returns None, it means item was not present
    check_query_exist = self.r.hget(sess+':query_to_id:'+query_hash_id[:2], query_hash_id[2::])


    # check_query_cardinality contains info about the number of lines present in cache for a given query
    check_query_cardinality = self.r.zcard(sess+':query_id:'+str(query_id_val)+':match_lines')
    

    '''
    cache_bool_value is True if either of the two conditions are met
    1. if the query does not exist in the data store
    2. if number of requested lines is greater than already present in cache, for a given query of a file
    '''

    cache_condition_check_query_exist = type(check_query_exist) == type(None)
    cache_condition_check_query_cardinality = max_results > check_query_cardinality

    print('query not present in cache: ', cache_condition_check_query_exist)
    print('number of requested lines exceed the ones in cache: ',  cache_condition_check_query_cardinality)
    
    # cache_bool_value stores information if caching of the query and response has to be performed or not
    cache_bool_value = cache_condition_check_query_exist or cache_condition_check_query_cardinality
    
    return cache_bool_value




'''
1 query with 104 returned lines takes : 30KB
92 hashs with 109 fields (94.85% of keys, avg size 1.18)
2 strings with 325 bytes (02.06% of keys, avg size 162.50)
3 streams with 217 entries (03.09% of keys, avg size 72.33)
'''

def cache_response_to_redis(self, sess, query, response, max_results):
    '''
    sess: type of string
    query: type of string
    response:type of Ordereddict [(line, score)]
    '''
    
    print('hold on tight 🌠 caching query and response to redis')

    # number of bits from sha1 hash to be used for line and query ids
    # chech hash_it function for more details on why we are slicing part of sha1 hash

    APPEND_FROM = 0
    UNIQUE_RESULTS_FOUND=True

    query_hash_id = get_sliced_hash(query)

    # start redis atomic operation
    
    #pipe = self.r.pipeline()
    pipe = self.r


    '''
    pfadd is hyperloglog data structure from redis to check if the 
    item being inserted into the key is unique or not
    If pfadd for the given query returns 1, then it means the field is being inserted for the first time
    ''' 
    unique_query_bool = pipe.pfadd('uuid:'+sess+':queries', query_hash_id)
    # give unique id to unique query

    
    if unique_query_bool == 1:

         # create new query id
        query_id_val = pipe.hincrby('unq_ids', sess+':query_ids', 1)


        # create a map between query to query id
        '''
        10 bits hash can map to around 4 million queries
        so we split the 3 million queries into 4096 hashes containing 1000 fields
        each storing a particular query id
        '''
        pipe.hset(sess+':query_to_id:'+query_hash_id[:2], query_hash_id[2::], query_id_val)
        
        # create a map between query_id to string_val
        pipe.hset(sess+':query_id:'+str(query_id_val), 'query', query)

        # add asked global field value to given query
        pipe.hset(sess+':query_id:'+str(query_id_val), 'asked_global', 0)
        
    else:

        # query has already been cached with n elements, 
        # we are appending the cachce stream with new elements

        query_id_val = pipe.hget(sess+':query_to_id:'+query_hash_id[:2], query_hash_id[2::])

        if len(response) > pipe.zcard(sess+':query_id:'+str(query_id_val)+':match_lines'):
            # more results are present
            APPEND_FROM = pipe.zcard(sess+':query_id:'+str(query_id_val)+':match_lines')

        if max_results > len(response):
            UNIQUE_RESULTS_FOUND = False




    if UNIQUE_RESULTS_FOUND:

        for line, score in list(response.items())[APPEND_FROM::]:

            line_hash_id = get_sliced_hash(line)
            
            # give unique id to unique line
            
            unique_line_bool = pipe.pfadd('uuid:'+sess+':lines', line_hash_id)

            if unique_line_bool == 1:

                # generate new line id
                line_id_val = pipe.hincrby('unq_ids', sess+':line_ids', 1)

                # create map between line to line_id_val
                pipe.hset(sess+':line_to_id:'+line_hash_id[:2], line_hash_id[2::], line_id_val)

                # create a map between query_id to string_va
                pipe.hset(sess+':line_id:'+str(line_id_val), 'content', line)

                # add bookmarked global field value to given line
                pipe.hset(sess+':line_id:'+str(line_id_val), 'bookmarked_global', 0)

            else:

                # get line_id_val

                line_id_val = pipe.hget(sess+':line_to_id:'+line_hash_id[:2], line_hash_id[2::])


            pipe.zadd(sess+':query_id:'+str(query_id_val)+':match_lines', {line_id_val: score}, 'nx')
            


    print('response and query cached 🌻')

    


def get_cache_data_from_redis(self, sess, query, max_results):

    query_hash_id = get_sliced_hash(query)

    query_id_val = self.r.hget(sess+':query_to_id:'+query_hash_id[:2], query_hash_id[2::])

    line_id_and_score = self.r.zrevrange(sess+':query_id:'+str(query_id_val)+':match_lines', 0, max_results, withscores=True)

    redis_response = []

    for line_id_val, score in line_id_and_score:
        content = self.r.hget(sess+':line_id:'+str(line_id_val), 'content')
        redis_response.append([content, score])

 
    return json.dumps(OrderedDict(redis_response))
