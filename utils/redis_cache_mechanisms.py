import json
from collections import OrderedDict 

import hashlib




# Checks if a request, given sess and query has to be cached or not.
# The function returns a boolean returning True or false depending on
# whether file id that is sess and query are present or not
# if uuid and query are not cached in redis then return true
# if requested number of lines is greater than what's cached, then return true

def check_if_request_to_be_cached(self, sess, query, max_results):
    print('in check for cache or not')
    cache_condition_chech_query_exist = self.r.sismember('uuid:'+sess+':queries', str(query))
    cache_condition_check_query_cardinality = self.r.zcard('match_lines_sorted_set:'+sess+':'+str(query))
    
    # cache_bool_value stores information if caching of the query and response has to be performed or not
    cache_bool_value = cache_condition_chech_query_exist is False or max_results > cache_condition_check_query_cardinality
    
    return cache_bool_value




def hash_it(string):
    '''
    sha1 hash creates a 40 character string encoded as hexadecimal.
    hexadecimal bit contains 2^4 characters
    so if we slice the sha1 output to first 9 bits.
    Ideally it should map 2^(4*9) number of strings. ~= 6.8*10^10 = 68 billion lines
    For practical cases, taking into consideration the birthday problem and other collision issues,
    A collision can be considered to take place roughly every 2^(4*n*0.5) where n is the number of bits
    '''

    # for our purposes we will keep first 10 bits for line ids, which can map atleast 1,048,576 lines (2^(4*0.5*10))
    # because an average 1MB book contains 6000 strings
    # if we max out the book size to 50MB and containing 300,000 lines, we still have additional 700,000 to spare

    # for query ids we will keep first 11 bits, which can map atleast 4 million query. Enough to start with

    return hashlib.sha1(string.encode('utf-8')).hexdigest()






def cache_response_to_redis(self, sess, query, response):
    '''
    sess: type of string
    query: type of string
    response:type of Ordereddict [(line, score)]
    '''
    
    print('hold on tight 🌠 caching query and response to redis')

    # number of bits from sha1 hash to be used for line and query ids
    QUERY_HASH_SHA1_BITS_RETAIN = 11
    LINE_HASH_SHA1_BITS_RETAIN = 10


    query_hash = hash_it(query)
    query_hash_id = query_hash[:QUERY_HASH_SHA1_BITS_RETAIN]

    # start redis atomic operation
    
    #pipe = self.r.pipeline()
    pipe = self.r


    unique_query_bool = pipe.sadd('uuid:'+sess+':queries', query_hash_id)
    print('\n\nuuid:'+sess+':queries', query_hash_id)
    # give unique id to unique query

    
    if unique_query_bool:
        # create new query id
        query_id_val = pipe.hincrby('unq_ids', sess+':query_ids', 1)
        

        # create a map between query to query id
        '''
        10 bits hash can map to around 4 million queries
        so we split the 3 million queries into 4096 hashes containing 1000 fields
        each storing a particular query id
        '''
        pipe.hset(sess+':query_to_id:'+query_hash_id[:2], query_hash_id[3::], query_id_val)
        

        # create a map between query_id to string_val
        pipe.hset(sess+':query_id:'+str(query_id_val), 'query', query)
        

        # add asked global field value to given query
        pipe.hset(sess+':query_id:'+str(query_id_val), 'asked_global', 0)
        
    else:

        query_id_val = pipe.hget(sess+':query_to_id:'+query_hash_id[:2], query_hash_id[3::])



    for line, score in response.items():


        line_hash = hash_it(line)
        line_hash_id = line_hash[:LINE_HASH_SHA1_BITS_RETAIN]
        
        # give unique id to unique line
        

        
        unique_line_bool = pipe.sadd('uuid:'+sess+':lines', line_hash_id)

        if unique_line_bool:
            
            line_id_val = pipe.hincrby('unq_ids', sess+':line_ids', 1)
                 
            # create map between line to line_id_val
            pipe.hset(sess+':line_to_id:'+line_hash_id[:2], line_hash_id[3::], line_id_val)

            # create map between line_id_val to string_val
            pipe.hset(sess+':line_id:'+str(line_id_val), 'content', line)

            # add bookmarked global field value to given line
            pipe.hset(sess+':line_id:'+str(line_id_val), 'bookmarked_global', 0)

        else:

            # get line_id_val

            line_id_val = pipe.hget(sess+':line_to_id:'+line_hash_id[:2], line_hash_id[3::])

        pipe.zadd(sess+':query_id:'+str(query_id_val)+':match_lines', {line_id_val: score}, 'nx')


    print('response and query cached 🌻')

    


def get_cache_data_from_redis(self, sess, query, max_results):

    redis_response = self.r.zrevrange('match_lines_sorted_set:'+sess+':'+str(query), 0, max_results, withscores=True)

    return json.dumps(OrderedDict(redis_response))
