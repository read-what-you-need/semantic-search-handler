import json
from collections import OrderedDict 

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
