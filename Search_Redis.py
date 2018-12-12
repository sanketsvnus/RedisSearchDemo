import redis
import logging
import time
import redisearch

# Open handle for redis-server installed at CentOS Spark VM using Redissearch package.
index_name = 'govbld'
redis_server = '10.0.0.141'
redis_port = '6379'
rs = redisearch.Client(index_name,
                       host=redis_server,
                       port=redis_port)

# Open handle for redis-server installed at CentOS VM using redis package.
r = redis.Redis(
    host=redis_server,
    port=redis_port)

# Get logger to log proper Info/Debug messages.
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('Search_Redis')

# To calculate total time to get required result.
t = time.process_time()

# search same address using redissearch api
search_address = '5100 E WINNEMUCCA BLVD'
try:
    for doc in rs.search(search_address).docs:
        logger.info(f'{doc}')
        found_locationcode = doc.id
        total_parking_spaces = doc.Total_Parking_Spaces
except Exception as err:
    raise err

elapsed_time = time.process_time() - t
logger.info(f'It took {elapsed_time} seconds to find {search_address} from redis using RedisSearch.'
            f'LocationCode for same {found_locationcode}.'
            f'Total Number of parking spaces for same {total_parking_spaces} ')

# Storing Execution Time in Redis so it can be plotted later on.
search_execution_time = {'SEARCH': elapsed_time}

r.hmset('Execuation_Time', search_execution_time)
