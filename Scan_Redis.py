import redis
import logging
import time

# Open handle for redis-server installed at CentOS VM using redis python package.
redis_server = '10.0.0.141'
redis_port = '6379'
r = redis.Redis(
    host=redis_server,
    port=redis_port)

# To use logging module to log Info/Debug message properly
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('import_insert_redis')

# To calculate total time to get required result.
t = time.process_time()

# Look for Govt building at 5100 E WINNEMUCCA BLVD and find out how many parking spaces are there. EndUser doesn't know
# Location Code(Hash) for building located at 5100 E WINNEMUCCA BLVD . To get required information,I need to scan
# through each hashkey,need to get value of Bldg_Address1, need to compare it to required address.
# Once we find haskey, we can find,available parking lots can be founded  using recently founded hashkey.
scan_address = '5100 E WINNEMUCCA BLVD'
for keyvalue in r.scan_iter():
    # Since I am using redisearch,redis-server also contains Index created by RedisSearch.
    # RedisSearch internally uses hash in addition to various another datatypes to store indexed information.
    # Don't scan them to get required value.
    # Also store ScanTime result in redis hash so it can be plotted later.
    logger.debug(keyvalue)
    logger.debug(r.type(keyvalue))
    if r.type(keyvalue) is not None:
        if r.type(keyvalue).decode("utf-8") == 'hash' and keyvalue.decode("utf-8") != 'Execuation_Time':
            compare_1 = ''
            compare_2 = ''
            bldg_address1 = r.hget(keyvalue, 'Bldg_Address1')
            bldg_address = bldg_address1.decode("utf-8")
            logger.debug(f'{keyvalue}:{bldg_address1}')
            compare_1 = ''.join(sorted(scan_address)).strip()
            compare_2 = ''.join(sorted(bldg_address)).strip()
            logger.debug(f'compare_1:{compare_1}\tcompare_2:{compare_2}')
            if ''.join(sorted(scan_address)).strip() == ''.join(sorted(bldg_address)).strip():
                logger.info(f'FOUND.Building location for requested address {keyvalue}')
                total_parking_spaces = r.hget(keyvalue, 'Total_Parking_Spaces')
                logger.info(f' Total Number of Parking Spaces at {scan_address}:{total_parking_spaces}')
                found_locationcode = keyvalue
        # Not exiting loop to make sure there isn't any another keyvalue containing requested address.

elapsed_time = time.process_time() - t
logger.info(f'It took {elapsed_time} seconds to find {scan_address} from redis using SCAN.'
            f'LocationCode for same {found_locationcode}.'
            f'Total Number of parking spaces for same {total_parking_spaces} ')

# Storing Execution Time in Redis so it can be plotted later on.
scan_execution_time = {'SCAN': elapsed_time}
r.hmset('Execuation_Time', scan_execution_time)
