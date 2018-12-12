import redis
import logging
import time

t = time.process_time()

# Open handle for redis-server installed at CentOS VM using redis python package.
redis_server = '10.0.0.141'
redis_port = '6379'
r = redis.Redis(
    host=redis_server,
    port=redis_port)

# Get logger to log proper Info/Debug messages.
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('index_existing_hash')

'''
# Copy of mapping to troubleshoot 
mapping['Region_Code'] = row [1]
mapping['Bldg_Address1'] = row[2]
mapping['Bldg_City'] = row[4]
mapping['Bldg_County'] = row[5]
mapping['Bldg_State'] = row[6]
mapping['Bldg_Zip'] = row[7]
mapping['Congressional_District'] = row[8]
mapping['Bldg_Status'] = row[9]
mapping['Property_Type'] = row[10]
mapping['Bldg_ANSI_Usable'] = row[11]
mapping['Total_Parking_Spaces'] = row[12]
mapping['Owned_Leased'] = row[13]

'''

# Creating Index If not exist
index_name = 'govbld'
check_index_cmd = 'FT.INFO' + ' ' + index_name
try:
    response = r.execute_command(check_index_cmd)
except redis.ResponseError as err:
    logger.debug(f'{err}')
    logger.debug(f'Requested Index {index_name} does not exit.Creating one')
    logger.debug(err.args[0])

    if 'Unknown Index name' == err.args[0]:
        logger.debug(f'Index does not exist.Creating RedisSearch {index_name}')
        create_index_cmd = 'FT.CREATE' + ' ' + index_name + ' ' + \
                           'SCHEMA Region_Code NUMERIC Bldg_Address1 TEXT NOSTEM' \
                           'Bldg_City TEXT NOSTEM Bldg_County TEXT NOSTEM Bldg_State TEXT NOSTEM' \
                           'Bldg_Zip NUMERIC Congressional_District TEXT NOSTEM Bldg_Status TEXT NOSTEM' \
                           'Property_Type TEXT NOSTEM Bldg_ANSI_Usable TEXT NOSTEM ' \
                           'Total_Parking_Spaces NUMERIC Owned_Leased TEXT NOSTEM'

        try:
            r.execute_command(create_index_cmd)
            logger.info(f'Requested Index {index_name} got created successfully ')
        except redis.RedisError as err:
            logger.error(f'{err}')
            raise err

# Scan through existing hash, index all fields as per above defined schema using FT.ADDHASH command.
# As of now all items/documents are getting stored with Score 1

for keyvalue in r.scan_iter():
    # redis-server also contains Index created by RedisSearch. RedisSearch internally uses hash in addition to
    # various another datatypes to store indexed information.
    if r.type(keyvalue).decode("utf-8") == 'hash' and keyvalue.decode("utf-8") != 'Execuation_Time':
        try:
            index_existing_hash_cmd = 'FT.ADDHASH' + ' ' + index_name + ' ' + keyvalue.decode('utf-8') + ' ' + '1'
            print(index_existing_hash_cmd)
            r.execute_command(index_existing_hash_cmd)
            logger.info(f'hash {keyvalue} got indexed successfully ')
        except redis.RedisError as err:
            logger.error(f'{err}')
            raise err