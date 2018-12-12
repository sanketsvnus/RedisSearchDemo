import redis
import csv
import logging
from redis import WatchError

# Open handle for redis-server installed at CentOS VM using redis python package
redis_server = '10.0.0.141'
redis_port = '6379'
pool = redis.ConnectionPool(host=redis_server, port=redis_port, db=0)
r = redis.Redis(connection_pool=pool)

# To use logging module to log Info/Debug message properly
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('import_insert_redis')

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

# Read CSV file. Loop through each line.
# Convert each line into mapping dictionary(Key-Value) pair.
# Insert it to Redis using Location Code as Hash Key. Index inserted data using ADDHASH command
# Log proper Info/Debug messages.
# Inserting row into Redis hash and creating text based index on top of it gets executed as redis transaction
# to make sure we don't miss indexing any hash.

with open('datagovbldgrexus.csv') as datagovbldgrexus:
    datagovbldgrexus_reader = csv.reader(datagovbldgrexus, delimiter=',')
    line_count = 0
    for row in datagovbldgrexus_reader:
        if line_count == 0:
            logger.info(f'Column names are {", ".join(row)}')
            line_count += 1
        else:
            logger.info(f'Inserting..\tLocation_Code:{row[0]}\tRegion_Code:{row[1]}\tBldg_Address1:{row[2]}'
                        f'\tBldg_City:{row[4]}\tBldg_County:{row[5]}\tBldg_State:{row[6]}\tBldg_Zip:{row[7]}'
                        f'\tCongressional_District:{row[8]}\tBldg_Status:{row[9]}\tProperty_Type:{row[10]}'
                        f'\tBldg_ANSI_Usable:{row[11]}\tTotal_Parking_Spaces:{row[12]}\tOwned_Leased:{row[13]}'
                        f'\tConstruction_Date:{row[14]}')
            mapping = {'Region_Code': row[1], 'Bldg_Address1': row[2], 'Bldg_City': row[4], 'Bldg_County': row[5],
                       'Bldg_State': row[6], 'Bldg_Zip': row[7], 'Congressional_District': row[8],
                       'Bldg_Status': row[9], 'Property_Type': row[10], 'Bldg_ANSI_Usable': row[11],
                       'Total_Parking_Spaces': row[12], 'Owned_Leased': row[13]}
            # mapping['Location_Code']=row[0]
            pipeline = r.pipeline()
            while True:
                try:
                    pipeline.watch(row[0])
                    pipeline.multi()
                    pipeline.hmset(row[0], mapping)
                    index_existing_hash_cmd = 'FT.ADDHASH' + ' ' + index_name + ' ' + row[0] + ' ' + '1'
                    logger.debug(index_existing_hash_cmd)
                    pipeline.execute_command(index_existing_hash_cmd)
                    logger.info(f'hash {row[0]} got indexed successfully ')
                    pipeline.execute()
                    break
                except redis.RedisError as err:
                    logger.error(f'{err}')
                    raise err
                except WatchError:
                    continue
                finally:
                    pipeline.reset()
            line_count += 1
    logger.info(f'Processed {line_count} lines.')
