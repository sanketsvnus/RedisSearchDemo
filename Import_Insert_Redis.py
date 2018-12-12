import redis
import csv
import logging


# Open handle for redis-server installed at CentOS VM using redis python package.
redis_server = '10.0.0.141'
redis_port = '6379'
r = redis.Redis(
    host=redis_server,
    port=redis_port)

# To use logging module to log Info/Debug message properly
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('import_insert_redis')

# Read CSV file. Loop through each line.
# Convert each line into mapping dictionary(Key-Value) pair.
# Insert it to Redis using Location Code as Hash Key.
# Log proper Info/Debug messages.
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
            mapping = {}
            #mapping['Location_Code']=row[0]
            mapping['Region_Code'] = row[1]
            mapping['Bldg_Address1'] = row[2]
            mapping['Bldg_City'] = row[4]
            mapping['Bldg_County'] = row[5]
            mapping['Bldg_State'] = row[6]
            mapping['Bldg_Zip'] = row[7]
            mapping['Congressional_District'] = row[8]
            mapping['Bldg_Status']=row[9]
            mapping['Property_Type'] = row[10]
            mapping['Bldg_ANSI_Usable'] = row[11]
            mapping['Total_Parking_Spaces'] = row[12]
            mapping['Owned_Leased'] = row[13]
            r.hmset(row[0],mapping)

            line_count += 1
    logger.info(f'Processed {line_count} lines.')