# RedisSearchDemo
Sample Python Scripts to demonstrate RedisSearch can be used to add secondary indexes on top of Redis Data.

Execution Steps For Demo

1) Once Redis and RedisSearch is  installed , modify redis_server value in each script with your redis server hostname/ip address.
   
2) Install below packages using PIP
    - redis
    - redissearch
    - six
    - mplotb
3) Run Import_Insert_Redis.py to load dataset into redis.
4) Run Scan_Redis.py to scan all hashes and look for number of parking lots at address 5100 E WINNEMUCCA BLVD.
   This script also stores execution time. It can be plot latter.
5) Run index_existing_hash.py to index existing redis data using RedisSearch api.
6) Run Search_Redis.py to look for number of available parking lots at address 5100 E WINNEMUCCA BLVD.
   This script also stores execution time. It can be plot latter.
7) Run VisualizationCode/Plot_Result.py to plot graph.
8) FlushDB existing redisdata using RedisCli.
9) Run import_insert_index_redis.py. This script will load data to redis as well as index it using RedisSearch api.
