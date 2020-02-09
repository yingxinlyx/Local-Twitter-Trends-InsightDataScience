import findspark
findspark.init("/usr/local/spark")
findspark.find()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import re
import pandas as pd
import psycopg2

# location list for the US
df_loc = pd.read_csv('/home/ubuntu/uscities.csv')[[
    'city', 'state_id', 'state_name'
]]
d = {}
for index, row in df_loc.iterrows():
    if row['state_name'] not in d:
        d[row['state_name']] = row['state_id']


def match_loc(tweet):
    location = 'N/A'
    locs = tweet['user']['location'].split(',')
    if len(locs) == 2:
        loc1 = locs[0].strip().lower().capitalize()
        loc2 = locs[1].strip().upper()
        if loc2 in d.values():
            location = loc2
        elif loc2 == 'USA' and loc1 in d:
            location = d[loc1]
    return [
        tweet['created_at'], tweet['user']['id'], location,
        re.findall('#\w+', tweet['text']) + re.findall('@\w+', tweet['text'])
    ]


def topicMapper(tweet):
    res = []
    for ele in tweet[3]:
        res.append(((tweet[2], ele), 1))
    return res


def saveTopic(time, rdd):
    def savePartition(iter):
        conn = psycopg2.connect(database='******',
                                user='******',
                                password='******',
                                host='10.0.0.6',
                                port='5432')
        cur = conn.cursor()

        for record in iter:
            qry = "INSERT INTO twitter_trend (timestamp, state, tag, cnt) VALUES ('%s','%s','%s',%s);" % (time, record[0], record[1], record[2])
            cur.execute(qry)
        
        conn.commit()
        cur.close()
        conn.close()

    rdd.foreachPartition(savePartition)


def saveCount(time, rdd):
    def saveEachPartition(iter):
        conn = psycopg2.connect(database='******',
                                user='******',
                                password='******',
                                host='10.0.0.6',
                                port='5432')
        cur = conn.cursor()

        for record in iter:
            qry = "INSERT INTO activity (timestamp, state, cnt) VALUES ('%s','%s',%s);" % (time, record[0], record[1])
            cur.execute(qry)
        
        conn.commit()
        cur.close()
        conn.close()

    rdd.foreachPartition(saveEachPartition)


bootstrap_servers = '10.0.0.10:9092'
topic = 'test'

sc = SparkContext()
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 1)
ssc.checkpoint('/home/ubuntu/checkpoint')

# create a stream reading data from kafka
kvs = KafkaUtils.createDirectStream(
    ssc, [topic], {"metadata.broker.list": bootstrap_servers})

# get tweets with locations in the US
tweets = kvs.map(lambda x: json.loads(x[1]))\
    .filter(lambda x: 'created_at' in x and not x['user']['location'] is None)\
    .map(match_loc)\
    .filter(lambda x: x[2] != 'N/A')

# get the number of tweet by state
count = tweets.map(lambda x: (x[2], 1))\
    .reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b, 600, 60)\
    .filter(lambda x: x[1] > 0)

# get topic count by state and topic
topics = tweets.filter(lambda x: len(x[3]) != 0)\
    .flatMap(topicMapper)\
    .reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b, 600, 60)\
    .filter(lambda x: x[1] > 0)\
    .map(lambda x: [x[0][0], x[0][1], x[1]])

# save to db
count.foreachRDD(saveCount)
topics.foreachRDD(saveTopic)

# print to console
count.pprint(20)
topics.pprint(20)

ssc.start()
ssc.awaitTermination()
ssc.stop()
