import findspark
findspark.init("/usr/local/spark")
findspark.find()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import re
import pandas as pd
from psycopg2.pool import ThreadedConnectionPool


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
    return [tweet['created_at'], tweet['user']['id'], location,
            re.findall('#\w+', tweet['text']) + re.findall('@\w+', tweet['text'])]


def topicMapper(tweet):
    res = []
    for ele in tweet[3]:
        res.append(((tweet[2], ele), 1))
    return res


def saveTopic(time, rdd):
    def savePartition(iter):
        pool = ThreadedConnectionPool(1,
                                      5,
                                      database='postgres',
                                      user='db_select',
                                      password='student',
                                      host='10.0.0.6',
                                      port='5432')
        conn = pool.getconn()
        cur = conn.cursor()

        for record in iter:
            qry = "INSERT INTO twitter_trend (timestamp, state, tag, cnt) VALUES ('%s','%s','%s',%s);" % (
                time, record[0][0], record[0][1], record[1])
            cur.execute(qry)

        conn.commit()
        cur.close()
        pool.putconn(conn)

    rdd.foreachPartition(savePartition)


def saveCount(time, rdd):
    def saveEachPartition(iter):
        pool = ThreadedConnectionPool(1,
                                      5,
                                      database='postgres',
                                      user='db_select',
                                      password='student',
                                      host='10.0.0.6',
                                      port='5432')
        conn = pool.getconn()
        cur = conn.cursor()

        for record in iter:
            qry = "INSERT INTO activity (timestamp, state, cnt) VALUES ('%s','%s',%s);" % (
                time, record[0], record[1])
            cur.execute(qry)

        conn.commit()
        cur.close()
        pool.putconn(conn)

    rdd.foreachPartition(saveEachPartition)


# define city dictionary to match
df_loc = pd.read_csv('/home/ubuntu/uscities.csv')[['city', 'state_id', 'state_name']]
d = {}
for index, row in df_loc.iterrows():
    if row['state_name'] not in d:
        d[row['state_name']] = row['state_id']


# set up spark cluster
bootstrap_servers = '******'
topic = '******'
myKeyID = "******"
mySecretKey = "******"

# initial spark context with master node and app name
sc = SparkContext("spark://10.0.0.6:7077", "Twitter Trends")

# set console to show error only
sc.setLogLevel('ERROR')

# config connection to s3
sc._jsc.hadoopConfiguration().set(
    "fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", myKeyID)
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", mySecretKey)

ssc = StreamingContext(sc, 1)
ssc.checkpoint("s3://li-yingxin-checkpoint/")


# read stream from kafka
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": bootstrap_servers})

tweets = kvs.map(lambda x: json.loads(x[1]))\
    .filter(lambda x: 'created_at' in x and not x['user']['location'] is None)\
    .map(match_loc)\
    .filter(lambda x: x[2] != 'N/A')

count = tweets.map(lambda x: (x[2], 1))\
    .reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b, 600, 60)\
    .filter(lambda x: x[1] > 0)

topics = tweets.filter(lambda x: len(x[3]) != 0)\
    .flatMap(topicMapper)\
    .reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b, 600, 60)\
    .filter(lambda x: x[1] > 0)

# save to db
count.foreachRDD(saveCount)
topics.foreachRDD(saveTopic)

count.pprint(20)
topics.pprint(20)

ssc.start()
ssc.awaitTermination()
ssc.stop()
