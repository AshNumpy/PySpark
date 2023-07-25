import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from collections import namedtuple

TagCount = namedtuple('TagCount', ("tag", "count"))

def extractTweetText(tweetJson):
    if 'text' in tweetJson:
        return tweetJson['text']
    else:
        return ''


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: trending_tags.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="StreamingTwitterHashtags")
    window = 10
    ssc = StreamingContext(sc, window)

    tweetsDStream = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    (tweetsDStream
        .map(lambda tweet: extractTweetText(json.loads(tweet))) # extract tweet text
        .flatMap(lambda text: text.split(" ")) # split tweet text into words
        .filter(lambda word: word.lower().startswith("#")) # filter words that start with #
        .map(lambda word: (word.lower(), 1)) # emit tuple (word, 1)
        .reduceByKey(lambda a, b: a + b) # count the number of times each word occurs
        .map(lambda rec: TagCount(rec[1], rec[0])) # map to (count, hashtag)
        .foreachRDD(lambda rdd: rdd.toDF().registerTempTable("tag_counts")) # for each RDD, print top 10 hashtags
    )

    ssc.start()

    sqlContext = SQLContext(sc)

    count = 0
    while count < 100:
        time.sleep(window)
        count += 1
        top_10_hashtags = sqlContext.sql("select tag, count from tag_counts order by count desc limit 10")
        for row in top_10_hashtags.collect():
            print(row.tag, row['count'])
        print("=============================================")

    ssc.awaitTermination()

