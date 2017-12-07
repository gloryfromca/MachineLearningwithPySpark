import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

sc = SparkContext(appName="SparkKinesisApp")
ssc = StreamingContext(sc, 1)
lines = KinesisUtils.createStream(ssc, "SparkKinesisApp", "myStream","[https://kinesis.us-east-1.amazonaws.com","us-east-1", InitialPositionInStream.LATEST, 2)

#lines.saveAsTextFiles('/home/zh/streaming_logsout.txt')
lines.pprint()
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word,
1)).reduceByKey(lambda a, b: a + b)
counts.pprint()
ssc.start()
ssc.awaitTermination()
