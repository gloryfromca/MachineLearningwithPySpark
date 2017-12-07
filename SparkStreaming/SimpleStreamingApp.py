import sys
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
#initialize sc,then initialize ssc
sc = SparkContext(appName="SimpleStreaming")
ssc = StreamingContext(sc, batchDuration=5)# 5 seconds window 
# create socketTextStream
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
# create actions
counts = lines.map(lambda x:x)
counts.pprint()
# start sc_stream
ssc.start()
ssc.awaitTermination()


