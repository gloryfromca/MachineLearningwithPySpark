from pyspark.streaming import StreamingContext
from pyspark import SparkContext
#initialize sc,then initialize ssc
sc = SparkContext(appName="SimpleStreaming")
sc_stream = StreamingContext(sc, batchDuration=5)# 5 seconds window 
# create socketTextStream
purchases = sc_stream.socketTextStream('localhost', 7778, )
# create actions
purchases.map(lambda x:tuple(x.split(','))).pprint()
# start sc_stream
sc_stream.start()
sc_stream.awaitTermination()


