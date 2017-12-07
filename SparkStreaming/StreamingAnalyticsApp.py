import sys
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
#initialize sc,then initialize ssc
sc = SparkContext(appName="StreamingAnalysis")
#just print ssc result and error 
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, batchDuration=5)# 5 seconds window 
# create socketTextStream
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
lines = lines.map(lambda x:tuple(x.split(',')))
# create actions

def stream_function_foreachRDD(A_RDD):
    try:
        numPurchases = A_RDD.count()
        uniqueUsers = A_RDD.map(lambda x:x[0]).distinct().count()
        # empty rdd can't reduce, raise error
        totalRevenue = A_RDD.map(lambda x:int(x[2])).reduce(lambda x, y:x+y)
        productByPopularity = A_RDD.map(lambda x:(x[1],1)).reduceByKey(lambda x, y:x+y).collect()
        productByPopularity.sort(key=lambda x:x[1], reverse=True)
        mostPopular = productByPopularity[0]
        print('numPurchases:%s'%numPurchases)
        print('uniqueUsers:%s'%uniqueUsers)
        print('totalRevenue:%s'%totalRevenue)
        print('mostPopular:%s, numMostPopular:%s'%mostPopular)  
    except Exception as e:
        print('something wrong!\n%s'e)
    return
lines.pprint()
lines.foreachRDD(lambda rdd:stream_function_foreachRDD(rdd))
# start sc_stream
ssc.start()
ssc.awaitTermination()


