import sys
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

#initialize sc,then initialize ssc
sc = SparkContext(appName="StreamingState")
#just print ssc result and error 
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, batchDuration=5)# batch rdd per 5 seconds 
ssc.checkpoint('hdfs://zh:9000/StreamingState')

# create socketTextStream
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
lines = lines.map(lambda x:tuple(x.split(','))).map(lambda x:(x[0], (x[1], int(x[2]))))

def getByIndex(_seq, _index, default_value):
    try:
        return _seq[_index]
    except Exception as e:
        print('somethingWrong')
        return default_value

def updateStateByKey_func(seqPerUser, currentState): 
    #seqPerUser:list, currentState:pattern==>(numProductPerUser, revenuePerUser)
    numProductPerUser = len(seqPerUser) + getByIndex(currentState, 0, 0) 
    revenuePerUser = sum([price for _, price in seqPerUser]) + getByIndex(currentState, 0, 0)
    return (numProductPerUser, revenuePerUser)

initialStateRDD = sc.parallelize([('testUser1', (0, 0)), ('testUser2', (0, 0))])
#it won't work, should use real name for initializing userState 

# create actions
lines.pprint()
purchasesPerUser = lines.updateStateByKey(updateStateByKey_func, initialRDD=initialStateRDD )
purchasesPerUser.pprint(14)
# start sc_stream
ssc.start()
ssc.awaitTermination()


