import sys
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD
import numpy as np

#initialize sc,then initialize ssc
sc = SparkContext(appName="StreamingModel")
#just print ssc result and error 
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, batchDuration=5)# batch rdd per 5 seconds
stream = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

#create model 
len_term = 100
Model = StreamingLinearRegressionWithSGD(stepSize=0.1, numIterations=50)
Model.setInitialWeights(np.array([0.0, ]*len_term))

# create actions
labeledStream = stream.map(
    lambda x:tuple(x.split(','))
    ).map(
    lambda x:(float(x[0]), np.array([float(i) for i in x[1:]]) )
    ).map(
    lambda x:LabeledPoint(label=x[0], features=x[1])
    )

labeledStream.pprint(1)
Model.trainOn(labeledStream)
Model.predictOn(labeledStream.map(lambda x:x.features)).pprint(1)

# start sc_stream
ssc.start()
ssc.awaitTermination()


