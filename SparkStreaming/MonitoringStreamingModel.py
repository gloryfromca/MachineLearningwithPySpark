import sys
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD
import numpy as np
import math
from datetime import datetime

#initialize sc,then initialize ssc
sc = SparkContext(appName="StreamingModel1")
#just print ssc result and error 
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, batchDuration=5)# batch rdd per 5 seconds  
stream = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

#create model 
len_term = 100
Model1 = StreamingLinearRegressionWithSGD(stepSize=0.01, numIterations=50)
Model1.setInitialWeights(np.array([0.0, ]*len_term))
Model2 = StreamingLinearRegressionWithSGD(stepSize=0.1, numIterations=50)
Model2.setInitialWeights(np.array([0.0, ]*len_term))
Model3 = StreamingLinearRegressionWithSGD(stepSize=1, numIterations=50)
Model3.setInitialWeights(np.array([0.0, ]*len_term))
#0.01 is slow; 1 leads to diverge; 0.1 is good   

# create actions
labeledStream = stream.map(
    lambda x:tuple(x.split(','))
    ).map(
    lambda x:(float(x[0]), np.array([float(i) for i in x[1:]]) )
    ).map(
    lambda x:LabeledPoint(label=x[0], features=x[1])
    )

Model1.trainOn(labeledStream)
Model2.trainOn(labeledStream)
Model3.trainOn(labeledStream)

def RMSEPerRDD(rdd):
    #don't put latestModel() outside, Model_latest should update as labeledStream updates every time  
    Model1_latest = Model1.latestModel() 
    Model2_latest = Model2.latestModel()
    Model3_latest = Model3.latestModel()
    
    try:
        mse1 = rdd.map(lambda x:x.label - Model1_latest.predict(x.features)).map(lambda x:x*x).mean()
        rmse1 = math.sqrt(mse1)
        mse2 = rdd.map(lambda x:x.label - Model2_latest.predict(x.features)).map(lambda x:x*x).mean()
        rmse2 = math.sqrt(mse2)
        mse3 = rdd.map(lambda x:x.label - Model3_latest.predict(x.features)).map(lambda x:x*x).mean()
        rmse3 = math.sqrt(mse3)
        time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        print("""
--------------------------
|time:%s|
--------------------------
            """%time)
        print('Model1:RMSE is %s, Model2:RMSE is %s, Model3:RMSE is %s'%(rmse1, rmse2, rmse3))
    except Exception as e:
        print(e)
    return 

labeledStream.foreachRDD(RMSEPerRDD )

# start sc_stream
ssc.start()
ssc.awaitTermination()


