# Big Data Programming 
# Lab 2 - Spark 

from __future__ import print_function
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# check if there's a file 
if len(sys.argv) != 2:
    print("Usage: KMeans-ML-NBA <file>", file=sys.stderr)
    sys.exit(-1)

from math import sqrt
from random import uniform as rand
from collections import defaultdict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql import *
from pyspark.sql.types import *


# ---------- helper functions ----------     

def euclDist(point, centroid):
    '''
    point: a row of list
    centroid: a row of list
    output: a float value
    '''
    val = 0.
    for i in range(len(point)):
        val += (point[i] - centroid[i]) ** 2
    dist = math.sqrt(val)   
    return dist

# a function to return the index of the closet centroid
def closestCentroid(col1, col2, col3):
	'''
	input: float value
	output: integer 
	'''
	points = [col1, col2, col3]
	dist_list = []
	for c in newCenter:
		dist_list.append(euclDist(points, c))
	closest = float('inf') # an unbounded upper value for comparison
	index = -1
	for i, v in enumerate(dist_list):
		if v < closest:
			closest = v
			index = i
	return int(index)

# a udf to add column on RDD 
minCenter = udf(closestCentroid, IntegerType())


# a function to calculate the new centroids
def calNewCentroid(df, col):
	'''
	output: a float or int value 
	'''
	sumVal = round(df.select(F.sum(col)).collect()[0][0], 2)
	n = df.count()
	if n > 0: 
		val = round(sumVal / n,  2)
	return val


# -------------------------
# ----- Main Program -----

spark = SparkSession.builder.appName("NBA-kmeans").getOrCreate()


# ---- load and preprocessing -------

df = spark.read.format("csv").load(sys.argv[1], header = "true", inferSchema = "true")
dataPts = df.filter(df.player_name == 'james harden').select('SHOT_DIST','CLOSE_DEF_DIST', 'SHOT_CLOCK').na.drop()
dataRDD = dataPts.rdd.map(lambda r: (r[0], r[1], r[2]))

# -------- initialization ---------

# randomly select 4 initial centroid 
k = 4
int_centroid = dataPts.takeSample(False, k)

# -------- iteration -------------

# a variable to check change 
newCenter = int_centroid
oldCenter = int_centroid
iters = 0 

# an interation until
# newCenter = oldCenter
while True: 
	# add a new column with the assigned cluster
	rddCluster = dataPts.drop('Cluster')
	rddCluster = dataPts.withColumn('Center', minCenter(dataPts.SHOT_DIST, dataPts.CLOSE_DEF_DIST, dataPts.SHOT_CLOCK))
	# iteration to find the new centroids	
	converge = 0 
	# calculate the new centroids for each cluster 
	for i in range(k):
		kCluster = rddCluster.filter(rddCluster.Center == i)
		n = kCluster.count()
		sumCol = [0] * 3
		sumCol[0] = calNewCentroid(kCluster, 'SHOT_DIST')
		sumCol[1] = calNewCentroid(kCluster, 'CLOSE_DEF_DIST')
		sumCol[2] = calNewCentroid(kCluster, 'SHOT_CLOCK')
		sumOfCols = [round(x / n, 4) for x in sumCol]
		newCenter[i] = sumOfCols
		print(newCenter)
		if newCenter[i] == oldCenter[i]:
			converge += 1
		elif:
			diff = 0.0009 
			closeDiff = [round((a - b)**2, 6) for a, b in zip(newCenter[i], oldCenter[i])]
			if all(v <= diff for v in closeDiff):
				converge += 1
	iters += 1
	print("Iteration - %s round" %(iters))
	if converge >= 4:
		print("Converge at the %s iteration\n" %(iters))
		print("\nFinal Centroids: %s" %(new_centroid))
		break
	else:
		iters += 1
		print("Iteration - %s round" %(iters))
		old_centroid = new_centroid
		print('Update:',old_centroid,'\n')

spark.stop()