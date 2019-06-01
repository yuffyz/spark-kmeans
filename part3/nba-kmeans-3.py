# Big Data Programming 
# Lab 2 - Spark 

from __future__ import print_function
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# check if there's a file 
if len(sys.argv) != 2:
    print("nba-kmeans-1.py <file>", file=sys.stderr)
    sys.exit(-1)

import math
from math import sqrt
from pyspark.sql import SparkSession


# ---------- helper functions ----------    


def closestCenter(data, center):
	'''
	a function to return the index of the closet centroid
	for a given datapoint
	data: rdd
	center: 4 centroid 
	'''
	dist_list = [] 
	for c in center:
		val = 0.
		for i in range(3):
			val += (data[i] - c[i]) ** 2
		dist = sqrt(val)   
		dist_list.append(dist)   
	closest = float('inf') # an unbounded upper value for comparison
	index = -1
	for i, v in enumerate(dist_list):
		if v < closest:
			closest = v
			index = i
	return int(index), data


# a function to calcualte the new centroid 
def cal_centroid(data):
	"""
	input: a dictionary of dictyionary 
	output: new centroid, a list of list
	"""
	key, value = data[0], data[1]
	n = len(value)
	update = [0.] * 3
	for i in value:
		update[0] += float(i[0])
		update[1] += float(i[1])
		update[2] += float(i[2])
	nCenter = [round(x / n, 4) for x in update]
	return nCenter

# ----------------------------------------------
# ----------------- Main Program ---------------

spark = SparkSession.builder.appName("NBA-kmeans").getOrCreate()


# ---- load and preprocessing -------

df = spark.read.format("csv").load(sys.argv[1], header = "true", inferSchema = "true")
dataPts = df.filter(df.player_name == 'james harden').select('SHOT_DIST','CLOSE_DEF_DIST', 'SHOT_CLOCK').na.drop()
dataRDD = dataPts.rdd.map(lambda r: (r[0], r[1], r[2]))

# -------- initialization ---------

# randomly select 4 initial centroid 
k = 4
int_centroid = dataRDD.takeSample(False, k)

iters = 0 
old_centroid = int_centroid

for m in range(40):
	map1 = dataRDD.map(lambda x: closestCenter(x, old_centroid))
	reduce1 = map1.groupByKey()
	map2 = reduce1.map(lambda x: cal_centroid(x)).collect() # collect a list	
	new_centroid = map2
	converge = 0 
	for i in range(k):
		if new_centroid[i] == old_centroid[i]:
			converge += 1
		else:
			diff = 0.0009 
			closeDiff = [round((a - b)**2, 6) for a, b in zip(new_centroid[i], old_centroid[i])]
			if all(v <= diff for v in closeDiff):
				converge += 1
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
