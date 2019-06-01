from __future__ import print_function
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# check if there's a file 
if len(sys.argv) != 2:
    print("parking-kmeans.py <file>", file=sys.stderr)
    sys.exit(-1)

from pyspark.sql import SparkSession
import math
from math import sqrt
from collections import Counter
from collections import defaultdict
from operator import itemgetter


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

# Initialize the spark context
spark = SparkSession.builder.appName("parking-kmeans").getOrCreate()

# -----------------------------------
# ---- load and preprocessing -------

df = spark.read.format("csv").load(sys.argv[1], header = "true", inferSchema = "true")

# subset the 3 street code and black cars
black_list = ['BK', 'BLK', 'BK/', 'BK.', 'BLK.', 'BLAC', 'Black', 'BCK', 'BC', 'B LAC']
black = df.filter(df['Vehicle Color'].isin(black_list))
dataPts = black.select(black['Street Code1'], black['Street Code2'], black['Street Code3']).na.drop()
dataRDD = dataPts.rdd.map(lambda r: (r[0], r[1], r[2]))

# ---------------------------------
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

# for the given street code 
# calcualte the probability of getting tickets 
street_code = [34510, 10030, 34050]
closet = closestCenter(street_code, new_centroid)
map3 = dataRDD.filter(lambda x: closestCenter(x, new_centroid)[0] == closet[0]).collect()
count = len(map3)
token = dict(Counter(map3))
counter = len(token)
maxV = max(token.items(),key = itemgetter(1))[1]
probability = round(count/(maxV * counter), 6)
print ("Probability of getting ticets:\n")
print (probability)

spark.stop()
