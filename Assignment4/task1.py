from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
from graphframes import  *
from collections import Counter
from itertools import combinations
import time,sys,os

def get_candidate_users(l):
    res = []
    for a,b in list(combinations(l,2)):
        c1 = user_business[a]
        c2 = user_business[b]
        common_elements = c1.intersection(c2)
        if len(common_elements)>=filter_threshold:
            res.append(((a,b)))
            res.append((b,a))
    return res



Conf = SparkConf() \
    .setAppName('hw4') \
    .setMaster('local[3]')\
   .set('spark.jars.packages','graphframes:graphframes:0.6.0-spark2.3-s_2.11')

start = time.time()
sc = SparkContext(conf=Conf)
sc.setLogLevel("ERROR")

sqlContext = SQLContext(sc)
rdd = sc.textFile(sys.argv[2])
header = rdd.first()
filter_threshold = int(sys.argv[1])
user_business= required_rdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')) \
        .map(lambda x: ((x[0]), (x[1]))).groupByKey().mapValues(set).collectAsMap()

edges = get_candidate_users(user_business.keys())
vertices = list(set([element for tup in edges for element in tup]))

vertices = list(map(lambda x: [x],vertices))
res = []
edges = sqlContext.createDataFrame(edges).toDF("src","dst")
vertices = sqlContext.createDataFrame(vertices).toDF("id")
graph = GraphFrame(vertices,edges)
communities =graph.labelPropagation(maxIter=5)

sorted_communities =communities.rdd.map(lambda x:(x['label'],x['id'])).groupByKey().mapValues(lambda x:(sorted(list(x)),len(x)))\
    .map(lambda x:x[1]).sortBy(lambda x:(x[1],x[0][0])).map(lambda x:x[0]).collect()

with open(sys.argv[3],'w') as f:

    for community in sorted_communities:
        f.write(str(community)[1:-1])
       
        f.write("\n")
    
end = time.time()
print("Duration: ", end - start)