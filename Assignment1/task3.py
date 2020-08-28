from pyspark import SparkContext
from collections import OrderedDict
import json
import sys
import time

if len(sys.argv)!=6:
    print('Usage spark-submit task3.py <input_file> <output_file> <partition_type> <n_partitions> <n>')
    sys.exit(1)

sc = SparkContext()
num_partitions = int(sys.argv[4])

n = int(sys.argv[5])
partition_type = sys.argv[3]

answer = OrderedDict()
def partition_func(key):
    return hash(key)%num_partitions


if partition_type == 'default':
    rdd = sc.textFile(sys.argv[1])
    json_rdd = rdd.map(json.loads)
    transformation = json_rdd.map(lambda x: x['business_id']).map(lambda x:(x,1))
    n_items = transformation.glom().map(len).collect()
    result =transformation.reduceByKey(lambda x,v:x+v).filter(lambda x:x[1]>n).collect()
    answer['n_partitions'] = json_rdd.getNumPartitions()
    answer['n_items'] = n_items
    
    answer['result'] = result
    
    
else:
    rdd = sc.textFile(sys.argv[1],num_partitions)
    json_rdd = rdd.map(json.loads)
    transformation = json_rdd.map(lambda x: x['business_id']).map(lambda x: (x, 1))\
    .partitionBy(num_partitions,partition_func)
    n_items = transformation.glom().map(len).collect()
    result = transformation.reduceByKey(lambda x, v: x + v).filter(lambda x: x[1] > n).collect()
    answer['n_partitions'] = num_partitions
    answer['n_items'] = n_items
   
    answer['result'] = result
   
    
with open(sys.argv[2], 'w') as f:
    json.dump(answer, f)

