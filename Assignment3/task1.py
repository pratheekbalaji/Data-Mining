from pyspark import SparkContext,SparkConf
from itertools import combinations
import itertools
import json
from collections import OrderedDict
import random
import sys

import time
user_dict = OrderedDict()


def min_hash(a,b,user):
    hash_values = [(a*x+b)%m for x in user]
    return min(hash_values)


def generate_signature(x):
    business,user = x
    signature =[min_hash(a,b,user) for a,b in zip(A,B)]
    return((business,signature))


def hash_signatures(business_signatures):

    business,signature = business_signatures
    # bucket size =32 rows = 1
    b = 32
    r = 1
    res = [((i, hash(tuple(sorted(signature[i * r:(i + 1) * r])))), [business]) for i in range(0, b)]
    return res


def jaccard_similarity(x,y,rdd_map):
    x = set(rdd_map[x])
    y = set(rdd_map[y])
    a =x.union(y)
    b = x.intersection(y)
    return len(b)/len(a)

start = time.time()
SparkContext.setSystemProperty('spark.executor.memory', '4g')

sc = SparkContext()

sc.setLogLevel("ERROR")
rdd = sc.textFile(sys.argv[1])

json_rdd = rdd.map(json.loads)


user_id = json_rdd.map(lambda x:x['user_id']).distinct().collect()

user_index = 0

for users in user_id:
    user_dict[users] = user_index
    user_index+=1

signatures = 32
m = len(user_id)
A = random.sample(range(120,200),signatures)
B = random.sample(range(1,120),signatures)

required_rdd = json_rdd.map(lambda x:(x['business_id'],x['user_id'])).map(lambda x:(x[0],user_dict[x[1]])).groupByKey().map(lambda x: (x[0], list(set(x[1]))))
rdd_map = required_rdd.collectAsMap()
signature=required_rdd.map(generate_signature)
lsh_candidates = signature.flatMap(hash_signatures).reduceByKey(lambda x,v: x+v).filter(lambda x:len(x[1])>1).map(lambda x: x[1])

lsh_pairs = lsh_candidates.flatMap(lambda x: list(sorted(combinations(x,2)))).distinct().persist()
sim_candidates = lsh_pairs.map(lambda x:(x,(jaccard_similarity(x[0],x[1],rdd_map)))).filter(lambda x:x[1] >=0.05).distinct().collect()


with open(sys.argv[2],'w') as f:
    for x in sim_candidates:
        d = {"b1":x[0][0],"b2":x[0][1],"sim":x[1]}
        json.dump(d,f)
        f.write('\n')
end = time.time()
print(end-start)