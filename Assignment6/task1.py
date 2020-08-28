from pyspark import SparkContext
import binascii
import json
import math
import random
import csv
def generate_hash(A,B,x,m):
    idx = []
    for a,b in zip(A,B):
        hash_value = (a*x+b)%m
        idx.append(hash_value)
    return idx


def convert_to_int(s):
    if s=="":
        return -1
    return int(binascii.hexlify(s.encode('utf8')), 16)

sc = SparkContext()
first_rdd = sc.textFile('business_first.json')
second_rdd = sc.textFile('business_second.json')

cities_first =  first_rdd.map(lambda x: json.loads(x)).map(lambda x: x['city'])\
    .filter(lambda x : x!="").map(convert_to_int).distinct().collect()

cities_second =  second_rdd.map(lambda x: json.loads(x)).map(lambda x: x['city'])\
   .map(convert_to_int).collect()
k = 5
m = int((len(cities_first)*k)/(math.log(2)))

bloom_filter = [0]*m
random.seed(12)
A = random.sample(range(120,200),k)
B = random.sample(range(1,120),k)

for c in cities_first:
    indices = generate_hash(A,B,c,m=m)
    for i in indices:
        if bloom_filter[i]==0:
            bloom_filter[i] = 1
result = []
for c in cities_second:
    if c==-1:
        result.append(0)
        continue
    indices = generate_hash(A,B,c,m=m)
    flag = True
    for i in indices:
        if bloom_filter[i]==0:
            flag = False
            break
    if flag==True:
        result.append(1)
    if flag==False:
        result.append(0)
result = list(map(str,result))

with open('res', 'w') as fp:

    fp.write(str(result[:-1]))
print(len(result))
print(len(cities_second))