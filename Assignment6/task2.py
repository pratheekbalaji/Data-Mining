from pyspark import  SparkContext
from pyspark.streaming import StreamingContext
import json
import binascii
import random
import datetime
import sys
no_of_hash = 30
p = 15531
random.seed(5)
A = random.sample(range(1,p),no_of_hash)
B = random.sample(range(0,p),no_of_hash)
m = 69997
no_of_group = 2
res = []
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

def testing(rdd):
    f = open('task2_ans','a')
    cities = rdd.map(lambda x: json.loads(x)).map(lambda x:x['city']).collect()
    max_trailing_zeros = [-99999]*no_of_hash
    ground_truth = set()
    for c in cities:
        c = convert_to_int(c)
        hashed_values = generate_hash(A,B,c,m)
        binary_rep = [format(h,'016b') for h in hashed_values]
        for a,b in enumerate(binary_rep):
            len_trailing_zeros = len(b) - len(b.rstrip('0'))
            if len_trailing_zeros > max_trailing_zeros[a]:
                max_trailing_zeros[a] = len_trailing_zeros
        ground_truth.add(c)
    averages = []
    for i in range(0,len(max_trailing_zeros),no_of_group):
        sub_group = max_trailing_zeros[i:(i+1*no_of_group)]
        sub_group = [2**i for i in sub_group]
        average = sum(sub_group)/len(sub_group)
        averages.append(average)
    averages = sorted(averages)
    middle_element = len(averages)//2
    median = int(averages[middle_element])
    g = (len(ground_truth))
    f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+","+str(g)+","+str(median))
    f.write("\n")

    f.close()



f = open('task2_ans','w')
f.write("Time"+","+"Ground Truth"+","+"Estimation")
f.write("\n")
f.close()
sc = SparkContext()
ssc = StreamingContext(sc,5)
lines = ssc.socketTextStream("localhost",9999)
rdd = lines.window(30,10)
#rdd.pprint()
rdd.foreachRDD(testing)
ssc.start()
ssc.awaitTermination()