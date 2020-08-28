from pyspark import SparkContext
import json
from datetime import datetime

import re
import sys
from collections import OrderedDict
if (len(sys.argv)!=7):
     print ('Usage spark_submit task1.py <input_file> <output_file> <stopwords> <y> <m> <n>')
     sys.exit(1)

f = open(sys.argv[3])
stopword = []
for line in f:
    stopword.append(line)
stopword =list (map(lambda s: s.strip(),stopword))
f.close()
sc = SparkContext()
answer = OrderedDict()

rdd = sc.textFile(sys.argv[1])
json_data =rdd.map(json.loads)

a = json_data.count()
answer['A'] = a
year = str(sys.argv[4])
    
b = json_data.map(lambda x: x['date'])\
.filter(lambda x: year in x).map\
((lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S').year))\
.aggregate(0,lambda u,v:(u+1),lambda u,v:(u+v))

answer['B'] = b

c = json_data.map(lambda x:x['user_id']).distinct().count()
answer['C'] = c
m = int(sys.argv[5])
d_ans =json_data.map(lambda x:x['user_id']).map(lambda x:(x,1)).reduceByKey(lambda x,v:x+v).sortBy(lambda x:(-x[1],x[0])).take(m)


answer['D'] = d_ans

def remove_punctuations(x):
   
    x = x.lower()
    x = re.sub(r'[\(\)\[\],.!?:;]','',x)
    x = x.split()
    res =[a for a in x  if a not in stopword]
    return res
            

    
            

e = json_data.map(lambda x:x['text'])\
.flatMap(remove_punctuations).map(lambda x:(x,1)).reduceByKey(lambda x,v:x+v).sortBy(lambda x:(-x[1],x[0])).map(lambda x:x[0]).take(int(sys.argv[6]))
answer['E'] = e


with open(sys.argv[2],'w') as f:
    json.dump(answer,f)
    

