import sys
import json
from collections import defaultdict
from pyspark import SparkContext
if len(sys.argv)!=6:
    print ('Usage spark_submit task2.py <review_file> <business_file > <output_file> <if_spark> <n>')
    sys.exit(1)
sc = SparkContext()   

if_spark = sys.argv[4]
required_count = int(sys.argv[5])
answer = {}
def func(x):
    res = []
    if x[1] is not None:
        temp = x[1].strip()
        temp = temp.split(',')
        for values in temp:
            values = values.strip()
            res.append((x[0],values))
            
    
    return res
    
    

if if_spark == "spark":
    review_rdd = sc.textFile(sys.argv[1])
    business_rdd = sc.textFile(sys.argv[2])
    review_rdd_json = review_rdd.map(json.loads)
    business_rdd_json = business_rdd.map(json.loads)
    l1 = review_rdd_json.map(lambda x:(x['business_id'],x['stars']))
    result =l1.join(business_rdd_json.map(lambda x:(x['business_id'],(x['categories']))).flatMap(func))\
    .map(lambda x:(x[1][1],x[1][0])).aggregateByKey((0,0),lambda u,x:(u[0]+1,u[1]+x),lambda a,b:((a[0]+b[0]),(a[1]+b[1])))\
    .map(lambda x:(x[0],(x[1][1]/x[1][0]))).sortBy(lambda x:(-x[1],x[0])).take(required_count)
    
    answer['result'] = result
 
else:
    with open (sys.argv[1],'r') as f:
        review_file = f.readlines()
    with open(sys.argv[2],'r') as f:
        business_file = f.readlines()
    review_file_details = []
    for values in review_file:
        values = json.loads(values)
        review_file_details.append((values['business_id'],values['stars']))
        business_file_data = []
    for values in business_file:
        values = json.loads(values)
        if values['categories'] is not None:
            categories = values['categories'].strip()
            categories = categories.split(',')
            for category in categories:
                category = category.strip()
                business_file_data.append((values['business_id'],category))
    d =defaultdict(list)
    for row in review_file_details:
        d[row[0]].append(row[1])
    result =defaultdict(list)
    for row in business_file_data:
        if row[0] in d:
            for values in d[row[0]]:
               
                result[row[1]].append(values)
    avg_dict ={}
    for key,value in result.items():
        avg_dict[key] = sum(value)/len(value)
       
        
        
    result = sorted(avg_dict.items(), key=lambda x: (x[0]))
    result = sorted(result, key=lambda x: (x[1]) ,reverse=True)[:required_count]
    answer['result'] = result

with open(sys.argv[3],'w') as f:
    json.dump(answer,f)
      



