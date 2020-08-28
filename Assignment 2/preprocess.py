from pyspark import  SparkContext
import json
import csv
sc = SparkContext()
rdd = sc.textFile('review-002.json')
rdd_1 = rdd.map(json.loads).repartition(16).persist()

rdd_2 = sc.textFile('business.json')

rdd_2 = rdd_2.map(json.loads)
rdd_2= rdd_2.map(lambda x: (x['business_id'],x['state']))
rdd_1 = rdd_1.map(lambda x:(x['business_id'],x['user_id']))
res = rdd_1.join(rdd_2).filter(lambda x: x[1][1]=='NV').map(lambda x: (x[1][0],x[0])).collect()

with open('user_business.csv' ,'w') as f:

    fnames = ['user_id', 'business_id']
    writer = csv.DictWriter(f, fieldnames=fnames)
    writer.writeheader()

    for row in res:
        row = list(row)
        writer.writerow({'user_id': row[0],'business_id':row[-1]})
f.close()