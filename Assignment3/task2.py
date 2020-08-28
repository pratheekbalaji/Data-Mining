from pyspark import SparkContext
import sys,time
import json
import re
from collections import Counter
import math
stopword = []

f = open(sys.argv[3],'r')


for line in f:
    stopword.append(line)
stopword =list (map(lambda s: s.strip(),stopword))



def remove_stopwords(review):
    review = review.lower()
    review = re.sub(r'[^A-Za-z]',' ',review).strip()
    review = review.split()
    res = [x for x in review if x not in stopword]

    return res

def remove_rare_words(words,rare_words):
    l = [x for x in words if x not in rare_words]
    return l

def get_tf_idf(word_list):
    c = Counter(word_list)
    max_val = c.most_common(1)[0][1]
    for key in c:
        c[key] = c[key] / max_val
    for key in c:
        c[key] = c[key]*idf[key]
    tf_idf = sorted(c.items(),key=lambda x:x[1],reverse=True)
    final_words = []
    for x in tf_idf[:200]:
        final_words.append(x[0])
    return final_words

def get_idf(count):
    return math.log2(n_documents/count)
start = time.time()

sc = SparkContext()
sc.setLogLevel("ERROR")

rdd = sc.textFile(sys.argv[1]).map(json.loads)

# generating business profiles
business_words= rdd.map(lambda x: (x['business_id'],x['text'])).reduceByKey(lambda x,y:x+y)\
    .map(lambda x:(x[0],remove_stopwords(x[1])))
total_words = business_words.flatMap(lambda x:x[1])
total_words_all = total_words.count()
total_words_unique = total_words.distinct().collect()
# create a dictionary storing indexes of individual words in the dictionary
words_index = {}
for a,b in enumerate(total_words_unique):
    words_index[b]=a
business_words = business_words.map(lambda x:((x[0],[words_index[a] for a in x[1]])))
rare_words_threshold = 0.000001*total_words_all
print(rare_words_threshold)
rare_words = business_words.map(lambda x:x[1]).map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y).filter(lambda x: x[1]<rare_words_threshold).collectAsMap()
business_filtered = business_words.map(lambda x: (x[0],remove_rare_words(x[1],rare_words)))
n_documents = business_filtered.count()
#Calculating TF IDF


idf = business_filtered.mapValues(set).flatMap(lambda x:x[1]).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)\
    .mapValues(get_idf).collectAsMap()

print('idf done')
business_profile = business_filtered.mapValues(get_tf_idf).collectAsMap()


# generating user profile

def business_words(business):
    res = []
    for b in business:
        res.extend(business_profile[b])

    return list(set(res))

user_profile = rdd.map(lambda x:((x['user_id'],x['business_id']))).groupByKey()\
.mapValues(list).mapValues(business_words).collectAsMap()



res = [business_profile,user_profile]
with open(sys.argv[2],'w')as f:
        for r in res:
            json.dump(r,f)
            f.write('\n')


end = time.time()
print('Duration:', end - start)
