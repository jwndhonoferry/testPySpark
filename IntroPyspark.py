from pyspark import SparkContext, SparkConf
from operator import add

def spark():
    # conf = SparkConf()
    # conf.setAppName("app_spark").setMaster("local[0]")
    sc = SparkContext("local[*]", "app_spark")
    return sc
sc = spark()
#This method for turn off logging info in spark, i used this beacuse i want to see the result quickly
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

quiet_logs(sc)
words = sc.parallelize (
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"])
collect = words.collect()
count =  words.count()

def f(x):
    print(x)

fore = words.foreach(f)

words_filtered = words.filter(lambda x : 'spark' in x)
filtered = words_filtered.collect()

words_map = words.map(lambda x : (x, x + ' tambahan'))
mapping = words_map.collect()

nums = sc.parallelize([1,2,3,4,5])
adding = nums.reduce(add)


print("Number of words in RDD = ", collect)
print("Number of words  = ", count)
print("Words with filtered ", filtered)
print("Words with mapping ", mapping)
print("Adding all elements ", adding)


filter = [i for i  in range(25) if i%2==0]
print(filter)


baba = "Ini Ferru Jiwandhohono,awwaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
print("wors:" , baba)