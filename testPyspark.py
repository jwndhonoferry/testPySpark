import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

# nums = sc.parallelize([1,2,3,4])
# print(nums.map(lambda x: x*x).collect())
a = [('John', 19), ('Smith', 29), ('Adam', 35), ('Henry', 50)]
rdd = sc.parallelize(a)
ppl = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
DF_ppl = sqlContext.createDataFrame(ppl)
DF_ppl.printSchema()
