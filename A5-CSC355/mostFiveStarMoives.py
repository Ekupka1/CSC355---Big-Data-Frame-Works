#Finds the most five star movies from the bigData
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Assignment_5")
sc = SparkContext(conf=conf)

#Gets the files
ratings_rdd=sc.textFile("ratings.csv")
movies_rdd = sc.textFile("movies.csv")

rating_header = ratings_rdd.first()
ratings_rdd = ratings_rdd.filter(lambda row: row != rating_header)
movie_header = movies_rdd.first()
movies_rdd = movies_rdd.filter(lambda row: row != movie_header)

#Maps the data
ratings_temp = ratings_rdd.map(lambda x:(x.split(',')[1],float(x.split(',')[2])))
movies_temp = movies_rdd.map(lambda x:(x.split(',')[0], x.split(',')[1]))

ratings_temp = ratings_temp.mapValues(lambda x: 1 if int(x) == 5 else 0)

#Reduces the unwanted
ratings_final = ratings_temp.reduceByKey(lambda a,b: a+b)
ratings_final = ratings_final.sortBy(lambda x: -int(x[1]))
print(movies_temp.lookup(ratings_final.first()[0]),ratings_final.first()[1])

#Takes the first 10 data
print(ratings_temp.take(10))

sc.stop()
