#Finds the most five star rime movies from the bigData
from pyspark.sql import SparkSession
spark = SparkSession.builder     .master("local")     .appName("rating_dataframe")     .config("spark.some.config.option", "some-value")     .getOrCreate()

#Opens and reads the data
movies_frame = spark.read.option("header",True)      .csv("ml-25m/movies.csv")
ratings_frame = spark.read.option("header",True)      .csv("ml-25m/ratings.csv")

#Shows the 5th column in the data
ratings_frame.show(n=5)
movies_frame.show(n=5)

five_star = ratings_frame.filter(ratings_frame.rating == 5.0)
print(five_star.count())

#Adds to count
id_fives = five_star.groupBy('movieId').agg({'rating':'count'})
print(id_fives.count())
id_fives.show(n=5)

#Filters and gets the ID for the crime movies
movies_filtered = movies_frame.filter(movies_frame.genres.contains("Crime"))
joined = movies_filtered.join(id_fives,"movieId")
joined.show(n=5)

#Sorts the data
result = joined.sort("count(rating)", ascending = False)
result.show(5)
