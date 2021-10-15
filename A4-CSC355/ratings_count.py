from mrjob.job import MRJob

#Finds the number of ratings for each restaurant.
class num_ratings(MRJob):

	#splits the data line by every ","
	def mapper(self,_,value):
		words=value.split(',')
		yield words[1],1

	#Sets count to 0 then sets count to the sum of the values
	def reducer(self,key,values):
		count=0
		count=sum(values)
		yield key,count

if __name__ == '__main__':
	num_ratings.run()
