from mrjob.job import MRJob

#Finds the average ratings for each restaurant.
class average_ratings(MRJob):

	#splits the data lines by ","
	def mapper(self,_,value):
		words=value.split(',')
		yield words[1],(words[3])

	#sets count and result to 0 then inc result and count and returns the result(result/count
	def reducer(self,key,values):
		result=0
		count=0
		for value in values:
			result+=int(value)
			count+=1
		yield key,result/count

if __name__ == '__main__':
	average_ratings.run()
