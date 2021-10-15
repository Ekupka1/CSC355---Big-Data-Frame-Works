from mrjob.job import MRJob
from mrjob.step import MRStep

#Finds the list of restaurants with the maximum percentage of five start ratings.
class five_stars(MRJob):

	#splits the data lines returns the items
	def mapper_find_fives(self,_,line):
		items=line.split(',')
		if int(items[3])==5:
			yield str(items[1]),1
		else:
			yield str(items[1]),0

	#sets count and stars to 0 then a for loop to add to count and to five stars
	def reducer_sum_ratings(self,restaurant_name,values):
		count=0
		five_stars=0
		for value in values:
			five_stars+=value
			count+=1
		yield None,(restaurant_name,five_stars/count)

	#returns ratings
	def mapper_swap_kv(self,_,name_ratings):
		yield name_ratings[1],name_ratings[0]

	#makes name list then adds resturant names to list then returns rating and names
	def reducer_combine_rest_names(self,ratings,rest_names):
		names=[]
		for name in rest_names:
			names.append(name)
		yield None,(ratings,names)

	#returns the max of ratings
	def reducer_max_rating(self,_,rating_rest_pair):
		yield max(rating_rest_pair)

	#establishes the order the functions are excecuted
	def steps(self):
		return[
			MRStep(mapper=self.mapper_find_fives,
				reducer=self.reducer_sum_ratings),
			MRStep(mapper=self.mapper_swap_kv,
				reducer=self.reducer_combine_rest_names),
			MRStep(reducer=self.reducer_max_rating)
			]

if __name__ == '__main__':
	five_stars.run()
