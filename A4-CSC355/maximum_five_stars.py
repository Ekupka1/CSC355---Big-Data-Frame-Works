from mrjob.job import MRJob
from mrjob.step import MRStep

#Finds the list of restaurants with the most number of five start ratings.
class five_stars(MRJob):

	#splits every data line then returns string
	def mapper_find_fives(self,_,line):
		items=line.split(',')
		if int(items[3])==5:
			yield str(items[1]),1

	#finds the sum of each rating
	def reducer_sum_ratings(self,restaurant_name,values):
		count=int(sum(values))
		yield None,(restaurant_name,count)

	#returns the ratings then resturants names
	def mapper_swap_kv(self,_,name_ratings):
		yield name_ratings[1],name_ratings[0]

	#Makes a list for resturants names then adds the name of resturant to list then returns
	def reducer_combine_rest_names(self,ratings,rest_names):
		names=[]
		for name in rest_names:
			names.append(name)
		yield None,(ratings,names)

	#returns the max of the ratings
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
