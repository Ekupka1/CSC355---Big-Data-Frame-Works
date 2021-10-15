# AirCast
# By: Rahul Rangarajan, Alan Sitzman, Austin Stala, Peter Weber, and Ethan Kupka

#importing spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession.builder     .appName("Final Project")    .getOrCreate()

#plot imports
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.cbook import get_sample_data
import matplotlib.image as mpimg
import numpy as np
import csv
import PIL
from PIL import Image

#picture import
import requests

#Set user inputs
airport_df=spark.read.option("header",True)     .csv("airports.csv")

#sets location
userLat=41.884701
userLon=-91.7108

#scaling of plot
zoom=12
userRad=360/2**(zoom-2)

#convert to Unix time
userStart=1627905610
userEnd=1627948790

userAlt=40000

#converting to Meters
field_elevation=(672*.305)

#reduce our DataFrame to the stuff we need
full_df=spark.read.option("header",False)     .csv("24hours.csv")
full_df = full_df.withColumnRenamed("_c0","Time") \
	.withColumnRenamed("_c1","icao24") \
	.withColumnRenamed("_c2","Lat") \
	.withColumnRenamed("_c3","Lon") \
	.withColumnRenamed("_c7","Callsign") \
	.withColumnRenamed("_c8","OnGround") \
	.withColumnRenamed("_c12","Alt")

full_df = full_df.withColumn("Lat",full_df.Lat.cast("float")-userLat) \
	.withColumn("Lon",full_df.Lon.cast("float")-userLon) \
	.withColumn("Alt",full_df.Alt.cast("float"))

reduce_df = full_df.drop("_c4","_c5","_c6","_c9","_c10","_c11","_c13","_c14","_c15")

#creating a fully filtered DataFrame - time
filtered_df=reduce_df.filter(reduce_df.Time <= userEnd)
filtered_df=filtered_df.filter(filtered_df.Time >= userStart)
#lat and lon
filtered_df=filtered_df.filter(func.abs((filtered_df.Lat)) < userRad)
filtered_df=filtered_df.filter(func.abs((filtered_df.Lon)) < userRad)

filtered_df=filtered_df.filter(filtered_df.Alt<=userAlt)

#downloading the picture for the plot
key="CtnQt4zaWXTKzTqZRIeR97svfaDFObRb"
print("Getting Map...")
param={"center":str(userLat)+","+str(userLon),"key":key,"zoom":zoom,"dim":0,"size":"1024,1024"}
req=requests.get("https://open.mapquestapi.com/staticmap/v4/getmap",params=param)
with open("map.jpg","wb") as image:
    image.write(req.content)

#sorting dataframe for plotting
filtered_df = filtered_df.sort(filtered_df.Time)
sorted_df = filtered_df.groupBy("icao24").agg(func.collect_list("Lat"),func.collect_list("Lon"),func.collect_list("Alt"))

#creating the plot
print("Opening Map...")
im = Image.open("map.jpg")

#flips the picture right side up to show correctly on grid
print("Converting to PNG and flipping...")
im = im.transpose(PIL.Image.FLIP_TOP_BOTTOM)
im.save("flipped_map.png")
img=mpimg.imread('flipped_map.png')

#creating meshgrid the size of the image
print("Creating Grid...")
x, y = np.meshgrid(np.arange(-userRad/2,userRad/2,userRad/1024),np.arange(-userRad/2,userRad/2,userRad/1024))

#makes the 3D plot
print("Creating 3D plot...")
fig = plt.figure()
ax = plt.axes(projection='3d')

#adds the picture to the "bottom" of plot
print("Adding map to plot...")
ax.plot_surface(x, y, 0*x+field_elevation, rstride=2, cstride=2, zorder=0, facecolors=img, linewidth=0.01)

#ploting the planes in a for loop
print("Adding planes to plot...")
bigList=sorted_df.collect()
for i in bigList:
    ax.plot(i[2],i[1],i[3],linewidth=.03,zorder=10)

#saving the plotted planes to the png
print("Saving plot as result.png...")
ax.set_xlim(-userRad/2,userRad/2)
ax.set_ylim(-userRad/2,userRad/2)
ax.set_zlim(field_elevation,userAlt)
plt.savefig('result.png',bbox_inches='tight', pad_inches=0,dpi=600)
print("done")

spark.stop()


