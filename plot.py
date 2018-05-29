from pyspark import SparkContext
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
from sys import argv
import numpy as np


sc=SparkContext("local", "filterToPLot")

file=None
airportCode = None
outputFileName="tempData"
try:
	file=sc.textFile(argv[-1])
	airportCode = argv[-2]
except Exception as e:
	print("Wrong input\n")
	raise e 


filteredData = file.map(lambda x: x.split(",")).filter(lambda x: x[0]!="date" and x[1]==airportCode.upper())
resultX=filteredData.map(lambda x: x[0]).collect()
resultY=filteredData.map(lambda x: float(x[2])).collect()
#print(len(resultX))
if(len(resultX)==0):
	print("Wrong airport code")
plt.plot(resultX, resultY)
plt.xticks(np.arange(0, len(resultX), step=50))
plt.show()

'''plt.scatter(resultX, resultY)
plt.show()'''