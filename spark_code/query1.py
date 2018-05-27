from pyspark import SparkContext
from sys import argv

sc=SparkContext("local", "query1")

file=None
outputFileName=None
try:
	file=sc.textFile(argv[-2])
	outputFileName=argv[-1]
except Exception as e:
	print("Wrong input\n")
	raise e

splittedFile = file.map(lambda x: x.split(",")).filter(lambda x: x[0]!="Year")\
					.map(lambda x: (x[0]+"-"+x[1]+"-"+x[2], (int(x[21]), 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
resultFile = splittedFile.map(lambda x: (x[0], (x[1][0]/x[1][1])*100))
resultList = resultFile.collect()
fileWrite = open(outputFileName, "w")
for el in resultList:
	value = str(el[1]).split(".")
	if(len(value)>1):
		value=value[0]+","+value[1]
	else:
		value = value[0]
	fileWrite.write(el[0]+";"+value+"\n")
fileWrite.close()