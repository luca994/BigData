import datetime
from pyspark import SparkContext
from sys import argv

def mapFuncKey(elem):
	week = datetime.date(int(elem[0]), int(elem[1]), int(elem[2])).isocalendar()[1]
	year = None
	key = None
	if elem[1] == "1" and (week == 52 or week == 53):
		year = int(elem[0])-1
		key = str(year)+"-"+str(week)
	elif elem[1] == "12" and week == 1:
		year = int(elem[0])+1
		key = str(year)+"-"+str(week)
	else:
		key = elem[0]+"-"+str(week)
	return key

def mapFuncValue(elem):
	value = 0
	count = 0
	if int(elem[25])>0:
		value = 1
	if int(elem[14])>0:
		count = 1 
	return (value, count)

sc=SparkContext("local", "query2")

file=None
outputFileName=None
try:
	file=sc.textFile(argv[-2])
	outputFileName=argv[-1]
except Exception as e:
	print("Wrong input\n")
	raise e

splittedFile = file.map(lambda x: x.split(",")).filter(lambda x: x[0]!="Year" and x[25]!="NA" and x[14]!="NA").map(lambda x: (mapFuncKey(x), mapFuncValue(x)))\
					.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
result = splittedFile.map(lambda x: (x[0], (x[1][0]/x[1][1])*100)).collect()
fileWrite = open(outputFileName, "w")
for el in result:
	value = str(el[1]).split(".")
	if(len(value)>1):
		value=value[0]+","+value[1]
	else:
		value = value[0]
	fileWrite.write(el[0]+";"+value+"\n")
fileWrite.close()
