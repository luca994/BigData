from pyspark import SparkContext
from sys import argv
import datetime

def mapFuncKey(elem, airport):
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
	if(airport=="origin"):
		return key+"-"+elem[16]
	elif(airport=="destination"):
		return key+"-"+elem[17]

def originPenalty(elem):
	penalty = 0
	if(int(elem[15])>15):
		penalty = 1
	return penalty

def destPenalty(elem):
	penalty = 0
	if(int(elem[14])>15):
		penalty = 0.5
	return penalty

sc=SparkContext("local", "query1")

file=None
outputFileName=None
try:
	file=sc.textFile(argv[-2])
	outputFileName=argv[-1]
except Exception as e:
	print("Wrong input\n")
	raise e 

splittedFile = file.map(lambda x: x.split(",")).filter(lambda x: x[0]!="Year" and x[14]!="NA" and x[15]!="NA")\
					.flatMap(lambda x: ((mapFuncKey(x, "origin"), originPenalty(x)), (mapFuncKey(x, "destination"), destPenalty(x)))).reduceByKey(lambda x,y: x+y)
result = splittedFile.collect()
fileWrite = open(outputFileName, "w")
for el in result:
	value = str(el[1]).split(".")
	if(len(value)>1):
		value=value[0]+","+value[1]
	else:
		value = value[0]
	fileWrite.write(el[0]+";"+value+"\n")
fileWrite.close()