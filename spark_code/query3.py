from pyspark import SparkContext
from sys import argv

def distanceGroup(el):
	groupId = (int(el[18])//200)+1
	return "group"+str(groupId)

def value(el):
	arrDelay = int(el[14])
	depDelay = int(el[15])
	if arrDelay <= depDelay/2:
		return 1
	else:
		return 0


sc=SparkContext("local", "query3")

file=None
outputFileName=None
try:
	file=sc.textFile(argv[-2])
	outputFileName=argv[-1]
except Exception as e:
	print("Wrong input\n")
	raise e

splittedFile = file.map(lambda x: x.split(",")).filter(lambda x: x[0]!="Year" and x[18]!="NA" and x[14]!="NA" and x[15]!="NA" and int(x[15])>0)\
				.map(lambda x: (distanceGroup(x), (value(x), 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
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