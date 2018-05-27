from pyspark import SparkContext
from sys import argv

sc=SparkContext("local", "query5")

days = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

def mapKey(elem):
	group = (int(elem[4])//600)+1
	if(group==5):
		group=1
	return elem[0]+"-"+days[int(elem[3])-1]+"-"+str(group)

def mapValue(elem):
	value = 0
	if(int(elem[14])>0):
		value = 1
	return value

file=None
outputFileName=None
try:
	file=sc.textFile(argv[-2])
	outputFileName=argv[-1]
except Exception as e:
	print("Wrong input\n")
	raise e 

splittedFile = file.map(lambda x: x.split(",")).filter(lambda x: x[0]!="Year" and x[4]!="NA" and x[14]!="NA")\
					.map(lambda x: (mapKey(x), (mapValue(x), 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

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