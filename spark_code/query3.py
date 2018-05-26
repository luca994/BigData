from pyspark import SparkContext
from sys import argv

sc=SparkContext("local", "query1")

file=None
try:
	file=sc.textFile(argv[1])
except Exception as e:
	print("Wrong input\n")
	raise e

splittedFile = file.map(lambda x: x.split(",")).filter(lambda x: x[0]!="Year")