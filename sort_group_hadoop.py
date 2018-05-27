import sys
import re

def takeKey(elem):
	value = re.search('\d{1,}', elem[0])
	if value!=None:
		value = value.group(0)
	else:
		value=0
	return int(value)

fileName = sys.argv[-2]
outputFileName = sys.argv[-1]
if outputFileName==None:
	outputFileName=fileName+"_sort"
file = open(fileName, "r")
fileWrite = open(outputFileName, "w")
text =file.read()
file.close()
textSplit = text.split("\n")
listKeyValue = []
for kv in textSplit:
	listKeyValue += [kv.split("\t")]
listKeyValue.sort(key=takeKey)
for el in listKeyValue:
	try:
		fileWrite.write(el[0]+"\t"+el[1]+"\n")
	except Exception as e:
		print("empty\n")	
fileWrite.close()