import sys
import re

def takeKey(elem):
	if(len(elem)==1):
		return 0;
	temp = elem[0].split("-")
	month=int(temp[1])+10
	if(len(temp)>2):
		valueString = re.search('\D{1,}', temp[2])
	if(len(temp)>2 and valueString==None):
		day=int(temp[2])+10
		temp = temp[0]+str(month)+str(day)
	else:
		temp=temp[0]+str(month)
	value = re.search('\d{1,}', temp)
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