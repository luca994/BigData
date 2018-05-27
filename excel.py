from sys import argv

inFile = open(argv[-1], "r")
outFile = open(argv[-1]+"_excel", "w")

splittedText="init"

while(splittedText[0]!=''):
	splittedText = inFile.readline().split("\t")
	if(len(splittedText)>1):
		col1 = splittedText[0]
		col2 = splittedText[1].split(".")
		if(len(col2)>1):
			line = col1+";"+col2[0]+","+col2[1]
		else:
			line = col1+","+col2[0]
		outFile.write(line)
outFile.close()