import random

def mapSplitString(x):
	x = x.split(',')
	return x;

def filterExtractFirstLine(line):
	return line[0] == "Id"

def filterWithoutFirstLine(line):
	return line[0] != "Id"

def filter_select_some_random_values(line):
	return random.randint(1,10) == 1

def mapTo_FEATURES_id_value(line):
	line_return = [None] * (len(line) - 1)
	for i in range(0, (len(line) - 1)):
		line_return[i] = (list_features_var[i], (line[0], line[i+1]))
	return (line_return)

def flatMap_withoutChanges(element):
	return element

def reduce_concatenateUniqueElements(liste, (id, value)):
	if(value not in liste):
		return liste + value