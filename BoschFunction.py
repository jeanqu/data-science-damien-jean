import random

def mapSplitString(x):
	x = x.split(',')
	return x;

def filterExtractFirstLine(line):
	return line[0] == "Id"

def filterWithoutFirstLine(line):
	return line[0] != "Id"

def filter_select_some_random_values(line):
	return random.randint(1,20) == 1

def mapTo_FEATURES_id_value(line):
	line_return = [None] * (nbFeatures_broadcast.value - 1)
	for i in range(1, nbFeatures_broadcast.value):
		line_return[i-1] = (list_features_broadcast.value[i], (line[0], line[i]))
	return (line_return)

def mapTo_FEATURES_id_value_without_keys(line):
	line_return = [None] * (nbFeatures_broadcast.value - 1)
	for i in range(1, nbFeatures_broadcast.value):
		line_return[i-1] = (list_features_broadcast.value[i], line[0], line[i])
	return (line_return)

def flatMap_withoutChanges(element):
	return element

def reduce_concatenateUniqueElements(liste, (id, value)):
	return liste + value

def isNotNone(value):
	return value is not None