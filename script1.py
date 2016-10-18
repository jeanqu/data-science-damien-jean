execfile('BoschFunction.py')

RDDinitiales = sc.textFile('/train_categorical.csv')

list_features = RDDinitiales.take(1)
withoutFirstLine = RDDinitiales.filter(filterWithoutFirstLine)


list_features = mapSplitString(list_features[0])
list_features_broadcast = sc.broadcast(list_features)


partOfInitialeRDD = withoutFirstLine.filter(filter_select_some_random_values)
RDD1 = partOfInitialeRDD.map(mapSplitString)


# On commence par chercher les éléments non nuls 


RDD_features_FEATURES_value = RDD2.map(mapTo_FEATURES_id_value)

RDD_all_values_FEATURE_id_value = RDD_features_FEATURES_value.flatMap(flatMap_withoutChanges)

partitionOne = RDD_all_values_FEATURE_id_value.partitionBy(24)

differentElemnPerFeature = partitionOne.reduceByKey(reduce_concatenateUniqueElements)
differentElemnPerFeature.count()
