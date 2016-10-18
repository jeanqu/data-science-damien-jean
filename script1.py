##On cherche ici à effectuer une analyse des données sur un maximum de données

#Toutes les fonctions de mapper, reduce... Sont définies dans ce fichier:

execfile('BoschFunction.py')

RDDinitiales = sc.textFile('/train_categorical.csv')

#On sépare les données de la première ligne (celle qui contient les noms des colonnes)

list_features = RDDinitiales.take(1)
withoutFirstLine = RDDinitiales.filter(filterWithoutFirstLine)

#La variable list_features_broadcast contient tous les noms des colonnes (=les features) et peut être lu depuis tous les noeuds du cluster, en tant que broadcast variable 

list_features = mapSplitString(list_features[0])
list_features_broadcast = sc.broadcast(list_features)

#On ne prends qu'une partie des données sélectionnées aléatoirement

partOfInitialeRDD = withoutFirstLine.filter(filter_select_some_random_values)
RDD1 = partOfInitialeRDD.map(mapSplitString)


#Commençons par transformer le tableau de 2000 colonnes en un tableau de 3 colonnes sous la forme (feature, (Id, value))

RDD_features_FEATURES_value = RDD1.map(mapTo_FEATURES_id_value)
RDD_all_values_FEATURE_id_value = RDD_features_FEATURES_value.flatMap(flatMap_withoutChanges)

#Grâce au partitionBy on met les données de même clés (ici "feature") dans les mêmes partitions

partitionOne = RDD_all_values_FEATURE_id_value.partitionBy(24)

partitionOne.cache().count()



#On cherche maintenant à compter les différentes apparitions de variables non nulles dans chaque feature
differentElemnPerFeature = partitionOne.combineByKey(lambda value: str(value[1]), lambda value, x: value if (str(x[1]) in value) else str(x[1]) + value, lambda x, y: x +y)
differentElemnPerFeature.count()