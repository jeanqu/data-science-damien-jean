##On cherche ici à effectuer une analyse des données sur un maximum de données
from pyspark.sql import Row

#Toutes les fonctions de mapper, reduce... Sont définies dans ce fichier:

execfile('BoschFunction.py')

RDDinitiales = sc.textFile('/train_categorical.csv')

#On sépare les données de la première ligne (celle qui contient les noms des colonnes)

list_features = RDDinitiales.take(1)
withoutFirstLine = RDDinitiales.filter(lambda line: line != list_features[0])

#La variable list_features_broadcast contient tous les noms des colonnes (=les features) et peut être lu depuis tous les noeuds du cluster, en tant que broadcast variable 

list_features = mapSplitString(list_features[0])
#list_features = [e for e in list_features if e != "Id"]
nbFeatures = len(list_features)
list_features_broadcast = sc.broadcast(list_features)
nbFeatures_broadcast = sc.broadcast(nbFeatures)


#On ne prends qu'une partie des données sélectionnées aléatoirement

partOfInitialeRDD = withoutFirstLine.filter(filter_select_some_random_values)
RDD1 = partOfInitialeRDD.map(mapSplitString)


#Commençons par transformer le tableau de 2000 colonnes en un tableau de 3 colonnes sous la forme (feature, (Id, value))

RDD_features_FEATURES_value = RDD1.map(mapTo_FEATURES_id_value)
RDD_all_values_FEATURE_id_value = RDD_features_FEATURES_value.flatMap(flatMap_withoutChanges)

#Algo1: Grâce au partitionBy on met les données de même clés (ici "feature") dans les mêmes partitions
partitionOne = RDD_all_values_FEATURE_id_value.partitionBy(24)

#Algo2: On crée 2k listes contenant chaque colonne filtrée, puis on fait une union de ces listes. 2,6 min pour 5% des elements (beaucoup trop long)

#Algo3: On veut convertir notre tableau de données sous forme de dataFrame. On pourra ainsi manipuler les colonnes en plus des lignes
#Pour cela on doit convertir les lignes sous forme de "Row" (package SQL), puis on les convertit sous forme de dataFrame

#Problème: la fonction "Row" a une limite de 255 argument, on va donc créer plusieurs dataFrame puis on va les joindre 
i_feature = 1 
numero_dataframe = 0
arrayRowNamed = [None] * 11
#Séparation des données en 11 RDD organisées par Row SQL
while i_feature < nbFeatures_broadcast.value:
	j_feature = 0
	codeToCreateRowNamed = 'lambda line: Row(Id=line[0], '
	while (j_feature < 200) and (i_feature < nbFeatures_broadcast.value):
		codeToCreateRowNamed = codeToCreateRowNamed + str(list_features_broadcast.value[i_feature]) + '=line[' + str(i_feature) + '], '
		i_feature = i_feature + 1 
		j_feature = j_feature + 1
	codeToCreateRowNamed = codeToCreateRowNamed[:-2] + ')'
	arrayRowNamed[numero_dataframe] = RDD1.map(eval(codeToCreateRowNamed))
	numero_dataframe = numero_dataframe + 1

#On crée maintenant les dataFrames
array_df = [None] * 11
for i in range(0,len(arrayRowNamed)):
	array_df[i] = spark.createDataFrame(arrayRowNamed[i])

#On les concatène

DF_datas = array_df[0]
for i in range(1,len(array_df)):
	DF_datas = DF_datas.join(array_df[i], "Id")


#On cherche maintenant à compter les différentes apparitions de variables non nulles dans chaque feature
differentElemnPerFeature = partitionOne.combineByKey(lambda value: str(value[1]), 
	lambda value, x: value if (str(x[1]) in value) else str(x[1]) + value, 
	lambda x, y: x +y )
differentElemnPerFeature.count()

# 8 coeurs virtuels: algo1, 10% des données: 6 min
# 4 coeurs virtuels: algo1, 10% des données: 6,4 min 