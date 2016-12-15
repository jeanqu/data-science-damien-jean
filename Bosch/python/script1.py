##On cherche ici à effectuer une analyse des données sur un maximum de données
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#Toutes les fonctions de mapper, reduce... Sont définies dans ce fichier:

execfile('BoschFunction.py')

RDDinitiales = sc.textFile('/train_categorical.csv', 32)
RDDinitiales = sc.textFile('/train_categorical50.csv', 32)

#On sépare les données de la première ligne (celle qui contient les noms des colonnes)

list_features0 = RDDinitiales.take(1)
withoutFirstLine = RDDinitiales.filter(lambda line: line != list_features0[0])

#withoutFirstLine = withoutFirstLine.top(50000)
#La variable list_features_broadcast contient tous les noms des colonnes (=les features) et peut être lu depuis tous les noeuds du cluster, en tant que broadcast variable 

list_features = mapSplitString(list_features0[0])

#list_features = [e for e in list_features if e != "Id"]

nbFeatures = len(list_features)
list_features_broadcast = sc.broadcast(list_features)
nbFeatures_broadcast = sc.broadcast(nbFeatures)


#On ne prends qu'une partie des données sélectionnées aléatoirement

#partOfInitialeRDD = withoutFirstLine
partOfInitialeRDD = withoutFirstLine.filter(filter_select_some_random_values)
RDD1 = partOfInitialeRDD.map(mapSplitString)


#Commençons par transformer le tableau de 2000 colonnes en un tableau de 3 colonnes sous la forme (feature, (Id, value))

RDD_features_FEATURES_value = RDD1.map(mapTo_FEATURES_id_value)
RDD_all_values_FEATURE_id_value = RDD_features_FEATURES_value.flatMap(flatMap_withoutChanges)

#Algo1: Grâce au partitionBy on met les données de même clés (ici "feature") dans les mêmes partitions

partitionOne = RDD_all_values_FEATURE_id_value.partitionBy(32).cache()



#On cherche maintenant à compter les différentes apparitions de variables non nulles dans chaque feature
differentElemnPerFeature = partitionOne.combineByKey(lambda value: str(value[1]), 
	lambda value, x: value if (str(x[1]) in value) else str(x[1]) + value, 
	lambda x, y: x +y )
differentElemnPerFeature.count()

RDD_features_FEATURES_value = RDD_all_values_FEATURE_id_value.map(lambda (feature, (id, value)): (value, 1)).reduceByKey(lambda a, b: a + b)
RDD_features_FEATURES_value = partitionOne.map(lambda (feature, (id, value)): (value, 1)).reduceByKey(lambda a, b: a + b)


countElemnOccurencies = partitionOne.combineByKey(lambda value: str(value[1]), 
	lambda value, x: value if (str(x[1]) in value) else str(x[1]) + value, 
	lambda x, y: x +y )


#Algo2: On crée 2k listes contenant chaque colonne filtrée, puis on fait une union de ces listes. 2,6 min pour 5% des elements (beaucoup trop long)

#Algo4: avec les dataFrames:

RDD_features_FEATURES_id_value_without_keys = RDD1.map(mapTo_FEATURES_id_value_without_keys)
RDD_all_values_FEATURE_id_value_without_keys = RDD_features_FEATURES_id_value_without_keys.flatMap(flatMap_withoutChanges)


DF_all_datas = spark.createDataFrame(RDD1, list_features)

DF_three_columns = spark.createDataFrame(RDD_all_values_FEATURE_id_value_without_keys, ['feature', 'Id', 'value'])

D2 = DF_three_columns.groupby("value", "feature").count().sort(desc("count"))
D3 = D2.groupby('feature').pivot('value').sum("count")
D3.show()

# 8 coeurs virtuels: algo1, 10% des données: 6 min
# 4 coeurs virtuels: algo1, 10% des données: 6,4 min 

#2 machines: 5% des données: 1,5 min

# Faire l'ACM