#Ce script donne les résultats du document https://docs.google.com/document/d/1R8mPVqjG4nU9EfbA_CxmrolJOR0dgUilUyHq13INIFU/edit

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

#1: création puis mise en cache du RDD et du dataframe. On garde 50 000 données
#On les crée sous la forme (Feature, (Id, Value))

RDDinitiales = sc.textFile('/train_categorical50.csv', 64)


list_features0 = RDDinitiales.take(1)
withoutFirstLine = RDDinitiales.filter(lambda line: line != list_features0[0])

#withoutFirstLine = withoutFirstLine.top(50000)
#La variable list_features_broadcast contient tous les noms des colonnes (=les features) et peut être lu depuis tous les noeuds du cluster, en tant que broadcast variable 

list_features = mapSplitString(list_features0[0])


nbFeatures = len(list_features)
list_features_broadcast = sc.broadcast(list_features)
nbFeatures_broadcast = sc.broadcast(nbFeatures)


#On ne prends qu'une partie des données sélectionnées aléatoirement

#partOfInitialeRDD = withoutFirstLine
RDD1 = withoutFirstLine.map(mapSplitString)

RDD_features_FEATURES_value = RDD1.map(mapTo_FEATURES_id_value)
RDD_all_values_FEATURE_id_value = RDD_features_FEATURES_value.flatMap(flatMap_withoutChanges).cache()

RDD_all_values_FEATURE_id_value.count()

#Création du dataFrame; sur 3 colonnes:

RDD_features_FEATURES_id_value_without_keys = RDD1.map(mapTo_FEATURES_id_value_without_keys)
RDD_all_values_FEATURE_id_value_without_keys = RDD_features_FEATURES_id_value_without_keys.flatMap(flatMap_withoutChanges)
DF_three_columns = spark.createDataFrame(RDD_all_values_FEATURE_id_value_without_keys, ['feature', 'Id', 'value']).cache()

#2: Le nombre d’occurence de chaque variable ((‘’: ), (‘T1: 50000 fois”), …)

#RDD:
RDD_VALUE_id_feature = RDD_all_values_FEATURE_id_value.map(lambda (feature, (id, value)): (value, (id, feature)))
RDD_countOccurenciVaraible = RDD_VALUE_id_feature.combineByKey(lambda value: 1, 
	lambda conteur, newValue: conteur + 1, 
	lambda value1, value2: value1 + value2)

#DataFrame:
DF_countOccurenciVaraible = DF_three_columns.groupby("value").count()

#3: Les patterns de features possibles et leurs nombre d’occurences ((“”, 25623541), (“T1, T32”,  52), ...)

#RDD
RDD_ID_feature_value = RDD_all_values_FEATURE_id_value.map(lambda (feature, (id, value)): (id, (feature, value)))

RDD_ID_patternFeature = RDD_ID_feature_value.combineByKey(lambda (feature, value): feature, 
	lambda pattern, (feature, value): pattern if (value == "") else pattern + ', ' + feature, 
	lambda pattern1, pattern2: pattern1 + ', ' + pattern2)

RDD_PATTERN_id =  RDD_ID_patternFeature.map(lambda (id, pattern): (pattern, id))
RDD_PATTERN_count = RDD_PATTERN_id.combineByKey(lambda id: 1, 
	lambda conteur, id: conteur + 1, 
	lambda conteur1, conteur2: conteur1 + conteur2)

#dataFrame
DF_countOccurenciPattern = DF_three_columns.groupby("ID")