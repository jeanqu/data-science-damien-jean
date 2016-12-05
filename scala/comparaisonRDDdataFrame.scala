import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()


:load BoschFunction.scala
:load packages/myUDF

val RDDinitiales = sc.textFile("/train_numeric200.csv", 64)
RDDinitiales.count

val RDDinitiales = sc.textFile("/train_categorical50.csv", 64)
val list_features0 = RDDinitiales.take(1)

//val withoutFirstLine = RDDinitiales.filter(line => line != list_features0(0))

val withoutFirstLine = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val list_features = mapSplitString(list_features0(0))

val nbFeatures = list_features.length

val list_features_broadcast = sc.broadcast(list_features)
val nbFeatures_broadcast = sc.broadcast(nbFeatures)


val RDD1 = withoutFirstLine.map(mapSplitString)


val RDD_features_FEATURES_value = RDD1.map(line => mapTo_FEATURES_id_value(line, nbFeatures_broadcast.value, list_features_broadcast.value))

val RDD_all_values_FEATURE_id_value = RDD_features_FEATURES_value.flatMap(element => element).persist(StorageLevel.MEMORY_AND_DISK_SER )

//Création du dataFrames

val RDD_feature_id_value = RDD_all_values_FEATURE_id_value.map(feature_id_values => (feature_id_values._1, feature_id_values._2._1, feature_id_values._2._2))


case class X(feature: String, Id: String, value: String)

val DF_three_columns = RDD_feature_id_value.map (feature_id_values => X(feature_id_values._1, feature_id_values._2, feature_id_values._3)).toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)

//2: Le nombre d’occurence de chaque variable ((‘’: ), (‘T1: 50000 fois”), …)

//RDD:
val RDD_VALUE_id_feature = RDD_feature_id_value.map(FEATURE_id_value => (FEATURE_id_value._3, (FEATURE_id_value._2, FEATURE_id_value._1)))

val RDD_countOccurenciVaraible = RDD_VALUE_id_feature.combineByKey((value: (String, String)) => 1, (conteur: Int, newValue: (String, String)) => conteur + 1, (conteur1:Int, conteur2:Int) => conteur1 + conteur2)

//DF:
val DF_countOccurenciVaraible = DF_three_columns.groupBy("value").count()

//3: Les patterns de features possibles et leurs nombre d’occurences ((“”, 25623541), (“T1, T32”,  52), ...)

//RDD
val RDD_ID_feature_value = RDD_feature_id_value.map(feature_id_values => (feature_id_values._2, (feature_id_values._1, feature_id_values._3)))

val RDD_ID_patternFeature = RDD_ID_feature_value.combineByKey(initializeConcatenate, combinerConcatenate , mergeConcatenate)

val RDD_PATTERN_id =  RDD_ID_patternFeature.map(id_pattern => (id_pattern._2, id_pattern._1))

val RDD_PATTERN_count = RDD_PATTERN_id.combineByKey((id: String) => 1, (conteur: Int, id: String) => conteur + 1, (conteur1: Int, conteur2: Int) => conteur1 + conteur2)

//dataFrame
val DF_id_PatternFeature = DF_three_columns.groupBy("Id").agg(GroupConcat($"feature", $"value")).toDF(Seq("Id", "PatternFeature"): _*)

val DF_countOccurenciPattern = DF_id_PatternFeature.groupBy("PatternFeature").count()

//Vérifications:

val rdd_features = DF_countOccurenciPattern.rdd.map(roww => (roww(0).toString, roww(1)))
val lf = rdd_features.leftOuterJoin(RDD_PATTERN_count)
val rr = lf.filter(KEY_one_two => KEY_one_two._2._2 == None)

//Sélection des colonnes non vides:
val DF_colonne_utilises = DF_three_columns.where($"value" !== "").groupBy("feature").count()

//Arbre de décision:
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import Array._

val RDDnumeric1 = sc.textFile("/train_numeric50.csv", 64)
val RDDnumeric2 = RDDnumeric1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val RDDnumeric3 = RDDnumeric2.map(mapSplitString)

//On veut juste sélectionner la colonne ID et la colonne réponse 
val features_numeric1 = RDDnumeric1.take(1)
val features_numeric2 = mapSplitString(features_numeric1(0))
val nb_feature_num = features_numeric2.length
val nbFeatures_broadcast_num = sc.broadcast(nb_feature_num)

//RDD
val RDDnumeric_ID_response = RDDnumeric3.map(line => (line(0), line(nbFeatures_broadcast_num.value-1)))

val RDD_ID_allvalues = RDD1.map(line => ("a", line.slice(1, nbFeatures_broadcast.value))).

val RDD_ID_allvalues_response = RDD_ID_allvalues.join(RDDnumeric_ID_response).persist(StorageLevel.MEMORY_AND_DISK_SER)

val RDD_id_response_allvaluescategorical = RDD_ID_allvalues_response.map(line => line._1 +: line._2._2 +: line._2._1)

//data Frame

val DF_ID_response = RDDnumeric_ID_response.toDF("Id", "response")

val DF_ID_allFeatures = spark.read.load("DF_all_datas")

val df_ID_response_allFeatures = DF_ID_response.join(DF_ID_allFeatures, DF_ID_response("Id") === DF_ID_allFeatures("Id"))

val df_partitionned = df_ID_response_allFeatures.repartition($"L0_S1_F25", 64)




val dataRDD = sc.parallelize(data, 2)
