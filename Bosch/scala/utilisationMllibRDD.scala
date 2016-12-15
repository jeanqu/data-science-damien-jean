
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.udf

import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

:load BoschFunction.scala
:load packages/myUDF

val RDDinitiales = sc.textFile("/train_categorical50.csv", 64)

//Création RDD
val list_features0 = RDDinitiales.take(1)
val withoutFirstLine = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val list_features = mapSplitString(list_features0(0))
val nbFeatures = list_features.length
val list_features_broadcast = sc.broadcast(list_features)
val nbFeatures_broadcast = sc.broadcast(nbFeatures)
val RDD1 = withoutFirstLine.map(mapSplitString)


val RDD_features_FEATURES_value = RDD1.map(line => mapTo_FEATURES_id_value(line, nbFeatures_broadcast.value, list_features_broadcast.value))
val RDD_all_values_FEATURE_id_value = RDD_features_FEATURES_value.flatMap(element => element).persist(StorageLevel.MEMORY_AND_DISK_SER )

//Extractione des données dates (juste ID et Response)

val RDDnumeric1 = sc.textFile("/train_numeric50.csv", 64)
val RDDnumeric2 = RDDnumeric1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val RDDnumeric3 = RDDnumeric2.map(mapSplitString)
val RDDnumeric4 = RDDnumeric3.zipWithIndex

//On veut juste sélectionner la colonne ID et la colonne réponse 
val features_numeric1 = RDDnumeric1.take(1)
val features_numeric2 = mapSplitString(features_numeric1(0))
val features_numeric2_brodcast = sc.broadcast(features_numeric2)
val nb_feature_num = features_numeric2.length
val nbFeatures_broadcast_num = sc.broadcast(nb_feature_num)

val RDDnumeric_id_feature_response_value = RDDnumeric3.map(line => mapTo_id_feature_response_value(line, nbFeatures_broadcast_num.value, features_numeric2_brodcast.value))
val RDDnumeric_all_id_feature_response_value = RDDnumeric_id_feature_response_value.flatMap(element => element)

val RDDDouble = RDDnumeric3.map(line => line.map(convertOneElemToDouble))

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

val RDDLablePoint = RDDDouble.map(line => LabeledPoint(line(nbFeatures_broadcast_num.value-1), Vectors.dense(line.slice(1, nbFeatures_broadcast_num.value-2))))

val splits = RDDLablePoint.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 10000 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "gini"
val maxDepth = 8
val maxBins = 512

val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

val labelAndPreds = testData.map { point => (point.label, model.predict(point.features)) }

val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()

import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

val pca = new PCA(5).fit(RDDLablePoint.map(_.features))

val projected = RDDLablePoint.map(p => p.copy(features = pca.transform(p.features)))

