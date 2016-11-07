//Initialisation

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

val RDDnumeric_ID_response_index = RDDnumeric4.map(line => (line._1(0), line._1(nbFeatures_broadcast_num.value-1), line._2))

val DF_ID_response_index = RDDnumeric_ID_response_index.toDF("Id", "response", "index")

//Création dataFrame

val RDD_feature_id_value = RDD_all_values_FEATURE_id_value.map(feature_id_values => (feature_id_values._1, feature_id_values._2._1, feature_id_values._2._2))

case class X(feature: String, Id: String, value: String)
val DF_three_columns = RDD_feature_id_value.map (feature_id_values => X(feature_id_values._1, feature_id_values._2, feature_id_values._3)).toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)

val DF_id_PatternFeatureConcatenate = DF_three_columns.groupBy("Id").agg(GroupConcat($"feature", $"value")).toDF(Seq("Id", "PatternFeature"): _*)


val DF_id_patternFeatureArray = DF_id_PatternFeatureConcatenate.map(line => line.getAs[String]("Id"))

val DF_id_patternFeatureArray = DF_id_PatternFeatureConcatenate.select($"Id", split($"PatternFeature", (", ")))
val DF_id_response_patternFeatureArray = DF_id_patternFeatureArray.join(DF_ID_response_index, "Id").toDF(Seq("Id", "PatterneFeatures", "response", "index"): _*).persist(StorageLevel.MEMORY_AND_DISK_SER)

//Proba qu'un élément soit positif / négatif en sachant que l'élément précédent est positif / négatif

val moins1: String => String = {
	_.toUpperCase
}

val moins1UDF = udf(moins1)

val DF_element_precedent = DF_id_response_patternFeatureArray.withColumn("former_Id", moins1UDF('Id)).show()


//Test du modèle: INUTIL POUR LE MOMENT
val RDDinitiales_test = sc.textFile("/test_categorical50.csv", 64)
val withoutFirstLine_test = RDDinitiales_test.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val RDD1_test = withoutFirstLine_test.map(mapSplitString)
val RDD_features_FEATURES_value_test = RDD1_test.map(line => mapTo_FEATURES_id_value(line, nbFeatures_broadcast.value, list_features_broadcast.value))
val RDD_all_values_FEATURE_id_value_test = RDD_features_FEATURES_value_test.flatMap(element => element).persist(StorageLevel.MEMORY_AND_DISK_SER )
val RDD_feature_id_value_test = RDD_all_values_FEATURE_id_value_test.map(feature_id_values => (feature_id_values._1, feature_id_values._2._1, feature_id_values._2._2))
case class X(feature: String, Id: String, value: String)
val DF_three_columns_test = RDD_feature_id_value_test.map(feature_id_values => X(feature_id_values._1, feature_id_values._2, feature_id_values._3)).toDF()

val RDDnumeric1_test = sc.textFile("/test_numeric50.csv", 64)
val RDDnumeric2_test = RDDnumeric1_test.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val RDDnumeric3_test = RDDnumeric2_test.map(mapSplitString)
val RDDnumeric4_test = RDDnumeric3_test.zipWithIndex
//On veut juste sélectionner la colonne ID et la colonne réponse 

//ATTENTION: PAS DE VARIABLE "REPONSE" DANS LES DONNES TEST
val RDDnumeric_ID_response_index_test = RDDnumeric4_test.map(line => (line._1(0), line._1(nbFeatures_broadcast_num.value-1), line._2))
val DF_ID_response_index_test = RDDnumeric_ID_response_index_test.toDF("Id", "response", "index")


//RANDOM FOREST
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

//val data = DF_id_response_patternFeatureArray



val categoricalFeatColNames = list_features_broadcast.value.drop(1)


//val transformers: Array[org.apache.spark.ml.PipelineStage] = df_ID_response_allFeatures.columns.map(cname => new StringIndexer().setInputCol(cname).setOutputCol(s"${cname}_index"))


//TRAIN:
val indexer = new StringIndexer().setInputCol("value").setOutputCol("value_index")
val indexed = indexer.fit(DF_three_columns).transform(DF_three_columns)


val DF_all_Index_Value = indexed.groupBy("Id").pivot("feature", categoricalFeatColNames).sum("value_index")
val DF_all_Index_Value2 = DF_all_Index_Value.repartition(64)
//val df_ID_response_allFeatures = DF_ID_response_index.join(DF_all_Index_Value2, DF_ID_response_index("Id") === DF_all_Index_Value2("Id"))
val df_ID_response_allFeatures = DF_ID_response_index.join(DF_all_Index_Value2, Seq("Id"))

val assembler = new VectorAssembler().setInputCols(categoricalFeatColNames).setOutputCol("ListFeatures")

val data = assembler.transform(df_ID_response_allFeatures)

val data2 = data.select($"Id", $"response", $"ListFeatures")

val trainingData_testData = data2.randomSplit(Array(0.7, 0.3))
trainingData_testData(0).toDF("Id", "response", "ListFeatures").cache()
trainingData_testData(1).toDF("Id", "response", "ListFeatures").cache()
val trainingData = trainingData_testData(0).toDF("Id", "response", "ListFeatures").cache()
val testData = trainingData_testData(1).toDF("Id", "response", "ListFeatures").cache()

trainingData.count()
testData.count()


//TEST: ne marche pas car pas de variable "Réponse"
//val indexed_test = indexer.fit(DF_three_columns_test).transform(DF_three_columns_test)

//val DF_all_Index_Value_test = indexed_test.groupBy("Id").pivot("feature", categoricalFeatColNames).sum("value_index")
//val DF_all_Index_Value2_test = DF_all_Index_Value_test.repartition(64)
//val input_test = DF_ID_response_index_test.join(DF_all_Index_Value2_test, DF_ID_response_index_test("Id") === DF_all_Index_Value2_test("Id"))

//val assembler = new VectorAssembler().setInputCols(categoricalFeatColNames).setOutputCol("ListFeatures")

//val input = assembler.transform(df_ID_response_allFeatures_test)



val labelIndexer = new StringIndexer().setInputCol("response").setOutputCol("indexedLabel").fit(trainingData)

val featureIndexer = new VectorIndexer().setInputCol("ListFeatures").setOutputCol("indexedFeatures").setMaxCategories(30).fit(trainingData)

val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(100)

val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

val model = pipeline.fit(trainingData)

//TEST:

val predictions = model.transform(testData)

predictions.select("predictedLabel", "indexedLabel", "ListFeatures").show(5)

//val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
//val accuracy = evaluator.evaluate(predictions)

val predictions2 = predictions.withColumn("diff", abs($"prediction" - $"response"))

val accuracy = predictions2.groupBy($"newCol").agg(countError($"prediction", $"response"))




//A FAIRE: l'ensemble des features.
val all_features = list_features.drop(1) ++ features_numeric2.drop(1)

//A FAIRE: Joindre les catégorielles avec les numériques. On met le vecteur sur une colonne, pour mofifier les nulles.
val RDDnumeric_features_FEATURES_value = RDDnumeric3.map(line => mapTo_FEATURES_id_value(line, nbFeatures_broadcast.value, features_numeric2_brodcast.value))
val RDDnumeric_all_values_FEATURE_id_value = RDDnumeric_features_FEATURES_value.flatMap(element => element).persist(StorageLevel.MEMORY_AND_DISK_SER )


//A FAIRE: Trouver quoi faire des nulles, y a t il des valeurs négatives? == 0?
val without_nulles_values = 

//A FAIRE: Voir si on peut utiliser les datas dates (temps entre 2 features)


//Extractione des données dates (juste ID et Response)

val RDDnumeric1 = sc.textFile("/train_numeric50.csv", 64)
val RDDnumeric2 = RDDnumeric1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val RDDnumeric3 = RDDnumeric2.map(mapSplitString)
val RDDnumeric4 = RDDnumeric3.zipWithIndex