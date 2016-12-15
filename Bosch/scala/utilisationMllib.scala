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

val RDDnumeric1 = sc.textFile("/train_numeric200.csv", 64)
val RDDnumeric2 = RDDnumeric1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val RDDnumeric3 = RDDnumeric2.map(mapSplitString)
val RDDnumeric3_corrected = RDDnumeric3.filter(line => line(0) != "Id")

val RDDnumeric4 = RDDnumeric3.zipWithIndex

//On veut juste sélectionner la colonne ID et la colonne réponse 
val features_numeric1 = RDDnumeric1.take(1)
val features_numeric2 = mapSplitString(features_numeric1(0))
val features_numeric2_brodcast = sc.broadcast(features_numeric2)
val nb_feature_num = features_numeric2.length
val nbFeatures_broadcast_num = sc.broadcast(nb_feature_num)

val RDDnumeric_id_feature_response_value = RDDnumeric3_corrected.map(line => mapTo_id_feature_response_value(line, nbFeatures_broadcast_num.value, features_numeric2_brodcast.value))
val RDDnumeric_all_id_feature_response_value = RDDnumeric_id_feature_response_value.flatMap(element => element)
val DFnumeric_id_feature_response_value = RDDnumeric_all_id_feature_response_value.toDF("Id", "feature", "response", "value")

val RDDnumeric_ID_response_index = RDDnumeric4.map(line => (line._1(0), line._1(nbFeatures_broadcast_num.value-1).toDouble, line._2))

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


//ALGORITHME TEST
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
s


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

val assembler = new VectorAssembler().setInputCols(categoricalFeatColNames).setOutputCol("features")

val data = assembler.transform(df_ID_response_allFeatures)

val data2 = data.select($"Id", $"response", $"features")

val trainingData_testData = data2.randomSplit(Array(0.7, 0.3))
val trainingData = trainingData_testData(0).toDF("Id", "response", "features").cache()
val testData = trainingData_testData(1).toDF("Id", "response", "features").cache()

val taData = trainingData.toDF()
val teData = testData.toDF()

taData.count()
teData.count()


//StringIndexer par colonne: trop long
val DF_all_Columns = spark.read.load("DF_all_datas")

val transformers: Array[org.apache.spark.ml.PipelineStage] = DF_all_Columns.drop("Id").columns.map(cname => new StringIndexer().setInputCol(cname).setOutputCol(s"${cname}_index"))
val assembler  = new VectorAssembler().setInputCols(DF_all_Columns.drop("Id").columns.map(cname => s"${cname}_index")).setOutputCol("features")
val stages = transformers :+ assembler
val pipeline1 = new Pipeline().setStages(stages)

val model = pipeline1.fit(DF_all_Columns)

val data = model.transform(DF_all_Columns)

val data_response = data.join(DF_ID_response_index, Seq("Id"))

data_response.write.save("dataResponse50.parquet")


val trainingData_testData = data_response.select($"Id", $"response", $"features").toDF("Id", "label", "features").randomSplit(Array(0.7, 0.3))
val trainingData = trainingData_testData(0).cache()
val testData = trainingData_testData(1).cache()

val taData = trainingData.toDF()
val teData = testData.toDF()

taData.count()
teData.count()

//Apprentissage valeurs numériques
val numericalFeatColNames = features_numeric2_brodcast.value.drop(1).dropRight(1)

val DFnumeric_allcolumn_Id_response_Value = DFnumeric_id_feature_response_value.groupBy("Id", "response").pivot("feature", numericalFeatColNames).sum("value")

val assembler = new VectorAssembler().setInputCols(numericalFeatColNames).setOutputCol("features")

val data_numeric = assembler.transform(DFnumeric_allcolumn_Id_response_Value).select("Id", "response", "features").toDF("Id", "label", "features").persist(StorageLevel.MEMORY_AND_DISK_SER )

val trainingData_testData = data_numeric.randomSplit(Array(0.7, 0.3))
val trainingData = trainingData_testData(0).toDF().cache()
val testData = trainingData_testData(1).toDF().cache()

val taData = trainingData.toDF()
val teData = testData.toDF()

taData.count()
teData.count()



//val DF_all_Columns = DF_three_columns.groupBy("Id").pivot("feature", categoricalFeatColNames).agg(simpleConcat($"value"))


//RADOM FOREST

val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(taData)

val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(5).fit(taData)

val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(100)

val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

val model = pipeline.fit(taData)

val predictions = model.transform(teData)


//ACP (PCA EN ANGLAIS) (Avec ML)

import org.apache.spark.ml.feature.{ PCA => MlPCA}
import org.apache.spark.ml.linalg.Vectors


val pca = new MlPCA().setInputCol("features").setOutputCol("pcaFeatures").setK(50).fit(data_numeric)

val pcaDF = pca.transform(data_numeric)
val result = pcaDF.select("Id", "label", "pcaFeatures")
result.show()

val data = result

val layers = Array[Int](50, 50, 50, 2)

//ACP (PCA), avec MLLIB
import org.apache.spark.mllib.feature.{ PCA => MllibPCA}
import org.apache.spark.mllib.linalg.{Vectors => MllibVectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

val withoutEmpty = RDDnumeric3_corrected.map(line => line.map(convertOneElemToDouble))

val withoutEmpty_key_value = withoutEmpty.map(line => (line(0), line.drop(1)))

val toVector = withoutEmpty_key_value.map(line => MllibVectors.dense(line._2))

val labbl = toVector.map(vec => new LabeledPoint(0, vec)) 

val pca = new MllibPCA(50).fit(labbl.map(_.features))

val projected = labbl.map(p => p.copy(features = pca.transform(p.features)))



//NEURAL NETWORK:
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val layers = Array[Int](numericalFeatColNames.length, 20, 2)

val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)

val data2 = data.select("label", "pcaFeatures").toDF("label", "features")

val model = trainer.fit(taData)

val result = model.transform(teData)

result.filter(result("prediction")===1.toDouble).show()
// train the model


//TEST:


predictions.select("predictedLabel", "indexedLabel", "features").show(5)

//val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
//val accuracy = evaluator.evaluate(predictions)

val predictions2 = predictions.withColumn("diff", abs($"prediction" - $"response"))


//COmpter le nombre d'éléments différents trouvés dans chaque feature
val df_nbe = DFnumeric_id_feature_response_value.groupBy("feature").agg(countDifferentElements($"value"))