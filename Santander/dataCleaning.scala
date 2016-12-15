
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()



:load function.scala

val RDDinitiales = sc.textFile("/train_ver2.csv", 48)

val RDDtest = sc.textFile("/test_ver2.csv", 48)

val RDDtest2 = RDDtest.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(line => (line.split(",")(1).replaceAll(" ", "").toInt, line.split(",")(2)))

val name_line_i = RDDinitiales.take(1).map(line => line.split(","))

val name_line = name_line_i(0)

val RDD2 = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val RDD3 = RDD2.map(w => w.replaceAll(" ", "")).map(line => line.split(","))

val RDD_lineTooLongRepared = RDD3.filter(line => line.length == 49).map(line => concatenateTwoElementsInArray(line, 20, 21))

val RDD_normal_size = RDD3.filter(line => line.length == 48)

val RDD4 = RDD_normal_size.union(RDD_lineTooLongRepared)

// tableau (ville => moyenne)

//Séparer les données en pas mai 2016 / mai 2016

val RDD_ID_features_values = RDD4.map(addMissingValue).map(line => (line(1).toInt, (takeOnlyFeaturesClient(line), takeOnlyGoodValues(line))))


//A tester sans
//val RDD_only_buying = RDD_ID_features_values.filter(line => line._2._2.sum != 0)

//ON veut un RDD sous la forme: (ID, (feature, values_not_may, values_may))

//Pour l'apprentissage:
val RDD_ID_features_valuesNotMay_valuesMay = RDD_ID_features_values.map(putToFormat_ID_features_valuesNotMay_valuesMay)

//Pour le test:
//val RDD_ID_features_valuesNotMay_valuesMay = RDD_ID_features_values.map(putToFormat_ID_features_valuesNotJanuary_valuesJanuary)

val RDD_ID_features_valuesNotMay_valuesMay_reduced = RDD_ID_features_valuesNotMay_valuesMay.combineByKey(feature_month_may => feature_month_may, (acc: (Array[String], Array[Int], Array[Int]), feature_month_may: (Array[String], Array[Int], Array[Int])) => (chooseLastFeatures(acc._1, feature_month_may._1), addTwoArray(acc._2, feature_month_may._2), addTwoArray(acc._3, feature_month_may._3)), (acc1: (Array[String], Array[Int], Array[Int]), acc2: (Array[String], Array[Int], Array[Int])) => (chooseLastFeatures(acc1._1, acc2._1), addTwoArray(acc1._2, acc2._2), addTwoArray(acc1._3, acc2._3)))

//val RDD_ID_features_valuesNotMay_newValuesMay = RDD_ID_features_valuesNotMay_valuesMay_reduced.mapValues(line => (line._1, line._2, keepOnlyIfDifferent(line._3, line._2)))

val RDD_ville_moyenne = RDD_ID_features_valuesNotMay_valuesMay_reduced.filter(line => line._2._1(21) != "").map(line => (line._2._1(19), (line._2._1(21).toDouble, 1.toDouble))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues(values => values._1 / values._2).map(line => (line._1.replaceAll("\"", ""), line._2))

val broadcast_RDD_ville_moyenne = sc.broadcast(RDD_ville_moyenne.collect().toMap)

val RDD_withoutMissingRevenues = RDD_ID_features_valuesNotMay_valuesMay_reduced.mapValues(line => addRevenuMissig(line, broadcast_RDD_ville_moyenne.value))

val age_sommeCount = RDD_withoutMissingRevenues.filter(line => line._2._1(4) != "NA").map(line => (line._2._1(4).toDouble, 1.toDouble)).reduce((somme_count1, somme_count2) => (somme_count1._1 + somme_count2._1,  somme_count1._2 + somme_count2._2))
val broadcast_middleAge = sc.broadcast(age_sommeCount._1 / age_sommeCount._2)

val RDD_withoutMissingAge = RDD_withoutMissingRevenues.mapValues(line => addAgeMissing(line, broadcast_middleAge.value))

val RDD_ID_categoricalValues_numeriqueValues_mayValues = RDD_withoutMissingAge.mapValues(line => (line._1.slice(1, 4) ++ Array(line._1(6)) ++ line._1.slice(8, 21) ++ Array(line._1(22)), Array(line._1(4).toDouble) ++ Array(line._1(7).toDouble ) ++ Array(line._1(21).toDouble ) ++ castArrayIntToDouble(line._2), line._3))




//On cherche maintenant à mettre les données categorical sous forme numeriques
import org.apache.spark.sql.{DataFrame,Row}

val featuresNames_IdAndCategorical = name_line.slice(1, 5) ++ Array(name_line(7)) ++ name_line.slice(9, 22) ++ Array(name_line(23))

val featuresNames_indexed_IdAndCategorical = Array(featuresNames_IdAndCategorical(0).replaceAll("\"", "")) ++ featuresNames_IdAndCategorical.slice(1, 19).map(names => names.replaceAll("\"", "") + "_indexed")

val schema = StructType( featuresNames_IdAndCategorical.map( label =>  StructField(label.replaceAll("\"", ""), StringType, false) ) )

val categorical_rows = RDD_ID_categoricalValues_numeriqueValues_mayValues.map(line => Array(line._1.toString) ++ line._2._1).map(line => line.toSeq).map(line => Row(line:_*))

//val d = RDD_ID_categoricalValues_numeriqueValues_mayValues.map(line => Array(line._1.toString) ++ line._2._1)
//val e = d.filter(line => lookingForValue(line, "2015-07-10"))






val df_categorical2 = spark.createDataFrame(categorical_rows, schema)

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}




val indexer = new StringIndexer().setInputCol("ind_empleado").setOutputCol("ind_empleadoIndex").fit(df_categorical)

val indexed = indexer.transform(df_categorical)

val id_newFeat = indexed.select($"ncodpers", $"ind_empleadoIndex")

var new_df = df_categorical

:load convertAllCategorialColumns.scala


// val parquetFileDF = spark.read.parquet("/categorical_features_df.parquet")
//val cat_df = new_df
new_df2.write.parquet("/categorical_features_TEST.parquet")
import org.apache.spark.sql.functions._

val parquetFileDF = spark.read.parquet("/categorical_features_df.parquet")

val cols = List("ncodpers", "ind_empleado_indexed", "pais_residencia_indexed", "sexo_indexed", "ind_nuevo_indexed", "indrel_indexed", "ult_fec_cli_1t_indexed", "indrel_1mes_indexed", "tiprel_1mes_indexed", "indresi_indexed", "indext_indexed", "conyuemp_indexed", "canal_entrada_indexed", "indfall_indexed", "tipodom_indexed", "cod_prov_indexed", "nomprov_indexed", "ind_actividad_cliente_indexed", "segmento_indexed")
// 118 pays différents

//val onlyInterestingColumns = parquetFileDF.select(cols.head, cols.tail: _*)
val onlyInterestingColumns = parquetFileDF.select(cols.head, cols.tail: _*)

//onlyInterestingColumns.write.parquet("/categorical_features_onlyInterestingColumns_TEST.parquet")


val backToArrayRDD = onlyInterestingColumns.rdd.map(line => fromRowToArrayOfDouble(line, 19)).map(line => (line(0).toInt, line.slice(1, 20)))

val RDD_join1 = RDD_ID_categoricalValues_numeriqueValues_mayValues.join(backToArrayRDD)

//Dans le cas de la variable test:
val RDD_withoutNonTestable = RDDtest2.join(RDD_join1).mapValues(line => line._2)

//On essaie sans la variable "pais"
val RDD_feature_previsions = RDD_withoutNonTestable.map(line => (line._1, (line._2._2.slice(0, 1) ++ line._2._2.slice(3, 20) ++ line._2._1._2, line._2._1._3)))

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row


//Avec une seule colonne (label choisie parmis toutes)
val RDDvectors_features_response = RDD_feature_previsions.map(line => (line._1, Vectors.dense(line._2._1), line._2._2(2)))

val df_RDD_features_label1 = RDDvectors_features_response.toDF(Seq("Id", "features", "labels"): _*)

//Avec un dataframe contenant tous les labels que l'on veut prédire: un par colonne.

val name_columns_to_predict = name_line.slice(24, 48)







//TOUT RATE

val RDDvectors_features_labels = RDD_feature_previsions.map(line => Array(line._1, Vectors.dense(line._2._1), line._2._2(0), line._2._2(1), line._2._2(2), line._2._2(3), line._2._2(4), line._2._2(5), line._2._2(6), line._2._2(7), line._2._2(8), line._2._2(9), line._2._2(10), line._2._2(11), line._2._2(12), line._2._2(13), line._2._2(14), line._2._2(15), line._2._2(16), line._2._2(17), line._2._2(18), line._2._2(19), line._2._2(20), line._2._2(21), line._2._2(22), line._2._2(23)))

val RDDvectors_features_labels3 = RDDvectors_features_labels.map(line => Row(line:_*))

val schema = StructType( Array(StructField("Id", IntegerType, false)) ++ Array(StructField("features", Vectors, false)) ++ name_columns_to_predict.map( label =>  StructField(label, IntegerType, false) ) )


val df_RDD_features_arrayLabel = RDDvectors_features_Arraylabels.toDF(Seq("Id", "features", "labels"): _*)


import org.apache.spark.sql.functions.udf

var i = 0
val selectElementI = (a:Array[Int]) => a(0)
val createColumnI = udf(selectElementI)

df_RDD_features_arrayLabel.withColumn("upper", createColumnI($"labels")).show

for (i <- 0 to name_columns_to_predict.length-1)
{

}

//Code sal.. :

val RDDD1 = RDD_feature_previsions.map(line => (line._1, Vectors.dense(line._2._1), line._2._2(0), line._2._2(1), line._2._2(2), line._2._2(3), line._2._2(4), line._2._2(5), line._2._2(6), line._2._2(7), line._2._2(8), line._2._2(9), line._2._2(10)))
val RDDD2 = RDD_feature_previsions.map(line => (line._1,  line._2._2(11), line._2._2(12), line._2._2(13), line._2._2(14), line._2._2(15), line._2._2(16), line._2._2(17), line._2._2(18), line._2._2(19), line._2._2(20), line._2._2(21), line._2._2(22), line._2._2(23)))

val df1 = RDDD1.toDF(Seq("Id", "features", "ind_ahor_fin_ult1", "ind_aval_fin_ult1", "ind_cco_fin_ult1", "ind_cder_fin_ult1", "ind_cno_fin_ult1", "ind_ctju_fin_ult1", "ind_ctma_fin_ult1", "ind_ctop_fin_ult1", "ind_ctpp_fin_ult1", "ind_deco_fin_ult1", "ind_deme_fin_ult1"): _*)
val df2 = RDDD2.toDF(Seq("Id", "ind_dela_fin_ult1", "ind_ecue_fin_ult1", "ind_fond_fin_ult1", "ind_hip_fin_ult1", "ind_plan_fin_ult1", "ind_pres_fin_ult1", "ind_reca_fin_ult1", "ind_tjcr_fin_ult1", "ind_valo_fin_ult1", "ind_viv_fin_ult1", "ind_nomina_ult1", "ind_nom_pens_ult1", "ind_recibo_ult1"): _*)

val DF_bonne = df1.join(df2, Seq("Id"))

DF_bonne.write.parquet("/santander_features_allLabelsSeparated_TEST.parquet")


:load algorithmeWithCleanedValues.scala



import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

val labelIndexer = new StringIndexer().setInputCol("labels").setOutputCol("indexedLabel").fit(df_RDD_features_label1)

val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(31).fit(df_RDD_features_label1)

val Array(trainingData, testData) = df_RDD_features_label1.randomSplit(Array(0.7, 0.3))

val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(15).setMaxDepth(20).setMaxBins(64)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

// Chain indexers and forest in a Pipeline.
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.

// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")


val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

//0.025 error

// 30 maxDepth: 0.024
