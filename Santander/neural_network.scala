//neural network.
// On apprend le modèle sur le mois de mai, puis on l'applique au mois de juin

// /usr/local/spark/bin/spark-shell

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()



:load function.scala

val RDDinitiales = sc.textFile("train_ver2.csv", 36)

val name_line_i = RDDinitiales.take(1).map(line => line.split(","))

val name_line = name_line_i(0)

val RDD2 = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val RDD3 = RDD2.map(w => w.replaceAll(" ", "")).map(line => line.split(","))

val RDD_lineTooLongRepared = RDD3.filter(line => line.length == 49).map(line => concatenateTwoElementsInArray(line, 20, 21))

val RDD_normal_size = RDD3.filter(line => line.length == 48)

val RDD4 = RDD_normal_size.union(RDD_lineTooLongRepared)

val RDD5 = RDD4.map(line => addMissingValue(line, broadcast_RDD_ville_moyenne.value))

// tableau (ville => moyenne)


//Séparer les données en pas mai 2016 / mai 2016

val RDD_ID_features_values = RDD4.map(line => (line(1).toInt, (takeOnlyFeaturesClient(line), takeOnlyGoodValues(line))))

//A tester sans
//val RDD_only_buying = RDD_ID_features_values.filter(line => line._2._2.sum != 0)

//ON veut un RDD sous la forme: (ID, (feature, values_not_may, values_may))

val RDD_ID_features_valuesNotMay_valuesMay = RDD_ID_features_values.map(putToFormat_ID_features_valuesNotMay_valuesMay)

val RDD_ID_features_valuesNotMay_valuesMay_reduced = RDD_ID_features_valuesNotMay_valuesMay.combineByKey(feature_month_may => feature_month_may, (acc: (Array[String], Array[Int], Array[Int]), feature_month_may: (Array[String], Array[Int], Array[Int])) => (chooseLastFeatures(acc._1, feature_month_may._1), addTwoArray(acc._2, feature_month_may._2), addTwoArray(acc._3, feature_month_may._3)), (acc1: (Array[String], Array[Int], Array[Int]), acc2: (Array[String], Array[Int], Array[Int])) => (chooseLastFeatures(acc1._1, acc2._1), addTwoArray(acc1._2, acc2._2), addTwoArray(acc1._3, acc2._3)))

val RDD_ville_moyenne = RDD_ID_features_valuesNotMay_valuesMay_reduced.filter(line => line._2._1(21) != "").map(line => (line._2._1(19), (line._2._1(21).toDouble, 1.toDouble))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues(values => values._1 / values._2).map(line => (line._1.replaceAll("\"", ""), line._2))

val broadcast_RDD_ville_moyenne = sc.broadcast(RDD_ville_moyenne.collect().toMap)	
//On veut transformer les données "features" en données de type numériques.

(2015-01-28, N, ES, V, 55, 1998-09-01, 0, 203, 1, "", 1.0, I, S, N, "", KFC, N, 1, 48, "BIZKAIA", 0, "", 02-PARTICULARES)

"ind_empleado", "pais_residencia", "sexo", "age", "fecha_alta", "ind_nuevo", "antiguedad", "indrel", "ult_fec_cli_1t", "indrel_1mes", "tiprel_1mes", "indresi", "indext", "conyuemp", "canal_entrada", "indfall", "tipodom", "cod_prov", "nomprov", "ind_actividad_cliente", "renta", "segmento"

name_line.slice(2, 24)

def fromFeaturesToNumeriques(a: Array[String]) = {
        //On peut tester de garder al dernière date d'achat comme feature. On la modifie en numérique
        val date_last_buying = (a(0).split("-")(0).toDouble - 2014) * 12 + a(0).split("-")(1).toDouble
        (Array(date_last_buying, a(4).toDouble, a(7).toDouble, ))
}


//A tester sans
//val RDD_without_may_nulle = RDD_ID_features_valuesNotMay_valuesMay_reduced.filter(line => line._2._3.sum != 0)

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors => newVectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

val RDD_label1_vectors = RDD_ID_features_valuesNotMay_valuesMay_reduced.map(ID_features_valuesNotMay_valuesMay => (ID_features_valuesNotMay_valuesMay._2._3(2).toDouble, newVectors.dense(castArrayIntToDouble(ID_features_valuesNotMay_valuesMay._2._2))))

val df_RDD_label1_vectors = RDD_label1_vectors.toDF(Seq("label", "features"): _*)

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val splits = df_RDD_label1_vectors.randomSplit(Array(0.6, 0.4), seed = 1234L)
val train = splits(0)
val test = splits(1)

val layers = Array[Int](24, 24, 2)

val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)

val model = trainer.fit(df_RDD_label1_vectors)

val result = model.transform(test)

val predictionAndLabels = result.select("prediction", "label")

val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

println("Accuracy: " + evaluator.evaluate(predictionAndLabels))

// On cherche à obtenir les probabilités:
//ECHEC

val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)
val model = trainer.fit(df_RDD_label1_vectors);
val topoModel = FeedForwardTopology.multiLayerPerceptron(model.layers(), true).getInstance(model.weights());

//Gradient-boosted tree classifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

val RDD_label1_vectors = RDD_ID_features_valuesNotMay_valuesMay_reduced.map(ID_features_valuesNotMay_valuesMay => (ID_features_valuesNotMay_valuesMay._1, ID_features_valuesNotMay_valuesMay._2._3(2).toDouble, newVectors.dense(castArrayIntToDouble(ID_features_valuesNotMay_valuesMay._2._2))))

val df_RDD_label1_vectors = RDD_label1_vectors.toDF(Seq("Id", "label", "features"): _*)

val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df_RDD_label1_vectors)

val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(5).fit(df_RDD_label1_vectors)

val Array(trainingData, testData) = df_RDD_label1_vectors.randomSplit(Array(0.7, 0.3))

// Train a GBT model.
// Error (sur le 2): 0.03970481820311056
val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

val model = pipeline.fit(trainingData)

val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
println("Learned classification GBT model:\n" + gbtModel.toDebugString)



//Train a Random forest classifier
//On utilise les dataframes train et test précedents.
// Error (sur le 2): 0.045476314492363556

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

// Chain indexers and forest in a Pipeline.
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

// Train model. This also runs the indexers.
val model = pipeline.fit(trainingData)

// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

// Select (prediction, true label) and compute test error.
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
println("Learned classification forest model:\n" + rfModel.toDebugString)



//Obtenir un datafram avec toutes les colonnes:
val RDD5 = RDD4.map(line => line.toSeq)

val allColumns = Array("fecha_dato","ncodpers","ind_empleado","pais_residencia","sexo","age","fecha_alta","ind_nuevo","antiguedad","indrel","ult_fec_cli_1t","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall","tipodom","cod_prov","nomprov","ind_actividad_cliente","renta","segmento","ind_ahor_fin_ult1","ind_aval_fin_ult1","ind_cco_fin_ult1","ind_cder_fin_ult1","ind_cno_fin_ult1","ind_ctju_fin_ult1","ind_ctma_fin_ult1","ind_ctop_fin_ult1","ind_ctpp_fin_ult1","ind_deco_fin_ult1","ind_deme_fin_ult1","ind_dela_fin_ult1","ind_ecue_fin_ult1","ind_fond_fin_ult1","ind_hip_fin_ult1","ind_plan_fin_ult1","ind_pres_fin_ult1","ind_reca_fin_ult1","ind_tjcr_fin_ult1","ind_valo_fin_ult1","ind_viv_fin_ult1","ind_nomina_ult1","ind_nom_pens_ult1","ind_recibo_ult1")

val seq_allColumns = Seq("fecha_dato","ncodpers","ind_empleado","pais_residencia","sexo","age","fecha_alta","ind_nuevo","antiguedad","indrel","ult_fec_cli_1t","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall","tipodom","cod_prov","nomprov","ind_actividad_cliente","renta","segmento","ind_ahor_fin_ult1","ind_aval_fin_ult1","ind_cco_fin_ult1","ind_cder_fin_ult1","ind_cno_fin_ult1","ind_ctju_fin_ult1","ind_ctma_fin_ult1","ind_ctop_fin_ult1","ind_ctpp_fin_ult1","ind_deco_fin_ult1","ind_deme_fin_ult1","ind_dela_fin_ult1","ind_ecue_fin_ult1","ind_fond_fin_ult1","ind_hip_fin_ult1","ind_plan_fin_ult1","ind_pres_fin_ult1","ind_reca_fin_ult1","ind_tjcr_fin_ult1","ind_valo_fin_ult1","ind_viv_fin_ult1","ind_nomina_ult1","ind_nom_pens_ult1","ind_recibo_ult1")

val schema = StructType( allColumns.map( label =>  StructField(label, StringType, false) ) )

val DF1 = rows.toDF(seq_allColumns: _*)

import org.apache.spark.sql.{DataFrame,Row}

val rows = RDD5.map(x => Row(x:_*))

val df = spark.createDataFrame(rows, schema)


