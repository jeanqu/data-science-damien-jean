

// Prends des valeurs clean, peut importe le nombre de features pris en compte

// BUT: Retourner un tableau qui contient, pour chaque variable à prédire les probabilités d'avoir 1, 2, ou plus d'éléments
// Calculer ensuite les probas de chaque éléments en faisant: proba = somme(nbAcheté * proba)

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import spark.implicits._

import org.apache.spark.sql.{DataFrame,Row}


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

:load function.scala

val train = spark.read.parquet("/santander_features_allLabelsSeparated.parquet")
val test = spark.read.parquet("/santander_features_allLabelsSeparated_TEST.parquet")

val train_and_test = train.union(test)

val label_to_test = train_and_test.columns.slice(2, 30)


val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(31).fit(train_and_test)

val pipeline = new Pipeline().setStages(Array(featureIndexer))

val m = pipeline.fit(train_and_test)

val df_indexed_features_train = m.transform(train)
val df_indexed_features_test = m.transform(test)

var new_test_data = df_indexed_features_test

var Test_Error = new Array[Double] (label_to_test.length)

var i = 0

for (i <- 0 to label_to_test.length-1)
{      
	println(i)
	val label_tested = label_to_test(i)
	val new_label = label_tested + "label"

	val labelIndexer = new StringIndexer().setInputCol(label_tested).setOutputCol(new_label).fit(train_and_test)

	println("a")
	val rf = new RandomForestClassifier().setLabelCol(new_label).setFeaturesCol("indexedFeatures").setNumTrees(15).setMaxDepth(30).setMaxBins(128).setPredictionCol(label_tested + "_predi").setProbabilityCol(label_tested + "_proba").setRawPredictionCol(label_tested + "_rawPrediction")

	println("b")
	val labelConverter = new IndexToString().setInputCol(label_tested + "_predi").setOutputCol(label_tested + "_predictedLabel").setLabels(labelIndexer.labels)

	println("c")
	val pipeline = new Pipeline().setStages(Array(labelIndexer, rf, labelConverter))

	println("d")
	val model = pipeline.fit(df_indexed_features_train)

	println("e")
	new_test_data = model.transform(new_test_data)
	//new_test_data = model.transform(new_test_data)

	println("f")
	val evaluator = new MulticlassClassificationEvaluator().setLabelCol(new_label).setPredictionCol(label_tested + "_predi").setMetricName("accuracy")

	val accuracy = evaluator.evaluate(new_test_data)

	Test_Error(i) = 1.0 - accuracy
}

//al columns_predictions = data.columns ++ label_to_test.map(name => name + "_label") + label_to_test.map(name => name + "_proba")

//val DF_predictions = new_test_data.select(columns_predictions.head, columns_predictions.tail: _*)

new_test_data.write.parquet("/randomForest1_TEST.parquet")