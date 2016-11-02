//Initialisation

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


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
//Création dataFrame



case class X(feature: String, Id: String, value: String)
val DF_three_columns = RDD_feature_id_value.map (feature_id_values => X(feature_id_values._1, feature_id_values._2, feature_id_values._3)).toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)




import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

val df = spark.createDataFrame(Seq(
  (0, "a"),
  (1, "b"),
  (2, "c"),
  (3, "a"),
  (4, "a"),
  (5, "c")
)).toDF("id", "category")

val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
val indexed = indexer.transform(df)

val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
val encoded = encoder.transform(indexed)
encoded.select("id", "categoryVec").show()