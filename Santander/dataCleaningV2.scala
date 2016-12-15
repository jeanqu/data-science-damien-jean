
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()



:load function.scala

val RDDinitiales = sc.textFile("/train_ver2.csv", 48)


val name_line_i = RDDinitiales.take(1).map(line => line.split(","))

val name_line = name_line_i(0)

val RDD2 = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val RDD3 = RDD2.map(w => w.replaceAll(" ", "")).map(line => line.split(","))

val RDD_lineTooLongRepared = RDD3.filter(line => line.length == 49).map(line => concatenateTwoElementsInArray(line, 20, 21))

val RDD_normal_size = RDD3.filter(line => line.length == 48)

val RDD4 = RDD_normal_size.union(RDD_lineTooLongRepared)

val RDD_ID_features_values = RDD4.map(addMissingValue).map(line => (line(1).toInt, (takeOnlyFeaturesClient(line), takeOnlyGoodValues(line))))


val RDD_only_dates = RDD_ID_features_values.map(line => (line._2._1(0), 1)).reduceByKey((v1, v2) => v1 + v2)
val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
val Map_date_Int = RDD_only_dates.map(line => (line._1, format.parse(line._1))).collect.sortBy(t => t._2).zipWithIndex.map(line => (line._1._1, line._2)).toMap

val RDD_IDDate_features_values = RDD_ID_features_values.map(line => ((line._1, Map_date_Int(line._2._1(0))), (line._2)))
val RDD_IDDate_features_sumValues = RDD_IDDate_features_values.reduceByKey((v1, v2) => (v1._1, addTwoArray(v1._2, v2._2)))