//Implémentation de l'afc en spark en utilisant des dataframes, Ml et une méthode différente de celle utilisée précédemment. On se base sur cette méthode:
// http://www.mathematica-journal.com/2010/09/an-introduction-to-correspondence-analysis/


//Les données:
val authors = Array("harles Darwin", "Rene Descartes","Thomas Hobbes", "Mary Shelley", "Mark Twain")
val char = Array("B", "C", "D", "F", "G", "H", "I", "L", "M","N", "P", "R", "S", "U", "W", "Y")

val sampleCrosstab = Array(
Array(34, 37, 44, 27, 19, 39, 74, 44, 27, 61, 12, 65, 69, 22, 14, 21),
Array(18, 33, 47, 24, 14, 38, 66, 41, 36, 72, 15, 62, 63, 31, 12, 18),
Array(32, 43, 36, 12, 21, 51, 75, 33, 23, 60, 24, 68, 85, 18, 13, 14), 
Array(13, 31, 55, 29, 15, 62, 74, 43, 28, 73, 8, 59, 54, 32, 19, 20),
Array(8, 28, 34, 24, 17, 68, 75, 34, 25, 70, 16, 56, 72, 31, 14, 11), 
Array(9, 34, 43, 25, 18, 68, 84, 25, 32, 76, 14, 69, 64, 27, 11, 18),
Array(15, 20, 28, 18, 19, 65, 82, 34, 29, 89, 11, 47, 74, 18, 22, 17), 
Array(18, 14, 40, 25, 21, 60, 70, 15, 37, 80, 15, 65, 68, 21, 25, 9),
Array(19, 18, 41, 26, 19, 58, 64, 18, 38, 78, 15, 65, 72, 20, 20, 11), 
Array(13, 29, 49, 31, 16, 61, 73, 36, 29, 69, 13, 63, 58, 18, 20, 25),
Array(17, 34, 43, 29, 14, 62, 64, 26, 26, 71, 26, 78, 64, 21, 18, 12), 
Array(13, 22, 43, 16, 11, 70, 68, 46, 35, 57, 30, 71, 57, 19, 22, 20),
Array(16, 18, 56, 13, 27, 67, 61, 43, 20, 63, 14, 43, 67, 34, 41, 23), 
Array(15, 21, 66, 21, 19, 50, 62, 50, 24, 68, 14, 40, 58, 31, 36, 26),
Array(19, 17, 70, 12, 28, 53, 72, 39, 22, 71, 11, 40, 67, 25, 41, 17))

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._
import scala.util.Try
import org.apache.spark.sql.functions.udf



val rdd_data = sc.parallelize(sampleCrosstab).zipWithIndex().map(line => line._2 +: line._1).map(line => line.map(elem => elem.asInstanceOf[Number].doubleValue))
val rdd_key_value = rdd_data.map(line => line(0), line.pop(1))

val combinations = rdd_data.cartesian(rdd_data)


case class X(Id: Double, B: Double, C: Double, D: Double, F: Double, G: Double, H: Double, I: Double, L: Double, M: Double,N: Double, P: Double, R: Double, S: Double, U: Double, W: Double, Y: Double)

val n = rdd_data.map(line => line.sum())

val df_data = rdd_data.map(line => X(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16))).toDF()

val df_sumLine = df_data.select(char.map(col).reduce((c1, c2) => c1 + c2) as "sum")
val df_sumColonnes = df_data.groupBy().sum()

val n = sc.broadcast(nn.first().getDouble(0))
val rdd_dataTotalSum = data.map(line => line.map(elem => elem / n.value))



//Select only column "char";
data.select(char.map(data(_)): _*)

data.createOrReplaceTempView("data")

val sqlDF = spark.sql("SELECT * FROM data")
sqlDF.show()

val data = sc.parallelize(sampleCrosstab).zipWithIndex().toDF("Id", "B", "C", "D", "F", "G", "H", "I", "L", "M","N", "P", "R", "S", "U", "W", "Y")