import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import spark.implicits._

:load BoschFunction.scala

val RDDinitiales = sc.textFile("/train_categorical50.csv", 32)
val list_features0 = RDDinitiales.take(1)

val withoutFirstLine = RDDinitiales.filter(line => line != list_features0(0))


val list_features = fun.mapSplitString(list_features0(0))

val nbFeatures = list_features.length

val list_features_broadcast = sc.broadcast(list_features)
val nbFeatures_broadcast = sc.broadcast(nbFeatures)

val RDD1 = withoutFirstLine.map(fun.mapSplitString)


val RDD_features_FEATURES_value = RDD1.map(mapTo_FEATURES_id_value)
