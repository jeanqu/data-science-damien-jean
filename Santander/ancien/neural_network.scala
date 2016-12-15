//neural network.
// On apprend le modèle sur le mois de mai, puis on l'applique au mois de juin

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()


def concatenateTwoElementsInArray(a: Array[String], i1: Int, i2: Int) = {
        a.slice(0, i1 - 1) ++ Array(a(i1) + a(i2)) ++ a.slice(i2, a.length + 1)
}

def putToFormat_ID_features_valuesNotMay_valuesMay(ID_feat_val: (Int, (Array[String], Array[Int]))) = {
        if (ID_feat_val._2._1(0) == "2016-05-28")
        {
        	(ID_feat_val._1, (ID_feat_val._2._1, Array.fill[Int](ID_feat_val._2._2.length)(0), ID_feat_val._2._2))
        }
        else
        {
        	(ID_feat_val._1, (ID_feat_val._2._1, ID_feat_val._2._2, Array.fill[Int](ID_feat_val._2._2.length)(0)))
        }
}

def castArrayIntToDouble(a: Array[Int]) = {
	var lineReturn = new Array[Double] (a.length)
	for (i <- 0 to a.length-1) 
	{
		lineReturn(i) = a(i).toDouble
	}
	lineReturn
}

:load function.scala

val RDDinitiales = sc.textFile("/train_ver2.csv", 72)

val name_line_i = RDDinitiales.take(1).map(line => line.split(","))

val name_line = name_line_i(0)

val RDD2 = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val RDD3 = RDD2.map(w => w.replaceAll(" ", ""))

val RDD_lineTooLongRepared = RDD3.filter(line => line.length == 49).map(line => concatenateTwoElementsInArray(line, 20, 21))

val RDD_normal_size = RDD3.filter(line => line.length == 48)

val RDD4 = RDD_normal_size.union(RDD_lineTooLongRepared)

//Séparer les données en pas mai 2016 / mai 2016

val RDD_ID_features_values = RDD4.map(line => (line(1).toInt, (takeOnlyFeaturesClient(line), takeOnlyGoodValues(line))))

val RDD_only_buying = RDD_ID_features_values.filter(line => line._2._2.sum != 0)

//ON veut un RDD sous la forme: (ID, (feature, values_not_may, values_may))

val RDD_ID_features_valuesNotMay_valuesMay = RDD_only_buying.map(putToFormat_ID_features_valuesNotMay_valuesMay)

val RDD_ID_features_valuesNotMay_valuesMay_reduced = RDD_ID_features_valuesNotMay_valuesMay.combineByKey(feature_month_may => feature_month_may, (acc: (Array[String], Array[Int], Array[Int]), feature_month_may: (Array[String], Array[Int], Array[Int])) => (feature_month_may._1, addTwoArray(acc._2, feature_month_may._2), addTwoArray(acc._3, feature_month_may._3)), (acc1: (Array[String], Array[Int], Array[Int]), acc2: (Array[String], Array[Int], Array[Int])) => (acc1._1, addTwoArray(acc1._2, acc2._2), addTwoArray(acc1._3, acc2._3)))

val RDD_without_may_nulle = RDD_ID_features_valuesNotMay_valuesMay_reduced.filter(line => line._2._3.sum != 0)

val RDD_toCSV = RDD_ID_features_valuesNotMay_valuesMay_reduced.map(line => Array(Array(line._1).toString, line._2._1.mkString(","), line._2._2.toString.mkString(","), line._2._3.toString.mkString(",")))

RDD_ID_features_valuesNotMay_valuesMay_reduced.saveAsTextFile("clients_santander")


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors => newVectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

val RDD_label1_vectors = RDD_without_may_nulle.map(ID_features_valuesNotMay_valuesMay => (ID_features_valuesNotMay_valuesMay._2._3(2).toDouble, newVectors.dense(castArrayIntToDouble(ID_features_valuesNotMay_valuesMay._2._2))))

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

//Obtenir un datafram avec toutes les colonnes:
val RDD5 = RDD4.map(line => line.toSeq)

val allColumns = Array("fecha_dato","ncodpers","ind_empleado","pais_residencia","sexo","age","fecha_alta","ind_nuevo","antiguedad","indrel","ult_fec_cli_1t","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall","tipodom","cod_prov","nomprov","ind_actividad_cliente","renta","segmento","ind_ahor_fin_ult1","ind_aval_fin_ult1","ind_cco_fin_ult1","ind_cder_fin_ult1","ind_cno_fin_ult1","ind_ctju_fin_ult1","ind_ctma_fin_ult1","ind_ctop_fin_ult1","ind_ctpp_fin_ult1","ind_deco_fin_ult1","ind_deme_fin_ult1","ind_dela_fin_ult1","ind_ecue_fin_ult1","ind_fond_fin_ult1","ind_hip_fin_ult1","ind_plan_fin_ult1","ind_pres_fin_ult1","ind_reca_fin_ult1","ind_tjcr_fin_ult1","ind_valo_fin_ult1","ind_viv_fin_ult1","ind_nomina_ult1","ind_nom_pens_ult1","ind_recibo_ult1")

val seq_allColumns = Seq("fecha_dato","ncodpers","ind_empleado","pais_residencia","sexo","age","fecha_alta","ind_nuevo","antiguedad","indrel","ult_fec_cli_1t","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall","tipodom","cod_prov","nomprov","ind_actividad_cliente","renta","segmento","ind_ahor_fin_ult1","ind_aval_fin_ult1","ind_cco_fin_ult1","ind_cder_fin_ult1","ind_cno_fin_ult1","ind_ctju_fin_ult1","ind_ctma_fin_ult1","ind_ctop_fin_ult1","ind_ctpp_fin_ult1","ind_deco_fin_ult1","ind_deme_fin_ult1","ind_dela_fin_ult1","ind_ecue_fin_ult1","ind_fond_fin_ult1","ind_hip_fin_ult1","ind_plan_fin_ult1","ind_pres_fin_ult1","ind_reca_fin_ult1","ind_tjcr_fin_ult1","ind_valo_fin_ult1","ind_viv_fin_ult1","ind_nomina_ult1","ind_nom_pens_ult1","ind_recibo_ult1")

val schema = StructType( allColumns.map( label =>  StructField(label, StringType, false) ) )

val DF1 = rows.toDF(seq_allColumns: _*)

import org.apache.spark.sql.{DataFrame,Row}

val rows = RDD5.map(x => Row(x:_*))

val df = spark.createDataFrame(rows, schema)



//Conversion en vectors:
import org.apache.spark.mllib.linalg.Vectors
