val RDDinitiales = sc.textFile("/train_ver2.csv", 72)

val name_line_i = RDDinitiales.take(1).map(line => line.split(","))

val name_line = name_line_i(0)

val RDD2 = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val RDD3 = RDD2.map(w => w.replaceAll(" ", "")).map(line => line.split(","))

import scala.util.control.Exception.allCatch

def isAllDigits(x: String) = x forall Character.isDigit

def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

def changeElemToIntOrDoubleOrStringOrminus1 (e: String) = {
	if (e == "")
	{
		e
	}
	else if (e == "NA")
	{
		-1.toInt
	}
	else if (isAllDigits(e))
	{
		e.toInt
	}
	else if (isDoubleNumber(e))
	{
		e.toDouble
	}
	else
	{
		e
	}
}

def checkIfSomeNull (l: Array[String]) = {
	var rep = true
	var i = 0
	while (i < l.length)
	{
		if (l(i) == "")
		{
			rep = false
		}
		i = i + 1
	}
	rep
}

def addTwoArray(a1: Array[Int], a2: Array[Int]) = {
	var lineReturn = new Array[Double] (a1.length)
	for (i <- 0 to a1.length-1) 
	{
		lineReturn(i) = a1(i) + a2(i)
	}
	lineReturn
}

def parseRating(str: String): Rating = {
  val fields = str.split(",").map(w => w.replaceAll(" ", "")).map(w => w.replaceAll("NA", "0"))
  Rating(fields(1).toInt, fields(25).toInt, fields(26).toInt, fields(27).toInt, fields(28).toInt, fields(29).toInt, fields(30).toInt, fields(31).toInt, fields(32).toInt, fields(33).toInt, fields(34).toInt, fields(35).toInt, fields(36).toInt, fields(37).toInt, fields(38).toInt, fields(39).toInt, fields(40).toInt, fields(41).toInt, fields(42).toInt, fields(43).toInt, fields(44).toInt, fields(45).toInt, fields(46).toInt, fields(47).toInt)
}

def parseAllString(str: String): allRows = {
  val fields = str.split(",").map(w => w.replaceAll(" ", ""))
  allRows(fields(0), fields(1), fields(2), fields(2), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10), fields(11), fields(12), fields(13), fields(14), fields(15), fields(16), fields(17), fields(18), fields(19), fields(20), fields(21), fields(22), fields(23), fields(24), fields(25), fields(26), fields(27), fields(28), fields(29), fields(30), fields(31), fields(32), fields(33), fields(34), fields(35), fields(36), fields(37), fields(38), fields(39), fields(40), fields(41), fields(42), fields(43), fields(44), fields(45), fields(46), fields(47))
}

def takeOnlyGoodValues(str: Array[String]): Array[Int] = {
	  val fields = str.map(w => w.replaceAll("NA", "0"))
	  Array(fields(24).toInt, fields(25).toInt, fields(26).toInt, fields(27).toInt, fields(28).toInt, fields(29).toInt, fields(30).toInt, fields(31).toInt, fields(32).toInt, fields(33).toInt, fields(34).toInt, fields(35).toInt, fields(36).toInt, fields(37).toInt, fields(38).toInt, fields(39).toInt, fields(40).toInt, fields(41).toInt, fields(42).toInt, fields(43).toInt, fields(44).toInt, fields(45).toInt, fields(46).toInt, fields(47).toInt)	
}
def takeOnlyFeaturesClient(str: Array[String]): Array[String] = {
	val fields = str
	Array(fields(0)) ++ fields.slice(2, 24)
}

def splitString(str: String): Array[String] = {
	str.split(",").map(w => w.replaceAll(" ", ""))
}

//Return an array with the (id_feature - value) 
def addIdFeatureToValue(key: Int, a: Array[Int]) = {
	var rep = new Array[Tuple3[Int, Int, Int]](a.length)
	var i: Int = 0
	while (i < a.length)
	{
		rep(i) = (key, i, a(i))
		i = i+1
	}
	rep
}

def concatValueToTuppleOfTwoArray(a1: Array[String], a2: Array[String]) = {
	var rep = new Array[Tuple2[String, String]](a1.length)
	var i: Int = 0
	while (i < a1.length)
	{
		rep(i) = (a1(i), a2(i))
		i = i + 1
	}
	rep
}

def concatValueToTuppleOfTwoArrayOfAny(a1: Array[String], a2: Array[Any]) = {
	var rep = new Array[Tuple2[String, Any]](a1.length)
	var i: Int = 0
	while (i < a1.length)
	{
		rep(i) = (a1(i), a2(i))
		i = i + 1
	}
	rep
}

:load function.scala

//val RDD4 = RDD3.map(line => line.map(changeElemToIntOrDoubleOrStringOrminus1))

//Sur 2.2g, 8 process / noeud: 1,1 min

//val RDD5 = RDD2.map(parseRating)

val RDD6 = RDD3.filter(line => line.length == 48).map(line => (line(1).toInt, (takeOnlyFeaturesClient(line), takeOnlyGoodValues(line))))

//Customer distinct
val RDD7 = RDD6.combineByKey(feature_shopping => feature_shopping, (acc: (Array[String], Array[Int]), feature_shopping: (Array[String], Array[Int])) => (feature_shopping._1, addTwoArray(acc._2, feature_shopping._2)), (acc1: (Array[String], Array[Int]), acc2: (Array[String], Array[Int])) => (acc1._1, addTwoArray(acc1._2, acc2._2)))

//without null dates
val reg = "^[0-9]{4}-[0-1][0-9]-[0-3][0-9]$"
val RDD8 = RDD7.map(key_value => (key_value._1, key_value._2._1, key_value._2._2)).filter(line => line._2(5).matches("^[0-9]{4}-[0-1][0-9]-[0-3][0-9]$")).filter(line => line._2(0).matches("^[0-9]{4}-[0-1][0-9]-[0-3][0-9]$"))

//to class
case class allRows(fecha_dato: String, ncodpers: Int, ind_empleado: String, pais_residencia: String, sexo: String, age: Int, fecha_alta: String, ind_nuevo: String, antiguedad: String, indrel: String, ult_fec_cli_1t: String, indrel_1mes: String, tiprel_1mes: String, indresi: String, indext: String, conyuemp: String, canal_entrada: String, indfall: String, tipodom: String, cod_prov: String, nomprov: String, ind_actividad_cliente: String, renta: Double, segmento: String, ind_ahor_fin_ult1: Int, ind_aval_fin_ult1: Int, ind_cco_fin_ult1: Int, ind_cder_fin_ult1: Int, ind_cno_fin_ult1: Int, ind_ctju_fin_ult1: Int, ind_ctma_fin_ult1: Int, ind_ctop_fin_ult1: Int, ind_ctpp_fin_ult1: Int, ind_deco_fin_ult1: Int, ind_deme_fin_ult1: Int, ind_dela_fin_ult1: Int, ind_ecue_fin_ult1: Int, ind_fond_fin_ult1: Int, ind_hip_fin_ult1: Int,ind_plan_fin_ult1: Int, ind_pres_fin_ult1: Int, ind_reca_fin_ult1: Int,ind_tjcr_fin_ult1: Int, ind_valo_fin_ult1: Int, ind_viv_fin_ult1: Int,ind_nomina_ult1: Int, ind_nom_pens_ult1: Int, ind_recibo_ult1: Int)

import sqlContext.implicits._


val RDD9 = RDD8.map(line => line._2.slice(0, 1) ++ Array(line._1) ++ line._2.slice(1, 24) ++ line._3)

val RDD10 = RDD9.map(line => concatValueToTuppleOfTwoArrayOfAny(name_line, line).toMap)

val RDD_direct_to_elastic_done = RDD10.saveToEs("san2/any")


val RDD9 = RDD8.map(d => new allRows(d._2(0), d._1, d._2(1), d._2(2), d._2(3), d._2(4).toInt, d._2(5), d._2(6), d._2(7), d._2(8), d._2(9), d._2(10), d._2(11), d._2(12), d._2(13), d._2(14), d._2(15), d._2(16), d._2(17), d._2(18), d._2(19), d._2(20), if (d._2(21) == "") 0 else d._2(21).toDouble, d._2(22), d._3(0), d._3(1), d._3(2), d._3(3), d._3(4), d._3(5), d._3(6), d._3(7), d._3(8), d._3(9), d._3(10), d._3(11), d._3(12), d._3(13), d._3(14), d._3(15), d._3(16), d._3(17), d._3(18), d._3(19), d._3(20), d._3(21), d._3(22), d._3(23)))


val RDD_key_value = RDD2.map(line => parseRating(line)).map(line => (line(0), line.slice(1, 25)))

val RDD_Id_feature_value = RDD_key_value.map(key_value => addIdFeatureToValue(key_value._1, key_value._2)).flatMap(line => line)

import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

//indice 1 et 25:47
val usecols = Array("ncodpers", "ind_ahor_fin_ult1", "ind_aval_fin_ult1", "ind_cco_fin_ult1", "ind_cder_fin_ult1", "ind_cno_fin_ult1", "ind_ctju_fin_ult1", "ind_ctma_fin_ult1", "ind_ctop_fin_ult1", "ind_ctpp_fin_ult1", "ind_deco_fin_ult1", "ind_deme_fin_ult1", "ind_dela_fin_ult1", "ind_ecue_fin_ult1", "ind_fond_fin_ult1", "ind_hip_fin_ult1","ind_plan_fin_ult1", "ind_pres_fin_ult1", "ind_reca_fin_ult1","ind_tjcr_fin_ult1", "ind_valo_fin_ult1", "ind_viv_fin_ult1","ind_nomina_ult1", "ind_nom_pens_ult1", "ind_recibo_ult1")

case class Rating(ncodpers: Int, ind_ahor_fin_ult1: Int, ind_aval_fin_ult1: Int, ind_cco_fin_ult1: Int, ind_cder_fin_ult1: Int, ind_cno_fin_ult1: Int, ind_ctju_fin_ult1: Int, ind_ctma_fin_ult1: Int, ind_ctop_fin_ult1: Int, ind_ctpp_fin_ult1: Int, ind_deco_fin_ult1: Int, ind_deme_fin_ult1: Int, ind_dela_fin_ult1: Int, ind_ecue_fin_ult1: Int, ind_fond_fin_ult1: Int, ind_hip_fin_ult1: Int,ind_plan_fin_ult1: Int, ind_pres_fin_ult1: Int, ind_reca_fin_ult1: Int,ind_tjcr_fin_ult1: Int, ind_valo_fin_ult1: Int, ind_viv_fin_ult1: Int,ind_nomina_ult1: Int, ind_nom_pens_ult1: Int, ind_recibo_ult1: Int)

val allColumns = Array("fecha_dato","ncodpers","ind_empleado","pais_residencia","sexo","age","fecha_alta","ind_nuevo","antiguedad","indrel","ult_fec_cli_1t","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall","tipodom","cod_prov","nomprov","ind_actividad_cliente","renta","segmento","ind_ahor_fin_ult1","ind_aval_fin_ult1","ind_cco_fin_ult1","ind_cder_fin_ult1","ind_cno_fin_ult1","ind_ctju_fin_ult1","ind_ctma_fin_ult1","ind_ctop_fin_ult1","ind_ctpp_fin_ult1","ind_deco_fin_ult1","ind_deme_fin_ult1","ind_dela_fin_ult1","ind_ecue_fin_ult1","ind_fond_fin_ult1","ind_hip_fin_ult1","ind_plan_fin_ult1","ind_pres_fin_ult1","ind_reca_fin_ult1","ind_tjcr_fin_ult1","ind_valo_fin_ult1","ind_viv_fin_ult1","ind_nomina_ult1","ind_nom_pens_ult1","ind_recibo_ult1")

case class allRows(fecha_dato: String, ncodpers: Int, ind_empleado: String, pais_residencia: String, sexo: String, age: Int, fecha_alta: String, ind_nuevo: String, antiguedad: String, indrel: String, ult_fec_cli_1t: String, indrel_1mes: String, tiprel_1mes: String, indresi: String, indext: String, conyuemp: String, canal_entrada: String, indfall: String, tipodom: String, cod_prov: String, nomprov: String, ind_actividad_cliente: String, renta: Int, segmento: String, ind_ahor_fin_ult1: Int, ind_aval_fin_ult1: Int, ind_cco_fin_ult1: Int, ind_cder_fin_ult1: Int, ind_cno_fin_ult1: Int, ind_ctju_fin_ult1: Int, ind_ctma_fin_ult1: Int, ind_ctop_fin_ult1: Int, ind_ctpp_fin_ult1: Int, ind_deco_fin_ult1: Int, ind_deme_fin_ult1: Int, ind_dela_fin_ult1: Int, ind_ecue_fin_ult1: Int, ind_fond_fin_ult1: Int, ind_hip_fin_ult1: Int,ind_plan_fin_ult1: Int, ind_pres_fin_ult1: Int, ind_reca_fin_ult1: Int,ind_tjcr_fin_ult1: Int, ind_valo_fin_ult1: Int, ind_viv_fin_ult1: Int,ind_nomina_ult1: Int, ind_nom_pens_ult1: Int, ind_recibo_ult1: Int)

val df_Id_feature_value = RDD_Id_feature_valu.etoDF(Seq("userId", "featureId", "numberElements"): _*)

//val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("featureId").setRatingCol("numberElements")

val model = als.fit(df_Id_feature_value)


val predictions = model.transform(test)

val RDD_elastic = RDD2.map(splitString)
val RDD_same_size = RDD_elastic.filter(line => line.length == 48)

val reg = "^[0-9]{4}-[0-1][0-9]-[0-3][0-9]$"
val RDD_only_dates = RDD_same_size.filter(line => line(6).matches(reg) && line(0).matches(reg)) 

val RDD_withoutshoppingNull = RDD_only_dates.map(line => line.map(e => if (e == "NA") "-1" else e))

val RDD_elastic2 = RDD2.map(parseAllString)

val df_elastic = RDD_elastic2.toDF()

df_elastic.write.json("/jsonSantanderStringAll")

// Spark -> hadoop: 10 min.
// Hadoop -> local: 28min.
// Local -> elastisearch: ?

import org.elasticsearch.spark._

val RDD_direct_to_elastic = RDD_withoutshoppingNull.map(line => concatValueToTuppleOfTwoArray(name_line, line).toMap)

//val RDD_direct_to_elastic_bis = RDD_elastic.map(line => line.zipWithIndex.map(t => (t._2, t._1)).toMap)

val RDD_direct_to_elastic_done = RDD_direct_to_elastic.saveToEs("new_allsand/string")

//direct: Spark -> elaasticsearch: 32 min. Peut etre du au fait que toutes les données sont des string?

for (i <- 0 to name_line(0).split(",").length-1) {print("line(" + i.toString + "), ")}

// Contons les différents patterns:





curl -XPUT 'http://13.95.25.191:9200/blog/user/dilbert' -d '{ "name" : "Dilbert Brown" }'