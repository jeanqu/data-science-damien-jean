

def mapSplitString(s: String): Array[String] = { s.split(",", -1) }




// def mapTo_FEATURES_id_value(line: Array[String], nbFeatures: org.apache.spark.broadcast.Broadcast[Int], listFeatures: org.apache.spark.broadcast.Broadcast[Array[String]]): Array[Tuple2[String, Tuple2[String, String]]] = {
// 	var lineReturn:Array[Tuple2[String, Tuple2[String, String]]] = new Array[Tuple2[String, Tuple2[String, String]]](nbFeatures.value)
// 	for (i <- 1 to nbFeatures.value) {
// 		lineReturn(i-1) = (listFeatures.value(i), (line(0), line(i)))

// 	}
// 	lineReturn
// }

def mapTo_FEATURES_id_value(line: Array[String], nbFeatures: Int, listFeatures: Array[String]): Array[Tuple2[String, Tuple2[String, String]]] = {
	var lineReturn = new Array[Tuple2[String, Tuple2[String, String]]](nbFeatures-1)
	for (i <- 1 to nbFeatures-1) {
		lineReturn(i-1) = (listFeatures(i), (line(0), line(i)))

	}
	lineReturn
}


//Applicable seulement aux valeurs numériques
def mapTo_id_feature_response_value(line: Array[String], nbFeatures: Int, listFeatures: Array[String]): Array[Tuple4[Int, String, Double, Double]] = {
	var lineReturn = new Array[Tuple4[Int, String, Double, Double]](nbFeatures-2)
	var value = 9999999.toDouble
	for (i <- 1 to nbFeatures-2) {
		if (line(i) == "")
		{
			value = 9999999.toDouble
		}
		else
		{
			value = line(i).toDouble
		}
		lineReturn(i-1) = (line(0).toInt, listFeatures(i), line(nbFeatures-1).toDouble, value)
	}
	lineReturn
}

def initializeConcatenate(feature_value: (String, String)) = {
	if (feature_value._2 == "")
		"" 
	else 
		feature_value._1
}

def combinerConcatenate (pattern: String, feature_value: (String, String)) = {
	if (feature_value._2 == "") {
		pattern
	} 
	else {
		if (pattern.length() > 0 ){
			pattern + ", "  + feature_value._1
		}
		else {
			feature_value._1
		}
	}
}

def mergeConcatenate(pattern1: String, pattern2: String) = {
	if ((pattern1 != "") & (pattern2 != "")){
		pattern1 + ", " + pattern2
	}
	else {
		pattern1 + pattern2
	}
}

def convertOneElemToDouble(elem: String) = {
	if (elem == "")
	{
		0.toDouble
	}
	else
	{
		elem.toDouble
	}
}

import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._

def matrixToRDD(m: Matrix): RDD[Vector] = {
  val columns = m.toArray.grouped(m.numRows)
  val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
  val vectors = rows.map(row => new DenseVector(row.toArray))
  sc.parallelize(vectors)
}

def sqr(x: Double) = x * x

def khiDeux(nlh: Double, nl:Double, nc: Double, n: Double) = {
	val const = nl * nc / n
	sqr(nlh - const) / const
}

def map_datakhiDeux(line: Array[Double], nl:Double, array_nc: Array[Double], n: Double) = {
	var lineReturn = new Array[Double] (line.length)
	for (i <- 0 to lineReturn.length-1) 
	{
		lineReturn(i) = khiDeux(lineReturn(i), nl, array_nc(i), n)
	}
	lineReturn
}

def map_colonneFrequence(line: Array[Double], array_nc: Array[Double]) = {
	var lineReturn = new Array[Double] (line.length)
	for (i <- 0 to lineReturn.length-1) 
	{
		lineReturn(i) = line(i) / array_nc(i)
	}
	lineReturn
}

//marche seulement si a1 a autant d'éléments que a2
def addTwoArray(a1: Array[Double], a2: Array[Double]) = {
	var lineReturn = new Array[Double] (a1.length)
	for (i <- 0 to a1.length-1) 
	{
		lineReturn(i) = a1(i) + a2(i)
	}
	lineReturn
}

def constructOneElementOfMatriceLigne(j1: Array[Double], j2: Array[Double], array_fSumLine: Array[Double], sumj1: Double, sumj2: Double) = {
	var sum = 0.toDouble
	for (i <- 0 to j1.length-1) 
	{
		sum = sum + j1(i) * j2(i) / (array_fSumLine(i) * sumj2)
	}
	sum - sumj1
}

//fonction pour le combine du calcul de la mattrice de distance de Chi


type schiTable = Array[Array[Double]]
type type_ID_value_sum = (Long, (Array[Double], Double))

def addTwoMatrix(a1: schiTable, a2: schiTable) = {
	var lineReturn = new Array[Double] (a1.length)
	var matrixReturn = new schiTable (a1.length)
	for (l <- 0 to a1.length-1) 
	{
		for (i <- 0 to a1.length-1) 
		{
			lineReturn(i) = a1(l)(i) + a2(l)(i)
		}
		matrixReturn(l) = lineReturn
	}
	matrixReturn
}

def calculOneIterationChi2(fij1: Double, fij2: Double, fi: Double, fj2: Double) = {
	fij1 * fij2 / (fi * fj2)
}

def initializeChiLineCalcul(lineValue: Array[Double], sumLine: Double, array_fSumcol: Array[Double]) = {
	var lineReturn = new Array[Double] (lineValue.length)
	var matrixReturn = new schiTable (lineValue.length)
	for (l <- 0 to lineValue.length-1) 
	{
		for (i <- 0 to lineValue.length-1) 
		{
			lineReturn(i) = calculOneIterationChi2(lineValue(l), lineValue(i), sumLine, array_fSumcol(i))
		}
		matrixReturn(l) = lineReturn
	}
	matrixReturn
}

def addNewChiValue(arrayinit: schiTable, lineValue: Array[Double], sumLine: Double, array_fSumcol: Array[Double]) = {
	var lineReturn = new Array[Double] (lineValue.length)
	var matrixReturn = new schiTable (lineValue.length)
	for (l <- 0 to lineValue.length-1) 
	{
		for (i <- 0 to lineValue.length-1) 
		{
			lineReturn(i) = arrayinit(l)(i) + calculOneIterationChi2(lineValue(l), lineValue(i), sumLine, array_fSumcol(i))
		}
		matrixReturn(l) = lineReturn
	}
	matrixReturn
}

//Return an Array from the subtraction of an array and his zip index
def subTwoArrayWithIndex(a1_index: (Array[Double], Long), a2: Array[Double]) = {
	var lineReturn = new Array[Double] (a1_index._1.length)
	for (i <- 0 to a1_index._1.length - 1) 
	{
		lineReturn(i) = a1_index._1(i) - a2(a1_index._2.toInt)
	}
	lineReturn
}

//profilLine_ID_value_sum.map(line => (1, line)).combineByKey(
//																(line: type_ID_value_sum) => initializeChiLineCalcul(line._2._1, line._2._2, fSumColonne.value) , 
//																(acc: schiTable , new_line: type_ID_value_sum) => addNewChiValue(acc, new_line._2._1, new_line._2._2, fSumColonne.value) ,
//																addTwoMatrix
//															)


def constructMatriceLigne(line: Array[Double], lineSum: Array[Double], colonneSum: Array[Double]) = {
	var lineReturn = new Array[Double] (line.length)
	for (i <- 0 to a1.length-1) 
	{
		lineReturn(i) = a1(i) + a2(i)
	}
	lineReturn
}