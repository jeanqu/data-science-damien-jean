//Tentative d'implémentation d'un AFC en scala


import org.apache.spark.ml._
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, Vector => OldVector, Vectors => OldVectors}

:load packages/chiSquare
:load BoschFunction.scala


val data_feature = Array("ID", "INF05", "S0510", "S1020", "S2035", "S3550", "SUP50")
//Dans chaque case on a le nombre d'occurences de la variable "j" dans la catégorie "i"
val data = Array(Array("ARIE", "870", "330", "730", "680", "470", "890"), Array("AVER", "820", "1260", "2460", "3330", "2170", "2960"), Array("H.G.", "2290", "1070", "1420", "1830", "1260", "2330"), Array("GERS", "1650", "890", "1350", "2540", "2090", "3230"),Array("LOT", "1940", "1130", "1750", "1660", "770", "1140"), Array("H.P.", "2110", "1170", "1640", "1500", "550", "430"), Array("TARN", "1770", "820", "1260", "2010", "1680", "2090"), Array("T.G.", "1740", "920", "1560", "2210", "990", "1240"))





//Recréation PCA

import org.apache.spark.mllib.linalg.distributed.RowMatrix


val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(k)











val data = Array(
  Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
  Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
  Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))

val dataRDD = sc.parallelize(data, 2)

val mat: RowMatrix = new RowMatrix(dataRDD)



val data = Array(
Array(6.00, 6.00, 5.00, 5.50),
Array(8.00, 8.00, 8.00, 8.00),
Array(14.50, 14.50, 15.50, 15.00),
Array(14.00, 14.00, 12.00, 12.50),
Array(11.00, 10.00, 5.50, 7.00),
Array(5.50, 7.00, 14.00, 11.50),
Array(13.00, 12.50, 8.50, 9.50),
Array(9.00, 9.50, 12.50, 12.00))

val data = Array(
Array(11.39, 9.92, 2.66, 4.82),
Array(9.92, 8.94, 4.12, 5.48),
Array(2.66, 4.12, 12.06, 9.29),
Array(4.82, 5.48, 9.29, 7.91))

val dataVec = data.map(line => Vectors.dense(line))

val dataRDD = sc.parallelize(dataVec, 2)

//def mean(xs: Iterable[Double]) = xs.sum / xs.size

//val dataMEAN = dataRDD.map(line => line.map(x => (x - mean(line)) / mean(line)))


val mat: RowMatrix = new RowMatrix(dataRDD)

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

val cov: Matrix = mat.computeCovariance()

val cocov = new RowMatrix(matrixToRDD(cov))

val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(4)









val dataDF = dataMEAN.map(line => (line(0), line(1), line(2), line(3))).toDF("MATH", "PHYS", "FRAN", "ANGL")

dataDF.stat.cov("MATH", "PHYS")



Statistics.colStats(dataDF.take(1)




val dataCentre = dataDF.select($"MATH" - 10.125, $"PHYS" - 10.1875, $"FRAN" - 10.125, $"ANGL" - 10.125).toDF("MATH", "PHYS", "FRAN", "ANGL")

 val input: RDD[OldVector] = dataset.rdd.map {
  case Row(v: Vector) => OldVectors.fromML(v)
}



val mat: RowMatrix = new RowMatrix(dataVec)

val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(4)


val data = Array(
Array(37.00, 14.00, 51.00), 
Array(52.00, 15.00, 44.00), 
Array(33.00, 15.00, 63.00), 
Array(6.00, 1.00, 8.00))

val data = Array(("ARIE", Array(870, 330, 730, 680, 470, 890)),
	("AVER", Array(820, 1260, 2460, 3330, 2170, 2960)),
	("H.G.", Array(2290, 1070, 1420, 1830, 1260, 2330)),
	("GERS", Array(1650, 890, 1350, 2540, 2090, 3230)),
	("LOT", Array(1940, 1130, 1750, 1660, 770, 1140)),
	("H.P.", Array(2110, 1170, 1640, 1500, 550, 430)),
	("TARN", Array(1770, 820, 1260, 2010, 1680, 2090)),
	("T.G.", Array(1740, 920, 1560, 2210, 990, 1240))
)

val data = Array(
	("FR 870, 330, 730, 680, 470, 890"),
	("ESP 820, 1260, 2460, 3330, 2170, 2960"),
	("PLN 2290, 1070, 1420, 1830, 1260, 2330"),
	("EEU 1650, 890, 1350, 2540, 2090, 3230"),
	("GER 1940, 1130, 1750, 1660, 770, 1140"),
	("CHI 2110, 1170, 1640, 1500, 550, 430"),
	("ESP 1770, 820, 1260, 2010, 1680, 2090"),
	("CHN 1740, 920, 1560, 2210, 990, 1240")
)

val dataRDDinit = sc.parallelize(data, 2)

val dataRDD = dataRDDinit.map(id_value => (id_value._1, id_value._2.map(elem => elem.toDouble)))

val sumlines = sc.broadcast(dataRDD.map(ID_value => ID_value._2.sum).collect())
val sumColonnes = sc.broadcast(dataRDD.map(id_value => id_value._2).reduce((value1, value2) => addTwoArray(value1, value2)))

val sizeLine = sc.broadcast(sumColonnes.value.length)
val sizeColonne = sc.broadcast(sumlines.value.length)


val dataRDD_sumline = dataRDD.map(ID_value => (ID_value._1, (ID_value._2, ID_value._2.sum)))


val profilLine = dataRDD_sumline.map(ID_value_sum => (ID_value_sum._1, ID_value_sum._2._1.map(elem => elem/ID_value_sum._2._2)))

val profilColonne = dataRDD_sumline.map(ID_value_sum => (ID_value_sum._1, map_colonneFrequence(ID_value_sum._2._1, sumColonnes.value)))

val fSumColonne = sc.broadcast(profilLine.map(id_value => id_value._1).reduce((a1, a2) => addTwoArray(a1, a2)))
val fID_SumLine = profilColonne.map(id_value => (id_value._1, id_value._2.sum))

//val allCombinaisonsLine =  dataRDD_sumline.cartesian(dataRDD_sumline)

val profilLine_ID_value_sum = profilLine.join(fID_SumLine)

val chiLine = allCombinaisonsLine.combineByKey(initializeArrayOfDoubleSize(sizeLine.value))



val dataKhideux = dataRDD_sumline.map(line => map_datakhiDeux(line._1, line._2, sumColonnes.value, n.value))

val dataVector = dataKhideux.map(line => Vectors.dense(line))

val mat = new RowMatrix(dataVector)

val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(3)

//val dataVec = data.map(line => Vectors.dense(line))

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix

val chi2Array = profilLine_ID_value_sum.map(line => (1, line)).combineByKey(
																				(line: type_ID_value_sum) => initializeChiLineCalcul(line._2._1, line._2._2, fSumColonne.value) , 
																				(acc: schiTable , new_line: type_ID_value_sum) => addNewChiValue(acc, new_line._2._1, new_line._2._2, fSumColonne.value) ,
																				addTwoMatrix
																			)
									    .map(line => line._2)
									    .map(matrix => matrix.map(line => subTwoArray(line, fSumLine.value)))

val RDD_chi2Array = sc.parallelize(chi2Array.take(1)(0))

val (pc, explainedVariance) = mat.computePrincipalComponentsAndExplainedVariance(k)

import breeze.linalg.{axpy => brzAxpy, inv, svd => brzSvd, DenseMatrix => BDM, DenseVector => BDV, MatrixSingularException, SparseVector => BSV}
import breeze.numerics.{sqrt => brzSqrt}

val brzSvd.SVD(u: BDM[Double], s: BDV[Double], _) = brzSvd(mat)