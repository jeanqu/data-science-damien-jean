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

import org.apache.spark.mllib.linalg._

def addTwoArray(a1: Array[Double], a2: Array[Double]) = {
	var lineReturn = new Array[Double] (a1.length)
	for (i <- 0 to a1.length-1) 
	{
		lineReturn(i) = a1(i) + a2(i)
	}
	lineReturn
}

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._
val data = sc.parallelize(sampleCrosstab).zipWithIndex().map(line => (line._2, line._1.map(value => value.toDouble)))

val sumTotal = sc.broadcast(data.map(keyvalue => keyvalue._2.sum).sum())

val frequences = data.mapValues(values => values.map(eachValue => eachValue / sumTotal.value))

//.toDF("Id", "B", "C", "D", "F", "G", "H", "I", "L", "M","N", "P", "R", "S", "U", "W", "Y")
val rdd_sumlines = frequences.map(keyValues => keyValues._2.sum)


//On suppose que le vectuer "sumColonne" garde une taille relativement petite, on ne l'enregistre pas sous format distribué
val bro_sumColonnes = sc.broadcast(frequences.map(id_value => id_value._2).reduce((value1, value2) => addTwoArray(value1, value2)))

val nbLines = sc.broadcast(rdd_sumlines.count())

bro_sumColonnes.value

rdd_sumlines.take(2)

bro_sumColonnes.value

import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Matrices, DenseMatrix}

val vectors_sumline = rdd_sumlines.map(values => Vectors.dense(values))

//somme des lignes: RowMatrice distribuée d'une colonne et N lignes 
val rowMatrix_sumline: RowMatrix = new RowMatrix(vectors_sumline)

//somme des colonnes: Matrice locale d'une ligne et M colonnes
val matrix_sumColonnes_transpose = Matrices.dense(1, bro_sumColonnes.value.length, bro_sumColonnes.value)

//rowMatrix des Uij (produit de sumline i par sumCol j)
val rowMatrix_independence = rowMatrix_sumline.multiply(matrix_sumColonnes_transpose)

val rdd_independance = rowMatrix_independence.rows.map(vec => vec.toArray).zipWithIndex().map(line => (line._2, line._1))

rdd_independance.collect()


val jointure_frequence_independance = frequences.join(rdd_independance)

jointure_frequence_independance.take(1)

import scala.math._ 

def mapComputeStandardizedResiduals(fa: Array[Double], ia: Array[Double]) = {
    var lineReturn = new Array[Double] (fa.length)
	for (i <- 0 to fa.length-1) 
	{
		lineReturn(i) = math.abs(fa(i) - ia(i)) / math.sqrt(ia(i))
	}
	lineReturn
}

def computeReciprocalsSquareRootOneValue(valu: Double) = {
		1 / math.sqrt(valu)
}

import org.apache.spark.mllib.linalg.SingularValueDecomposition

val rdd_standardizedResiduals = jointure_frequence_independance.mapValues(line => mapComputeStandardizedResiduals(line._1, line._2))

val vectors_standardizedResiduals = rdd_standardizedResiduals.map(keyValues => Vectors.dense(keyValues._2))

val matrice_standardizedResiduals = new RowMatrix(vectors_standardizedResiduals)

val svd: SingularValueDecomposition[RowMatrix, Matrix] = matrice_standardizedResiduals.computeSVD(nbLines.value.toInt, computeU = true)

val U: RowMatrix = svd.U  
val s: Vector = svd.s  
val V: Matrix = svd.V 

//construction d'une matrice diagonale contenant les "reciprocals Squares Roots"
import org.apache.spark.mllib.linalg.{DenseMatrix,DenseVector}


val reciprocalsSquareRoot = rdd_sumlines.map(computeReciprocalsSquareRootOneValue)

//val sInverse = new DenseMatrix(2, 2, Matrices.diag(Vectors.dense(sInvArray)).toArray)

val vector_reciprocalSquareRoot = Vectors.dense(reciprocalsSquareRoot.collect())

val diagonalM_reciprocalSquareRoot = Matrices.diag(vector_reciprocalSquareRoot)
val diagonalM_s = Matrices.diag(s)



// val breeze_ReciprocalsSquareRoot: BDV[Double] = new BDV(reciprocalsSquareRoot.collect())
// val breeze_s: BDV[Double] = new BDV(s.toArray)

// val multi = breeze_ReciprocalsSquareRoot * breeze_s




//On suppose que la liste des sommes lignes peut exister en local (autre possibilité: effectuer une jointure et multiplier les éléments entre eux)

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix, BlockMatrix, CoordinateMatrix, MatrixEntry}


def matriceToRDD(m: Matrix) = {
  val columns = m.toArray.grouped(m.numRows)
  val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
  val vectors = rows.map(row => Vectors.dense(row.toArray))
  sc.parallelize(vectors)
}

def rowMatriceToRDD(rm: RowMatrix) = {
    rm.rows
}

def RDDToBlockMatrix(rd: RDD[Vector]) = {
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rd.zipWithIndex().map(line => new IndexedRow(line._2, line._1)))
    mat.toBlockMatrix()
}

def BlockMatrixToRDD(bl: BlockMatrix) = {
    bl.toIndexedRowMatrix().toRowMatrix().rows
}
def BlockMatrixToLocalarray(bl: BlockMatrix) = {
   matriceToRDD(bl.toLocalMatrix())
}

import org.apache.spark.mllib.util._

val blm_rsr = RDDToBlockMatrix(matriceToRDD(diagonalM_reciprocalSquareRoot))
val blm_U = RDDToBlockMatrix(rowMatriceToRDD(U))
val blm_s = RDDToBlockMatrix(matriceToRDD(diagonalM_s))

val row_score = blm_rsr.multiply(blm_U).multiply(blm_s)

val rdd_row_score = matriceToRDD(row_score.toLocalMatrix())


val arr = BlockMatrixToLocalarray(row_score).collect()

s

//val uu = toRDD(U).collect()

//val mult = rr.multiply(U).multiply(diagonalM_s).rows

//val arr= mult.collect()

//Vectors.sqdist(arr(0), arr(1))