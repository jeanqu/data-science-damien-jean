import org.apache.spark.sql.{DataFrame,Row}

import org.apache.spark.mllib.linalg.{DenseVector => oldDenseVector}
import org.apache.spark.ml.linalg.{DenseVector => newDenseVector}
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD

:load function.scala


val data_exemple = spark.read.parquet("/randomForest1.parquet")
val data = spark.read.parquet("/randomForest1_TEST.parquet")
//val data = new_test_data


val original_column = data.columns.slice(2, 26)

val ID = data.columns(0)
val Features = data.columns(1)
val label = original_column.map(c => c + "label")
val labels_predicted = original_column.map(c => c + "_predictedLabel")
val predicted = original_column.map(c => c + "_predi")
val probability = original_column.map(c => c + "_proba")

// If predictedlabel == predi.toInt, on prend la deuxième valeur "proba". Sinon la première.





data.select("ind_ctop_fin_ult1", "ind_ctop_fin_ult1_predictedLabel", "ind_ctop_fin_ult1_predi", "ind_ctop_fin_ult1_proba").show()



val inputRDD: RDD[newDenseVector] = data.rdd.map(row => row.getAs[newDenseVector]("ind_ahor_fin_ult1_proba"))
inputRDD.take(1)




val RDD1 = data.rdd.map(r => convertRowPredictionsToRDD(r, Features, original_column, probability, labels_predicted, predicted))

val RDD2 = RDD1.map(line =>(line._1, line._2, line._3, line._4.zipWithIndex.sortBy(t => -t._1).slice(0,7)))

val RDD_chosenElements = RDD2.map(line => line._1.toString + "," + line._4.map(elem => original_column(elem._2)).mkString(" "))



val t = rd_ID_features_values.map(line => castArrayStringToInt(line.slice(24, 50)))
val t2 = t.reduce((a1, a2) => addTwoArray(a1, a2))
val t3 = t2.zipWithIndex.sortBy(t => t._1)
val t4 = giveAValueToPosition(t3)


val rdinitiales = sc.textFile("/train_ver2.csv", 48)
val rd2 = RDDinitiales.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
val rd3 = RDD2.map(w => w.replaceAll(" ", "")).map(line => line.split(","))
val rdRDD_lineTooLongRepared = RDD3.filter(line => line.length == 49).map(line => concatenateTwoElementsInArray(line, 20, 21))
val rd_normal_size = RDD3.filter(line => line.length == 48)
val rd4 = RDD_normal_size.union(RDD_lineTooLongRepared)
val rd_ID_features_values = RDD4.map(addMissingValue)
val rd5 = rd_ID_features_values.filter(line => line(0) == "2016-05-28").map(line => (line(1).toInt, castArrayStringToInt(line.slice(24, 50))))

val rd6 = RDD1.map(line => (line._1, (line._2, line._3, line._4))).join(rd5)

val rd7 = rd6.mapValues(line => putToNegativeIfPresent(line._1._3, line._2))

val rd8 = rd7.mapValues(line => replace0WithPopular(line, t4))

val rd9 = rd8.mapValues(a => a.zipWithIndex.sortBy(t => -t._1).slice(0, 7))

val rd10 = rd9.map(line => line._1.toString + "," + line._2.map(elem => original_column(elem._2)).mkString(" "))










// tableau (ville => moyenne)

def checkIfDifferents(a1: )
//Séparer les données en pas mai 2016 / mai 2016

val RDD_ID_features_values = RDD4.map(addMissingValue)


rd10.saveAsTextFile("/submittedValues_2")

// cat part-00000 part-00001 part-00002 part-00003 part-00004 part-00005 part-00006 part-00007 part-00008 part-00009 part-00010 part-00011 part-00012 part-00013 part-00014 part-00015 part-00016 part-00017 part-00018 part-00019 part-00020 part-00021 part-00022 > submission1.txt
// (echo "ncodpers,added_products," ; cat submission1.txt) | sed 's/,/\t/g' > submittedValues_1.csv


val U = RDD_chosenElements.count()

val score = RDD_chosenElements.reduce()


List("banana", "pear", "apple").sortWith(sortByFirstElement)


cat part-00000  part-00006  part-00012  part-00018  part-00024  part-00030  part-00036  part-00042  part-00048  part-00054  part-00060  part-00066  part-00072  part-00078  part-00084  part-00090   part-00001  part-00007  part-00013  part-00019  part-00025  part-00031  part-00037  part-00043  part-00049  part-00055  part-00061  part-00067  part-00073  part-00079  part-00085  part-00091 part-00002  part-00008  part-00014  part-00020  part-00026  part-00032  part-00038  part-00044  part-00050  part-00056  part-00062  part-00068  part-00074  part-00080  part-00086  part-00092 part-00003  part-00009  part-00015  part-00021  part-00027  part-00033  part-00039  part-00045  part-00051  part-00057  part-00063  part-00069  part-00075  part-00081  part-00087  part-00093 part-00004  part-00010  part-00016  part-00022  part-00028  part-00034  part-00040  part-00046  part-00052  part-00058  part-00064  part-00070  part-00076  part-00082  part-00088  part-00094 part-00005  part-00011  part-00017  part-00023  part-00029  part-00035  part-00041  part-00047  part-00053  part-00059  part-00065  part-00071  part-00077  part-00083  part-00089  part-00095 > submission2.txt