
import org.apache.spark.ml.linalg.{DenseVector => newDenseVector}

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
def putToFormat_ID_features_valuesNotJanuary_valuesJanuary(ID_feat_val: (Int, (Array[String], Array[Int]))) = {
        if (ID_feat_val._2._1(0) == "2015-01-28")
        {
            (ID_feat_val._1, (ID_feat_val._2._1, Array.fill[Int](ID_feat_val._2._2.length)(0), ID_feat_val._2._2))
        }
        else
        {
            (ID_feat_val._1, (ID_feat_val._2._1, ID_feat_val._2._2, Array.fill[Int](ID_feat_val._2._2.length)(0)))
        }
}


def castArrayStringToInt(a: Array[String]) = {
        var lineReturn = new Array[Int] (a.length)
        for (i <- 0 to a.length-1)
        {
                lineReturn(i) = a(i).toInt
        }
        lineReturn
}

def castArrayIntToDouble(a: Array[Int]) = {
        var lineReturn = new Array[Double] (a.length)
        for (i <- 0 to a.length-1) 
        {
                lineReturn(i) = a(i).toDouble
        }
        lineReturn
}


def addMissingValue(str: Array[String]) = {

        //Valeurs non changés: fecha_alta, ind_actividad_cliente, nomprov
        if (str(6) == "" || str(6) == "NA")
        {
                str(6) = "1"
        }
        if (str(7) == "" || str(7) == "NA")
        {
                str(7) = "1"
        }
         if (str(8) == "" || str(8) == "NA")
        {
                str(8) = "0"
        }
        for (i <- 24 to str.length-1)
        {
            if (str(i) == "" || str(i) == "NA")
            {
                str(i) = "0"
            }
        }        
        str
}

def addRevenuMissig(line : (Array[String], Array[Int], Array[Int]), a_city: Map[String,Double]) = {

        if (line._1(21) == "")
        {
               line._1(21) =  a_city(line._1(19).replaceAll("\"", "")).toString
        }
        line
}

def addAgeMissing(line : (Array[String], Array[Int], Array[Int]), meanAge: Double) = {
    if (line._1(4) == "NA" || line._1(4) == "")
        {
               line._1(4) =  meanAge.toString
        }
        line
}


def takeOnlyGoodValues(str: Array[String]): Array[Int] = {
          castArrayStringToInt(str.slice(24, str.length + 1))
}

def takeOnlyFeaturesClient(str: Array[String]): Array[String] = {
        Array(str(0)) ++ str.slice(2, 24)
}

def addTwoArray(a1: Array[Int], a2: Array[Int]) = {
        var lineReturn = new Array[Int] (a1.length)
        for (i <- 0 to a1.length-1)
        {
                lineReturn(i) = a1(i) + a2(i)
        }
        lineReturn
}

//Ne prends pas en compte les jours; que les années et les mois!
def trueIfDate1LatterThanDate2(d1: String, d2: String) = {
        val d1_split = d1.split("-")
        val d2_split = d2.split("-")
        if (d1_split.length != 3 || d2_split.length != 3)
        {
                false
        }
        else if (d1_split(0).toInt > d2_split(0).toInt)
        {
                true
        }
        else if (d1_split(0).toInt < d2_split(0).toInt)
        {
                false
        }
        else
        {
                if (d1_split(1).toInt >= d2_split(1).toInt)
                {
                        true
                }
                else
                {
                        false
                }
        }
        
}

def chooseLastFeatures(feature1: Array[String], feature2: Array[String]) = {
//On vérifie si la rente est présente. Si oui, on la retourne. sinon, on choisie la date la plus proche.
        var feat1 = feature1
        var feat2 = feature2
        if (feat1(21) == "" && feat2(21) != "")
        {
                feat1(21) = feat2(21)
        }
        else if (feat1(21) != "" && feat2(21) == "")
        {
                feat2(21) = feat1(21)
        }

        if (feat1(4) == "NA" && feat2(4) != "NA")
        {
                feat1(4) = feat2(4)
        }
        else if (feat1(4) != "NA" && feat2(4) == "NA")
        {
                feat2(4) = feat1(4)
        }

        if (trueIfDate1LatterThanDate2(feat1(21), feat2(0)))
        {
                feat1onlyInterestingColumns.rdd
        }
        else 
        {
                feat2
        }
}

def fromFeaturesToNumeriques(a: Array[String]) = {
        var lineReturn = new Array[Double] (a.length)
        //On peut tester de garder al dernière date d'achat comme feature. On la modifie en numérique
        val date_last_buying = (a(0).split("-")(0).toDouble - 2014) * 12 + a(0).split("-")(1).toDouble

}

def fromRowToArrayOfDouble(r: Row, nbElements: Int) = {
    val doubleArray = new Array[Double] (nbElements)
    doubleArray(0) = r.getString(0).toDouble
    for (i <- 1 to nbElements-1)
    {
        doubleArray(i) = r.getDouble(i)
    }
    doubleArray
}

def castRowOneValueToDouble(r: Row, colName: String) = {
    r.getAs[Double](colName)
}

def convertRowPredictionsToRDD(row: Row, Feature_name_columne: String, labels_name_column: Array[String], probability_name_column: Array[String], labels_predicted_name_column: Array[String], predicted_name_column: Array[String]) = {
    // If predictedlabel == predi.toInt, on prend la deuxième valeur "proba". Sinon la première.
    
    
    val id = row.getInt(0)
    val features = row.getAs[newDenseVector](Feature_name_columne)
    val a_labels = new Array[Int] (labels_name_column.length)
    val a_proba = new Array[Double] (probability_name_column.length)

    var whichProba = 0
    var temp = new Array[Double](2)

    for (i <- 0 to labels_name_column.length-1)
    {
        a_labels(i) = row.getAs[Int](labels_name_column(i))
    }
    for (i <- 0 to probability_name_column.length-1)
    {
       if (row.getAs[String](labels_predicted_name_column(i)).toInt == row.getAs[Double](predicted_name_column(i)).toInt)
       {
            whichProba = 1
       }
       else
       {
            whichProba = 0
       }
       temp = row.getAs[newDenseVector](probability_name_column(i)).toArray
       a_proba(i) = temp(whichProba)
    }
    (id, features, a_labels, a_proba)
}


//Transforme [prob0, prob1, prob2...] en prob1*1 + prob2*2...
def toSumProba(a: Array[Double]) = {
    var s = 0.toDouble
    for (i <- 1 to a.length-1)
    {
        s = a(i) * i
    }
    s
}

def MAP7(line: (Int, org.apache.spark.ml.linalg.DenseVector, Array[Int], Array[(Double, Int)])) = {
    val m = line._3.sum
    if (m == 0)
    {
        0
    } 
    else
    {
        var nbTrouves = 0
        for (i <- 0 to line._4.length-1)
        {
            if (line._3(line._4(i)._2) == 1)
            {
                nbTrouves = nbTrouves + 1
            }
        }
        m - nbTrouves
    }
    
}

def putToNegativeIfPresent(a1: Array[Double], a2: Array[Int]) = {
    var rep = a1
    for (i <- 0 to a1.length-1)
    {
        if(a2(i) > 0)
        {
            rep(i) = -10
        }

    }
    rep
}

def giveAValueToPosition(a: Array[Tuple2[Int, Int]]) = {
    var rep = new Array[Double] (a.length)
    for (i <- 0 to a.length-1)
    {
        rep(a(i)._2) = -i * 0.2 
    }
    rep
}

def replace0WithPopular(a: Array[Double], pop: Array[Double]) = {
    var rep = new Array[Double] (a.length)
    for (i <- 0 to a.length-1)
    {
        if (a(i) == 0.toDouble)
        {
            rep(i) = pop(i)
        }
        else
        {
           rep(i) = a(i)
        }
    }
    rep
}
