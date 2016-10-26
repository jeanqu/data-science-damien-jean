

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