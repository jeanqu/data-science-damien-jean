
var new_df = df_categorical

for (i <- 1 to featuresNames_IdAndCategorical.length-1)
{      
	println(i)
	val name_column = featuresNames_IdAndCategorical(i).replaceAll("\"", "")
	val name_column_indexed = featuresNames_IdAndCategorical(i).replaceAll("\"", "") + "_indexed"
	val indexer = new StringIndexer().setInputCol(name_column).setOutputCol(name_column_indexed).fit(new_df)
	new_df = indexer.transform(new_df)

	//val id_newFeat = indexed.select("ncodpers", name_column_indexed)

	//new_df = new_df.join(id_newFeat, Seq("ncodpers"), "inner")
}

// Saved into "/categorical_features_df.parquet")

//new_df.write.parquet("/categorical_features_df_test.parquet")
