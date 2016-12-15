
#Algo3: On veut convertir notre tableau de données sous forme de dataFrame. On pourra ainsi manipuler les colonnes en plus des lignes
#Pour cela on doit convertir les lignes sous forme de "Row" (package SQL), puis on les convertit sous forme de dataFrame

#Problème: la fonction "Row" a une limite de 255 argument, on va donc créer plusieurs dataFrame puis on va les joindre 
i_feature = 1 
numero_dataframe = 0
arrayRowNamed = [None] * 11
#Séparation des données en 11 RDD organisées par Row SQL
while i_feature < nbFeatures_broadcast.value:
	j_feature = 0
	codeToCreateRowNamed = 'lambda line: Row(Id=line[0], '
	while (j_feature < 200) and (i_feature < nbFeatures_broadcast.value):
		codeToCreateRowNamed = codeToCreateRowNamed + str(list_features_broadcast.value[i_feature]) + '=line[' + str(i_feature) + '], '
		i_feature = i_feature + 1 
		j_feature = j_feature + 1
	codeToCreateRowNamed = codeToCreateRowNamed[:-2] + ')'
	arrayRowNamed[numero_dataframe] = RDD1.map(eval(codeToCreateRowNamed))
	numero_dataframe = numero_dataframe + 1

#On crée maintenant les dataFrames
array_df = [None] * 11
for i in range(0,len(arrayRowNamed)):
	array_df[i] = spark.createDataFrame(arrayRowNamed[i])

#On les concatène

ouaoua = array_df[0].join(array_df[1], "Id")

DF_datas = array_df[0]
for i in range(1,len(array_df)):
	DF_datas = DF_datas.join(array_df[i], "Id")
