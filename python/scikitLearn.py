from sklearn import decomposition
from sklearn import datasets

import csv as csv
import numpy as np

file = np.genfromtxt("train_numeric50.csv", delimiter=',', dtype=None)

data = file[1:,]

def toFloat(a,b):
    if (a>b): return a
    else: return b

data2 = np.empty_like(data)
for i in range(len(data)):
	for j in range(len(data[1,])):
		if data[i,j] == '':
			data2[i,j] = 0
		else:
			data2[i,j] = float(data[i,j])

data3 = data2.astype(np.float)

pca = decomposition.PCA(n_components=100)
pca.fit(data3)
data4 = pca.transform(data3)