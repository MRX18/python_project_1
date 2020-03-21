import rasterio
import os
import numpy as np


def listMatrixImage(path):
    images = []
    files = os.listdir(path)
    for file in files:
        with rasterio.open(path + file, 'r') as ds:
            images.append(ds.read()[0])
    return images


def outputMatrix(array):
    for i in range(len(array)):
        for j in range(len(array[i])):
            print(array[i][j], end=' ')
        print()


def cloudiness(array):
    count = 0
    for i in range(len(array)):
        for j in range(len(array[i])):
            if array[i][j] == 254 or array[i][j] == 255:
                count += 1
    return count/np.size(array)


arr = listMatrixImage("D:/python/test/")
#print(np.average(arr[1])) #середнє зважене
#print(np.std(arr[1])) # середнє квадратичне відхилення