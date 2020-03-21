from datetime import datetime

import pymysql
from pymysql.cursors import DictCursor
import rasterio
import os
import numpy as np
from collections import Counter
import DB


# connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4', cursorclass=DictCursor)


def listMatrixImage(path):
    count = 0;
    files = os.listdir(path)
    for file in files:
        element = file.split("_")
        with rasterio.open(path + file, 'r') as ds:
            arr = ds.read()[0]
            DB.insert(element[1], datetime.strptime(element[0], '%d%m%Y').date(), element[3], element[4], path+file, cloudiness(arr), np.average(arr), np.std(arr), dict(Counter(arr[1].ravel())))
            count += 1;
            print(count)


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
    return count / np.size(array)


listMatrixImage("D:/python/test/")
# print(np.average(arr[1])) # середнє зважене
# print(np.std(arr[1])) # середнє квадратичне відхилення
# print(dict(Counter(arr[1].ravel())))  # частот входження унікальних значень у вибірку