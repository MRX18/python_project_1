from datetime import datetime
from threading import Thread
import rasterio
import os
import numpy as np
from collections import Counter
import DB

globalCounter = 0


def listMatrixImage(path):
    count = 0;
    files = os.listdir(path)
    for file in files:
        element = file.split("_")
        with rasterio.open(path + file, 'r') as ds:
            arr = ds.read()[0]
            DB.insert(element[1], datetime.strptime(element[0], '%d%m%Y').date(), element[3], element[4], path + file,
                      cloudiness(arr), np.average(arr), np.std(arr), dict(Counter(arr[1].ravel())))
            count += 1;
            print(count)


def taskListMatrixImage(path, taskId, begin, end):
    files = os.listdir(path)
    for i in range(int(begin), int(end)):
        element = files[i].split("_")
        with rasterio.open(path + files[i], 'r') as ds:
            arr = ds.read()[0]
            DB.insert(element[1], datetime.strptime(element[0], '%d%m%Y').date(), element[3], element[4],
                      path + files[i], cloudiness(arr), np.average(arr), np.std(arr), dict(Counter(arr[1].ravel())))
            print(taskId)


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


def createTask(threadsCount, records):
    begin = 0
    end = begin + records
    for i in range(threadsCount):
        thread = Thread(target=taskListMatrixImage, args=("D:/python/254_NDVI/", str(i), str(begin), str(end)))
        thread.start()
        print(begin)
        begin = end;
        end = begin + records


def outputFieldData():
    field = input("Введіть назву поля: ")
    result = DB.select(field)
    print("%5s\t|\t%10s\t|\t%10s\t|\t%5s\t|\t%5s\t|\t%5s" % ("ID", "Date", "Field", "Cloudiness", "Average", "Srd"))
    for item in result:
        print("%5d\t|\t%10s\t|\t%10s\t|\t%5s\t|\t%5s\t|\t%5s" % (item["id"], item["date"], item["field"], round(item["cloudiness"], 4), round(item["average"], 2), round(item["std"], 2)))


# createTask(3, 10) # test
#createTask(5, 18181)

outputFieldData()