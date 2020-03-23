import ast
import json
from datetime import datetime
from threading import Thread
import rasterio
import os
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
import DB

globalCounter = 0


def listMatrixImage(path="D:/python/test/"):
    count = 0;
    files = os.listdir(path)
    for file in files:
        element = file.split("_")
        with rasterio.open(path + file, 'r') as ds:
            arr = ds.read()[0]
            DB.insert(element[1], datetime.strptime(element[0], '%d%m%Y').date(), element[3], element[4], path + file,
                      cloudiness(arr), np.average(arr), np.std(arr), dict(Counter(arr.ravel())))
            count += 1;
            print(count)


def taskListMatrixImage(path, taskId, begin, end):
    global globalCounter
    files = os.listdir(path)
    for i in range(int(begin), int(end)):
        if i != len(files):
            element = files[i].split("_")
            with rasterio.open(path + files[i], 'r') as ds:
                arr = ds.read()[0]
                DB.insert(element[1], datetime.strptime(element[0], '%d%m%Y').date(), element[3], element[4],
                          path + files[i], cloudiness(arr), np.average(arr), np.std(arr), dict(Counter(arr.ravel())))
                globalCounter += 1
                print("ID: %s\t Count: %d" % (taskId, globalCounter))


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
    return count / (np.size(array) - dict(Counter(array.ravel()))[0])


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
        print("%5d\t|\t%10s\t|\t%10s\t|\t%5s\t|\t%5s\t|\t%5s" % (
            item["id"], item["date"], item["field"], round(item["cloudiness"], 4), round(item["average"], 2),
            round(item["std"], 2)))


def jsonEncoder(data):
    j = []
    for item in data:
        array = []
        array.append(list(ast.literal_eval(json.loads(item)).keys()))
        array.append(list(ast.literal_eval(json.loads(item)).values()))
        j.append(array)
    return j


def histogram():
    field = input("Введіть назву поля: ")
    date = input("Введіть початкову дату: ")
    data = []
    result = DB.selectForHistogram(field, datetime.strptime(date, '%d.%m.%Y').date())
    for item in result:
        data.append(item["json"])
    data = jsonEncoder(data)
    print(data)
    ax = plt.axes()
    ax.yaxis.grid(True, zorder=1)
    for item in data:
        xs = range(len(item[0]))
        plt.bar([x for x in xs], item[1], width=0.2, color='red', alpha=0.7, zorder=2)
        plt.xticks(xs, item[0])
    plt.show()


def visualizationStatisticalIndicators():  # Візуалізація статистичних показників
    countField = input("Введіть кількість поля: ")
    field = []
    for i in range(int(countField)):
        field.append(input("Введіть назву поля: "))
    dateBegin = input("Введіть початкову дату: ")
    dateEnd = input("Введіть кінцеву дату: ")
    for i in range(int(countField)):
        result = DB.selectForGrowingSeason(field[i], datetime.strptime(dateBegin, '%d.%m.%Y').date(),
                                           datetime.strptime(dateEnd, '%d.%m.%Y').date())
        localData = []
        localDate = []
        for item in result:
            localDate.append(item["date"])
            localData.append(item["average"])
        plt.plot(localDate, localData)
    plt.show()


# createTask(3, 10) # test
#createTask(20, 4546)
histogram()