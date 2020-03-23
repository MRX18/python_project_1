import json
import threading
import pymysql
from pymysql.cursors import DictCursor

mutex = threading.Lock()


def insert(field, date, ndvi, other_data, path, cloudiness, avg, std, sampling_rate):
    mutex.acquire()
    connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4',
                                 cursorclass=DictCursor)
    with connection.cursor() as cursor:
        query = "INSERT INTO `data_image` (`field`, `date`, `ndvi`, `other_data`, `path`, `cloudiness`, `average`, `std`, `json`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.execute(query, (field, date, ndvi, other_data, str(path), float(cloudiness), float(avg), float(std),
                               json.dumps(str(sampling_rate))))
        connection.commit()
    mutex.release()


def select(field):
    connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4',
                                 cursorclass=DictCursor)
    with connection.cursor() as cursor:
        query = "SELECT * FROM python.data_image where `field` = %s Order By `date`"
        cursor.execute(query, field)
        return cursor.fetchall()


def selectForGrowingSeason(field, dateBegin, dateEnd):
    connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4',
                                 cursorclass=DictCursor)
    with connection.cursor() as cursor:
        query = "SELECT * FROM python.data_image where (`field` = %s) and (`date` between %s and %s) and `cloudiness` < '0.5' Order By `date`"
        cursor.execute(query, (field, dateBegin, dateEnd))
        connection.close()
        return cursor.fetchall()


def selectInYear(field, year):
    connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4',
                                 cursorclass=DictCursor)
    with connection.cursor() as cursor:
        query = 'SELECT * FROM python.data_image where (`field` = %s) and (DATE_FORMAT(`date`, "%%Y") = %s) Order By `date`'
        cursor.execute(query, (field, year))
        connection.close()
        return cursor.fetchall()


def selectForHistogram(field, dateBegin):
    connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4',
                                 cursorclass=DictCursor)
    with connection.cursor() as cursor:
        query = "SELECT * FROM python.data_image where (`field` = %s) and (`date` = %s) Order By `date`"
        cursor.execute(query, (field, dateBegin))
        connection.close()
        return cursor.fetchall()
