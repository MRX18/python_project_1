import json
import pymysql
from pymysql.cursors import DictCursor

def insert(field, date, ndvi, other_data, path, cloudiness, avg, std, sampling_rate):
    connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4', cursorclass=DictCursor)
    with connection.cursor() as cursor:
        query = "INSERT INTO `data_image` (`field`, `date`, `ndvi`, `other_data`, `path`, `cloudiness`, `average`, `std`, `json`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.execute(query, (field, date, ndvi, other_data, str(path), float(cloudiness), float(avg), float(std), json.dumps(str(sampling_rate))))
        connection.commit()


def select(field):
    connection = pymysql.connect(host='127.0.0.1', user='root', password='12345', db='python', charset='utf8mb4', cursorclass=DictCursor)
    with connection.cursor() as cursor:
        query = "SELECT * FROM python.data_image where `field` = %s Order By `date`"
        cursor.execute(query, field)
        return cursor.fetchall()