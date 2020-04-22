import csv
import re
from pymongo import MongoClient
import datetime as dt
from pprint import pprint



def read_data(csv_file,db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)

        uniq_id = 0
        list_for_db = []
        for artist in reader:
            uniq_id += 1
            artist_info = {'_id': uniq_id,
                           'Исполнитель': artist['Исполнитель'],
                           'Цена': artist['Цена'],
                           'Место': artist['Место'],
                           'Дата': dt.datetime.strptime('2020 ' + artist['Дата'], '%Y %d.%m')}

            list_for_db.append(artist_info)

        db.insert_many(list_for_db)

        



def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    tikets = list(db.find().sort('Цена', 1))
    for ticket in tikets:
        print(ticket)


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    regex = re.compile(name, re.IGNORECASE)
    result = list(db.find({'Исполнитель': regex}).sort('Цена', 1))
    for line in result:
        print(line)

def find_by_date(db):
    """
    Реализовать сортировку по дате мероприятия. 
    Найти билеты по дате (мероприятия с 1 по 30 июля.  $gte >=, $lte <=). 
    Также отсортировал по дате
    """
    day_start = dt.datetime(2020, 7, 1)
    day_end = dt.datetime(2020, 7, 30)
    result = list(db.find({'Дата': {'$gte': day_start, '$lte': day_end}}).sort('Дата', 1))
    for line in result:
        print(line)


if __name__ == '__main__':
    client = MongoClient()
    netology_db = client['netology']
    artists_collection = netology_db['artists']
    read_data('artists.csv', artists_collection)
    find_cheapest(artists_collection)
    find_by_name('Вася', artists_collection)
    find_by_date(artists_collection)
