import requests, time
from requests.structures import CaseInsensitiveDict
import json
from json import JSONEncoder
import datetime
from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200/")
index = "ibb-hal"
payload ={
    "item":{
        "Day": datetime.now().strftime('%Y-%m-%d')
        }
    }

def search(es_object, index, search):
    res = es_object.search(index=index, body=search)
    print(res)
    

def create_index(index):
    es.indices.create(index=index, ignore=400)
    print("Created index")

def insert_one_data(data):
    res = es.index(index=index, body=data)
    print(res)




create_index(index)




while True:
    response = requests.post('https://halfiyatlaripublicdata.ibb.gov.tr/api/HalManager/getProductPricebyDay', json=payload)
    dict_json = json.loads(response.text)
    print(dict_json)

    menu = dict_json['Results']

    res = [{'UrunAd': dct['UrunAd'], 'EnDusukFiyat': dct['EnDusukFiyat'], 'EnYuksekFiyat':dct['EnYuksekFiyat'], 'GuneAit': dct['GuneAit']} 
        for dct in menu if dct['UrunAd']]

    for dct in menu:
        data = {
        "UrunAd": dct['UrunAd'],
        "EnDusukFiyat": dct['EnDusukFiyat'],
        "EnYuksekFiyat": dct['EnYuksekFiyat'],
        "GuneAit": dct['GuneAit']
        }
        insert_one_data(data)

    time.sleep(43200)


