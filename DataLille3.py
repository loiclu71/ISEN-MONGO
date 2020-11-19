import json
from pprint import pprint

import requests
from bson import SON
from pymongo import MongoClient

atlas = MongoClient('mongodb+srv://loic:20140304remind@cluster0.rifny.gcp.mongodb.net/test?retryWrites=true&w=majority')
db = atlas.test

collection = db.stations
vLille = db.datas

def get_avaiable_stations(lon,lat,max_distance):
    collection.create_index([("geometry", "2dsphere")])
    stations = collection.find({
        'geometry':{
            '$near':SON([(
                '$geometry',SON([
                    ('type','Point'),
                    ('coordinates',[lon,lat])
                ]
                )),
                ('$maxDistance',max_distance)
            ]
            )
         }
    }
    )

    avaiable_stations = []
    for elem in stations:
        data = [
            {
                "name": elem.get('name'),
                "coordinates": elem.get('geometry', {}).get('coordinates'),
                "bike": get_bike(elem.get('_id')),
                "stand": get_stand(elem.get('_id'))
            }
        ]
        avaiable_stations.append(data)
    pprint(avaiable_stations)

def get_bike(id):
    tps = db.stations2.find_one({'station_id': id },{'bike_availbale': 1})
    return tps['bike_availbale']

def get_stand(id):
    tps = db.stations2.find_one({'station_id': id}, {'stand_availbale': 1})
    return tps['stand_availbale']

get_avaiable_stations(3.160344,50.72839,1000)