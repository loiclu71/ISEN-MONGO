import re
from datetime import datetime
from pprint import pprint

from pymongo import MongoClient, TEXT

atlas = MongoClient('mongodb+srv://loic:20140304remind@cluster0.rifny.gcp.mongodb.net/test?retryWrites=true&w=majority')
db = atlas.test
collection = db.stations
vLille = db.datas

# find station with name
def find_station(name):
    find = re.compile(name)
    name_find = {"name":find}
    for station in collection.find(name_find):
        pprint(station)

# We can find a station named TANNEURS but it doesnt exist
#find_station("TANNEURS")

# update a station
def update_station(id,etat_service):
    collection.update(
        {"_id": id},
        {"$set": {'etat',etat_service}}
    )
# Update a station by service status

# delete a station
def delete_station(id):
    collection.delete({"_id": id})
# directly delete a station by id

#deactive stations in an area
def deactive_stations(x0,x1,x2,x3,status):
    list_deactive_stations = []

    station_find = {"geometry": {
            "$geoWithin":{
                "type": "Point",
                "coordinates": [
                    [x0[0],x0[1]],
                    [x1[0],x1[1]],
                    [x2[0],x2[1]],
                    [x3[0],x3[1]]
                ]
            }
        }
    }
    deactive_stations = collection.find(station_find)
    collection.update_many(station_find, {"$set": {"available": status}})
    for station in deactive_stations:
        list_deactive_stations.append(station['name'])
    pprint(list_deactive_stations)

# We can choose four points which form an area then update its status
# and print the station's name
# x0 = [3.110669, 50.632393]
# x1 = [3.115101, 50.626846]
# x2 = [3.116677, 50.62486]
# x3 = [3.118579, 50.643883]
# status = "En service"

def ratio_stations():
    list_ratio_stations = vLille.aggregate([
        {
            "$group": {
                "_id": "$_id",
                "total_bike": {"$sum": "$bike_availbale"},
                "total_stand": {"$sum": "$stand_availbale"},
            }
        },
        {
            "$addFields": {
                "time": {"$toDate": "$date"}
            }
        },
        {
            "addFields": {
                "day": {"$dayOfWeek": "$time"}
            }
        },
        {
            "$addFields": {
                "hour": {"$hour": "$time"}
            }
        },
        {
            "$match": {
                "$hour": {
                    "$gte": datetime.strptime("18:00:00"),
                    "$lt": datetime.strptime("19:00:00")
                }
            }
        },
        {
            "$match": {
                "$day": {"$in": [1,2,3,4,5]}
            }
        },
        {
            "$addFields": {
                "total": {"$sum": ["$total_bike","$total_stand"]}
            }
        },
        {
            "$addFields": {
                "ratio": {"$divide": ["$total_bike","$total"]}
            }
        }
    ])

    for elem in list_ratio_stations:
        if(elem['ratio'] <= 0.2 ):
            find_station2 = {"name": elem['name']}
            for station in collection.find(find_station2):
                pprint(station)