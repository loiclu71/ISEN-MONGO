import json
from pprint import pprint

import requests
from pymongo import MongoClient


def get_vlille():
    url="https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&face=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET",url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records",[])

def get_vrennes():
    url="https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=stations-reparation-velo&q=&rows=30&sort=id&facet=etat&facet=gonflage&facet=reparation"
    response = requests.request("GET",url)
    request_json = json.loads(response.text.encode('utf8'))
    return request_json.get("records",[])

def get_vlyon():
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&facet=name&facet=is_renting&rows=300"
    response = requests.request("GET", url,headers={},data={})
    request_json = json.loads(response.text.encode('utf8'))
    return request_json.get("records",[])

def get_vparis():
    url="https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=1500"
    response = requests.request("GET",url)
    request_json = json.loads(response.text.encode('utf8'))
    return request_json.get("records",[])

vlilles = get_vlille()
vrennes = get_vrennes()
vlyons = get_vlyon()
vparis = get_vparis()

vlilles_to_insert = [
    {
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': 'Lille',
            'id_ext': elem.get('fields', {}).get('libelle')
       },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlilles
]

vrennes_to_insert = [
    {
        'name': elem.get('recordid'),
        'geometry': elem.get('geometry'),
        'source': {
            'dataset': 'Rennes',
       }
    }
    for elem in vrennes
]

vlyons_to_insert = [
    {
        'name': elem.get('fields', {}).get('name', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('available'),
        'source': {
            'dataset': 'Lyon',
            'id_ext': elem.get('fields', {}).get('gid')
       },
        'tpe': elem.get('fields',{}).get('banking','')  == 't'
    }
    for elem in vlyons
]

vparis_to_insert = [
    {
        'name': elem.get('fields', {}).get('name', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('capacity'),
        'source': {
            'dataset': 'Paris',
            'id_ext': elem.get('fields', {}).get('stationcode')
        },
        'tpe': True if elem.get('fields', {}).get('is_renting') == 'OUI' else False,
    }
    for elem in vparis
]

pprint(vlilles_to_insert)
pprint(vrennes_to_insert)
pprint(vlyons_to_insert)
pprint(vparis_to_insert)

atlas = MongoClient('mongodb+srv://loic:20140304remind@cluster0.rifny.gcp.mongodb.net/test?retryWrites=true&w=majority')
db = atlas.test

#for vlille in vlilles_to_insert:
#    db.stations.insert_one(vlille)

db.stations.insert_many(vlilles_to_insert)
db.stations.insert_many(vrennes_to_insert)
db.stations.insert_many(vlyons_to_insert)
db.stations.insert_many(vparis_to_insert)
