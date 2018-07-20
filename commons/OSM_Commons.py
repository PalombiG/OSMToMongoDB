import multiprocessing
import threading
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from commons import CommonFunctions


def saveOSMToMongo(rowslist, logger, db_server, db_port, db_database, db_collection, firstInsert=False):
    client = MongoClient(db_server, int(db_port))
    db = client[db_database]
    collection = db[db_collection]

    CommonFunctions.addLogMessage('Save ' + str(len(rowslist)) + ' elements in mongo database', logger, CommonFunctions.INFO_LOG)
    bulkop = collection.initialize_unordered_bulk_op()
    for row in rowslist:
        if "___remove___" in row:
            bulkop.find({'id': row["id"], 'osm_type': row["osm_type"]}).remove()
        else:
            bulkop.find({'id': row["id"], 'osm_type': row["osm_type"]}).upsert().update({'$set': row})
        if firstInsert:
            if (row["osm_type"]=='node'): bulkop.find({'nd.ref': row["id"], 'id': {'$exists': True}}).update({'$unset': {'geometry':""}})
            bulkop.find({'member.ref': row["id"], 'member.type': row["osm_type"], 'id': {'$exists': True}}).update({'$unset': {'geometry':""}})
    try:
        retval = bulkop.execute()
    except BulkWriteError as bwe:
        werrors = bwe.details['writeErrors']
        pass
    CommonFunctions.addLogMessage(str(len(rowslist)) + ' elements saved in mongo database', logger, CommonFunctions.INFO_LOG)
    del rowslist
    client.close()

def updateWays(db_server, db_port, db_database, db_collection, logger, element_at_time, checkarray = [], number_of_core=1):
    save_packages = []
    number_of_processors = multiprocessing.cpu_count()
    CommonFunctions.addLogMessage("Update geometries", logger, CommonFunctions.INFO_LOG)
    client = MongoClient(db_server, int(db_port))
    db = client[db_database]
    collection = db[db_collection]

    rowslist = []
    CommonFunctions.addLogMessage("Calculate ways", logger, CommonFunctions.INFO_LOG, len(checkarray) > 0)
    aggrfilter = [
        {'$match': {'osm_type': 'way', 'geometry': {'$exists': False}}},
        {'$graphLookup': {'from': db_collection, 'startWith': "$nd.ref", 'connectFromField': "nd.ref",
                          'connectToField': "id", 'as': "childs_geometries",
                          'restrictSearchWithMatch': {"osm_type": "node"}}}
    ]
    mongocursor = collection.aggregate(aggrfilter)
    mongocursor.batch_size(int(element_at_time/10))
    for document in mongocursor:
        singlerow = {'id': document['id'], 'osm_type':'way'}
        geom = {}
        coords = []
        for nds in document['nd']:
            for child in document['childs_geometries']:
                if child['id']==nds['ref']:
                    if 'geometry' in child:
                        if 'coordinates' in child['geometry']:
                            coords.append(child['geometry']['coordinates'])
                    break
        if document['nd'][0] != document['nd'][len(document['nd']) - 1] or \
                ("barrier" in document and ("area" not in document or str(document["area"]).lower()=='no')) or \
                ("highway" in document and ("area" not in document or str(document["area"]).lower()=='no')):
            geom['type'] = 'LineString'
            if len(coords) > 0:
                geom['coordinates'] = list(coords)
                del coords[:]
        else:
            geom['type'] = 'Polygon'
            coords = [coords]
            if len(coords) > 0:
                geom['coordinates'] = list(coords)
                del coords[:]

        if geom != {} :
            singlerow['geometry'] = geom
            rowslist.append(singlerow)

        if len(rowslist) == int(element_at_time/10):
            save_packages.append(list(rowslist))
            CommonFunctions.addLogMessage("Package " + str(len(save_packages)) + " of " + str(number_of_processors) + " prepared", logger, CommonFunctions.INFO_LOG)
            if len(save_packages)==number_of_processors:
                processes = []
                for save_package in save_packages:
                    p = threading.Thread(target=saveOSMToMongo, args=(list(save_package), logger, db_server, db_port, db_database, db_collection))
                    p.start()
                    processes.append(p)
                    del save_package
                for process in processes:
                    process.join()
                del processes
                del save_packages[:]
            del rowslist[:]
            del geom

    if len(rowslist) > 0:
        save_packages.append(list(rowslist))
        processes = []
        for save_package in save_packages:
            p = threading.Thread(target=saveOSMToMongo, args=(list(save_package), logger, db_server, db_port, db_database, db_collection))
            p.start()
            processes.append(p)
            del save_package
        for process in processes:
            process.join()
        del processes
        del save_packages[:]

        del rowslist[:]

    del collection
    del db
    client.close()
    del client

def updateRelations(db_server, db_port, db_database, db_collection, logger, element_at_time):
    unknown_types = []
    save_packages = []
    number_of_processors = multiprocessing.cpu_count()
    CommonFunctions.addLogMessage("Calculate relations", logger, CommonFunctions.INFO_LOG)
    client = MongoClient(db_server, int(db_port))
    db = client[db_database]
    collection = db[db_collection]

    rowslist = []
    '''mongocursor = collection.aggregate([
        {'$match': {'osm_type': 'relation', 'geometry': {'$exists': False}, 'type': {'$in': ['multipolygon', 'boundary', 'multilinestring', 'osm']}}},
        {'$graphLookup':{'from': db_collection,'startWith': "$member.ref", 'connectFromField': "member.ref", 'connectToField': "id",'as': "childs_geometries",
                         'restrictSearchWithMatch': {'geometry': {'$exists': True}}}}
    ])'''
    #TODO Riprendere da A_Buiding svuotando tutte le tabelle successive e controllando se va meglio con i barch number sotto
    mongocursor = collection.find({'osm_type': 'relation', 'geometry': {'$exists': False}}, no_cursor_timeout=True)
    mongocursor.batch_size(int(element_at_time/10))
    for document in mongocursor:
        outer_coords = []
        outer_polygons = []
        inner_coords = []
        inner_polygons = []
        singlerow = {'id': document['id'], 'osm_type':'relation'}
        geom = {}
        for member in document['member']:
            mongocursor_child = collection.find({'osm_type': member['type'], 'id': member['ref'], 'geometry': {'$exists': True}})
            for child in mongocursor_child:
                if 'geometry' in child and 'type' in document:
                    if document['type']=='multipolygon':
                        if member['role'] == 'outer':
                            outer_coords, outer_polygons = checkGeometry(child, outer_coords, outer_polygons, logger)
                        else:
                            inner_coords, inner_polygons = checkGeometry(child, inner_coords, inner_polygons, logger)
                    elif document['type'] == 'route' or document['type'] == 'route_master' \
                            or document['type'] == 'superroute' or document['type'] == 'restriction' \
                            or document['type'] == 'site' or document['type'] == 'associatedStreet' \
                            or document['type'] == 'public_transport' or document['type'] == 'street' \
                            or document['type'] == 'destination_sign' or document['type'] == 'waterway' \
                            or document['type'] == 'enforcement' or document['type'] == 'bridge' \
                            or document['type'] == 'tunnel':
                        if child['geometry']['type'] == 'GeometryCollection':
                            outer_coords = outer_coords+child['geometry']['geometries']
                        else:
                            outer_coords.append(child['geometry'])
                    elif document['type'] == 'boundary' or document['type'] == 'multilinestring' \
                            or document['type'] == 'osm':
                        if member['role'] == 'outer' or member['role'] == 'inner' or document['type'] == 'multilinestring':
                            if child['geometry']['type'] == 'LineString':
                                outer_coords.append(child['geometry']['coordinates'])
                            elif child['geometry']['type'] == 'Polygon' or child['geometry']['type'] == 'MultiLineString':
                                for coordinate in child['geometry']['coordinates']:
                                    outer_coords.append(coordinate)
                            elif child['geometry']['type'] == 'GeometryCollection':
                                for sgeometry in child['geometry']['geometries']:
                                    if sgeometry['type'] == 'LineString':
                                        outer_coords.append(sgeometry['coordinates'])
                            elif child['geometry']['type'] == 'Point':
                                continue
                            else:
                                CommonFunctions.addLogMessage("Unknown geometry - " + str(child['geometry']) + ": " + str(document), logger, CommonFunctions.INFO_LOG)
                    elif document['type'] == 'building':
                        if child['osm_type'] == 'way':
                            geom['type'] = 'Polygon'
                            geom['coordinates'] = child['geometry']['coordinates']
                    else:
                        if str(document['type'])not in unknown_types:
                            CommonFunctions.addLogMessage("Unknown type - " + str(document['type']), logger, CommonFunctions.INFO_LOG)
                            unknown_types.append(str(document['type']))
                            #CommonFunctions.addLogMessage(str(document['member']), logger, CommonFunctions.INFO_LOG)
                break

        if 'type' in document:
            if document['type'] == 'multipolygon':
                if len(outer_polygons)>0:
                    pols = []
                    for outer_polygon in outer_polygons:
                        vctrow = []
                        vctrow.append(outer_polygon)
                        if len(inner_polygons)>0: vctrow.append(inner_polygons[0])
                        pols.append(vctrow)
                    if len(pols)>0:
                        geom['type'] = 'MultiPolygon'
                        geom['coordinates'] = pols
            elif document['type'] == 'route' or document['type'] == 'route_master' \
                    or document['type'] == 'superroute' or document['type'] == 'restriction' \
                    or document['type'] == 'site' or document['type'] == 'associatedStreet' \
                    or document['type'] == 'public_transport' or document['type'] == 'street' \
                    or document['type'] == 'destination_sign' or document['type'] == 'waterway' \
                    or document['type'] == 'enforcement' or document['type'] == 'bridge' \
                    or document['type'] == 'tunnel' or document['type'] == 'circuit' \
                    or document['type'] == 'land_area' or document['type'] == 'network' \
                    or document['type'] == 'water' or document['type'] == 'collection' \
                    or document['type'] == 'landarea' or document['type'] == 'defaults':
                if len(outer_coords)>0:
                    geom['type'] = 'GeometryCollection'
                    geom['geometries'] = list(outer_coords)
                    del outer_coords[:]
            elif document['type'] == 'boundary' or document['type'] == 'multilinestring':
                if len(outer_coords) > 0:
                    geom['type'] = 'MultiLineString'
                    geom['coordinates'] = list(outer_coords)
                    del outer_coords[:]

        if geom != {} :
            singlerow['geometry'] = geom
            rowslist.append(singlerow)
        if len(rowslist) == int(element_at_time/10):
            save_packages.append(list(rowslist))
            CommonFunctions.addLogMessage("Package " + str(len(save_packages)) + " of " + str(number_of_processors) + " prepared", logger, CommonFunctions.INFO_LOG)
            if len(save_packages) == number_of_processors:
                processes = []
                for save_package in save_packages:
                    p = threading.Thread(target=saveOSMToMongo, args=(list(save_package), logger, db_server, db_port, db_database, db_collection))
                    p.start()
                    processes.append(p)
                    del save_package
                for process in processes:
                    process.join()
                del processes
                del save_packages[:]
            del rowslist[:]

    if len(rowslist) > 0:
        save_packages.append(list(rowslist))
        processes = []
        for save_package in save_packages:
            p = threading.Thread(target=saveOSMToMongo, args=(list(save_package), logger, db_server, db_port, db_database, db_collection))
            p.start()
            processes.append(p)
            del save_package
        for process in processes:
            process.join()
        del processes
        del save_packages[:]

        del rowslist[:]

    del collection
    del db
    client.close()
    del client

def checkGeometry(docsearch, coords_list, geoms_list, logger):
    if docsearch['geometry']['type'] == 'LineString':
        coords_list = coords_list + docsearch['geometry']['coordinates']
        if len(coords_list) > 0:
            if coords_list[0] == coords_list[len(coords_list) - 1]:
                geoms_list.append(list(coords_list))
                del coords_list[:]
    elif docsearch['geometry']['type'] == 'Polygon':
        geoms_list.append(docsearch['geometry']['coordinates'][0])
    elif docsearch['geometry']['type'] == 'MultiLineString' or docsearch['geometry']['type'] == 'MultiPolygon':
        for cds in docsearch['geometry']['coordinates']:
            geoms_list.append(cds)
    elif docsearch['geometry']['type'] == 'Point':
        coords_list.append(docsearch['geometry'])
    else:
        CommonFunctions.addLogMessage("Unknown check geometry - " + str(docsearch['geometry']['type'] + ": " + str(docsearch)), logger, CommonFunctions.INFO_LOG)
        CommonFunctions.addLogMessage("Unknown geoms_list - " + str(geoms_list), logger, CommonFunctions.INFO_LOG)
    return coords_list, geoms_list
