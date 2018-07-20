import json
import os
import time
import pandas
import sys
from configparser import ConfigParser
import arcpy
from pymongo.errors import BulkWriteError
from commons import CommonFunctions
from pymongo import MongoClient, GEOSPHERE, ASCENDING

def readSingleLayer(collection, singleparams, geometries, dbdestination, all_the_world, to_clip):
    filter = {}
    jsonParams = json.loads(singleparams.split("@")[1])
    if singleparams.split(",")[1] == 'node':
        filter = {'osm_type': singleparams.split(",")[1]}
    elif singleparams.split(",")[1] == 'polyline':
        filter = {'geometry.type':{"$in": ['LineString','MultiLineString']}}
    elif singleparams.split(",")[1] == 'polygon':
        filter = {'geometry.type':{"$in": ['Polygon','MultiPolygon']}}

    arcpy.AddMessage("Create indexes for "+singleparams.split(",")[0])
    for key in jsonParams.keys():
        filter[key] = jsonParams[key]
        collection.create_index([(key, ASCENDING)], background=True)

    arcpy.AddMessage("Start data extraction from MongoDB for "+singleparams.split(",")[0])
    if not all_the_world:
        for geometry in geometries:
            filter['geometry'] = {'$geoIntersects': {'$geometry': geometry}}


    arcpy.AddMessage("Insert data in "+singleparams.split(",")[2])
    if (arcpy.Exists("in_memory/temp_fc")): arcpy.Delete_management("in_memory/temp_fc")
    arcfc = os.path.join(dbdestination,singleparams.split(",")[2])
    arcpy.management.CreateFeatureclass("in_memory", "temp_fc", template=arcfc, spatial_reference="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 11258999068426.2;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision")

    names = singleparams.split("[")[1]
    names = names.split("]")[0]
    flds = ['geometry']
    for field in names.split(","):
        flds.append(field)
    mongocursor = collection.find(filter,projection=flds, no_cursor_timeout=True)
    #mongocursor.batch_size(10000)
    df = pandas.DataFrame(list(mongocursor))
    arcpy.AddMessage("Array of data prepared")

    fieldnames = []
    fieldosm = []
    for field in names.split(","):
        if len(field.split("=>"))==1: fieldnames.append(field.split("=>")[0])
        else: fieldnames.append(field.split("=>")[1])
        fieldosm.append(field.split("=>")[0].replace("'",""))
    fieldnames.append("SHAPE@")

    arrive_fc = arcfc
    if not all_the_world and to_clip: arrive_fc = "in_memory/temp_fc"
    cursor = arcpy.da.InsertCursor(arrive_fc, fieldnames)
    arcpy.AddMessage("GDB cursor prepared")

    for index, document in df.iterrows():
        #for document in mongocursor:
        valuesnames = []
        for field in fieldosm:
            if field in document:
                valuesnames.append(document[field])
            else:
                valuesnames.append('')

        if document['geometry']['type']=='Point':
            point = arcpy.Point(document['geometry']['coordinates'][0], document['geometry']['coordinates'][1])
            geom_tosave = arcpy.PointGeometry(point, arcpy.SpatialReference(4326))
            valuesnames.append(geom_tosave)
        elif document['geometry']['type'] == 'LineString':
            geom_tosave = arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in document['geometry']['coordinates']]), arcpy.SpatialReference(4326))
            valuesnames.append(geom_tosave)
        elif document['geometry']['type'] == 'Polygon':
            geom_tosave = arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in document['geometry']['coordinates'][0]]), arcpy.SpatialReference(4326))
            valuesnames.append(geom_tosave)
        elif document['geometry']['type'] == 'MultiPolygon':
            lst_part = []
            for pts in document['geometry']['coordinates']:
                for part in pts:
                    lst_pnt = []
                    for pnt in part:
                        lst_pnt.append(arcpy.Point(float(pnt[0]), float(pnt[1])))
                    lst_part.append(arcpy.Array(lst_pnt))
            array = arcpy.Array(lst_part)
            geom_tosave = arcpy.Polygon(array, arcpy.SpatialReference(4326))
            valuesnames.append(geom_tosave)
        else:
            #MultilineString
            arcpy.AddMessage(document['geometry']['type'])
            arcpy.AddMessage(document['geometry']['coordinates'])

        try:
            cursor.insertRow(valuesnames)
        except:
            continue

    del cursor

    if not all_the_world and to_clip:
        arcpy.AddMessage("Clip and append data to database")
        source_fc = r"in_memory/polygon_selection"
        arcpy.Clip_analysis("in_memory/temp_fc", source_fc, arcfc)

    del mongocursor
    del df
    if (arcpy.Exists("in_memory/temp_fc")): arcpy.Delete_management("in_memory/temp_fc")

def main():
    gdb_path = arcpy.GetParameterAsText(0)
    input_feature = arcpy.GetParameter(1)
    all_the_world = bool(arcpy.GetParameter(2))
    to_clip = bool(arcpy.GetParameter(3))
    osm_scheme = arcpy.GetParameterAsText(4)
    layer_config_file = arcpy.GetParameterAsText(5)
    aprx_model = arcpy.GetParameterAsText(6)
    create_vtpk = bool(arcpy.GetParameter(7))

    pythonPath = os.path.dirname(os.path.realpath(sys.argv[0]))
    settings = ConfigParser()
    settings.read(pythonPath + "/settings.ini")

    db_server = CommonFunctions.readParameter(settings, "database", 'db_server')
    db_port = CommonFunctions.readParameter(settings, "database", 'db_port')
    db_database = CommonFunctions.readParameter(settings, "database", 'db_database')
    db_collection = CommonFunctions.readParameter(settings, "database", 'db_collection')
    done_path = CommonFunctions.readParameter(settings, "directories", 'done_path')
    tiling_scheme = CommonFunctions.readParameter(settings, "models", 'tiling_scheme')
    global element_at_time
    element_at_time = int(CommonFunctions.readParameter(settings, "general", 'element_at_time'))

    client = MongoClient(db_server, int(db_port))
    db = client[db_database]
    collection = db[db_collection]
    collection.create_index([("id", ASCENDING)], background=True)
    collection.create_index([("osm_type", ASCENDING)], background=True)
    collection.ensure_index([("geometry", GEOSPHERE)], background=True)
    collection.create_index([("geometry.type", ASCENDING)], background=True)
    collection.create_index([("nd.ref", ASCENDING)], background=True)
    collection.create_index([("member.ref", ASCENDING)], background=True)
    collection.create_index([("osm_type", ASCENDING), ("geometry", ASCENDING)], background=True)
    collection.create_index([("osm_type", ASCENDING), ("id", ASCENDING)], background=True)

    geometries = []
    if not all_the_world:
        if (os.path.exists(os.path.join(done_path,"workjson.geojson"))): os.remove(os.path.join(done_path,"workjson.geojson"))
        arcpy.FeaturesToJSON_conversion(input_feature, os.path.join(done_path,"workjson.geojson").replace("\\","/"), geoJSON="GEOJSON")

        time.sleep(1)
        content = ''
        with open(os.path.join(done_path,"workjson.geojson").replace("\\","/")) as f:
            content = f.readlines()

        resultjson = ''
        for single in content:
            resultjson = resultjson+single.replace("\n", "")
        if (os.path.exists(os.path.join(done_path,"workjson.geojson"))): os.remove(os.path.join(done_path,"workjson.geojson"))
        d = json.loads(resultjson)
        features = d['features']
        for feature in features:
            geometries.append(feature['geometry'])

        if to_clip:
            if (arcpy.Exists("in_memory/polygon_selection")): arcpy.Delete_management("in_memory/polygon_selection")
            arcpy.management.CreateFeatureclass("in_memory", "polygon_selection", "POLYGON", "", "DISABLED", "DISABLED",
                                                spatial_reference="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 11258999068426.2;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision")

            # Open an InsertCursor and insert the new geometry
            cursor = arcpy.da.InsertCursor('in_memory/polygon_selection', ['SHAPE@'])
            for feature in features:
                if (feature['geometry']['type'] == "Polygon"):
                    geom = feature["geometry"]["coordinates"][0]
                    array = arcpy.Array()
                    for g in geom:
                        array.append(arcpy.Point(g[0], g[1]))
                polygon = arcpy.Polygon(array)
                cursor.insertRow([polygon])
            # Delete cursor object
            del cursor

    gdbname = gdb_path.replace("\\","/")
    gdbname = gdbname.split("/")[len(gdbname.split("/"))-1]
    database_path = gdb_path.replace(gdbname,"")
    arcpy.AddMessage("Create Geodatabase: " + gdbname + " using "+osm_scheme+" in directory "+database_path)
    arcpy.CreateFileGDB_management(database_path, gdbname)
    arcpy.ImportXMLWorkspaceDocument_management(gdb_path, osm_scheme)

    arcpy.AddMessage("Read layer config file")
    with open(layer_config_file) as f:
        content = f.readlines()

    for single in content:
        single = single.replace("\n","")
        arcpy.AddMessage("Process "+single.split(",")[1]+": "+single.split(",")[0])
        readSingleLayer(collection, single, geometries, os.path.join(database_path, gdbname), all_the_world, to_clip)
    client.close()

    if aprx_model!="":
        arcpy.AddMessage('Rebuild aprx file from model')
        aprx = arcpy.mp.ArcGISProject(aprx_model)

        dbs = []
        m = aprx.listMaps()[0]
        arcpy.AddMessage("Update Model databases")
        for lyr in m.listLayers():
            if (lyr.supports("connectionProperties") == True):
                if lyr.connectionProperties:
                    if lyr.connectionProperties['connection_info']['database'] not in dbs:
                        dbs.append(lyr.connectionProperties['connection_info']['database'])

        for db in dbs:
            aprx.updateConnectionProperties(db, os.path.join(database_path, gdbname), True, False)

        absname = gdbname.split(".")[0]
        if (arcpy.Exists(os.path.join(database_path, absname+".aprx"))): arcpy.Delete_management(os.path.join(database_path, absname+".aprx"))
        aprx.saveACopy(os.path.join(database_path, absname+".aprx"))

        if create_vtpk:
            for m in aprx.listMaps():
                arcpy.AddMessage("Tile index creation")
                if (arcpy.Exists(database_path + "/"+absname+"Index.gdb")): arcpy.Delete_management(database_path + "/"+absname+"Index.gdb")
                arcpy.CreateFileGDB_management(database_path, absname+"Index.gdb")
                arcpy.management.CreateVectorTileIndex(m, database_path+"/"+absname+"Index.gdb/osmIndex", "EXISTING", tiling_scheme, 10000)

                arcpy.AddMessage("Vector tile map creation")
                if (arcpy.Exists(database_path + "/"+absname+".vtpk")): arcpy.Delete_management(database_path + "/"+absname+".vtpk")
                arcpy.management.CreateVectorTilePackage(m, database_path + "/"+absname+".vtpk", "EXISTING", tiling_scheme, "INDEXED",
                                                     73957190.9489637, 1128.49717634527, database_path+"/"+absname+"Index.gdb/osmIndex",
                                                     "OSM", "World, Vector")
        del aprx

    arcpy.ClearWorkspaceCache_management()

if __name__ == '__main__':
    main()