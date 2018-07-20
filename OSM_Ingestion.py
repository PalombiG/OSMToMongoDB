import bz2
import multiprocessing
import os
import shutil
import sys
import threading
from lxml import etree
from configparser import ConfigParser
from commons import CommonFunctions, OSM_Commons
from pymongo import MongoClient, GEOSPHERE, ASCENDING


def uncompressFile (file, logger, done_path, db_server, db_port, db_database, db_collection):
    CommonFunctions.addLogMessage('Uncompress file: ' + file, logger, CommonFunctions.INFO_LOG)
    file_transformation = file.split(".")
    del file_transformation[file_transformation.__len__() - 1]
    osmfilepath = '.'.join(file_transformation)

    if (os.path.exists(osmfilepath)): os.remove(osmfilepath)
    with open(osmfilepath, 'wb') as new_file:
        with open(file, 'rb') as filebz2:
            decompressor = bz2.BZ2Decompressor()
            for block in iter(lambda: filebz2.read(900 * 1024), b''):
                new_file.write(decompressor.decompress(block))

    CommonFunctions.addLogMessage('Uncompress file finished: ' + file, logger, CommonFunctions.INFO_LOG)
    fname = file.replace("\\","/")
    fname = fname.split("/")[len(fname.split("/"))-1]
    CommonFunctions.addLogMessage('Move file to done directory: ' + fname, logger, CommonFunctions.INFO_LOG)
    shutil.move(file.replace("\\","/"), done_path + "/" + fname)
    processOSMFile(osmfilepath, logger, db_server, db_port, db_database, db_collection)

    OSM_Commons.updateWays(db_server, int(db_port), db_database, db_collection, logger, element_at_time)
    OSM_Commons.updateRelations(db_server, int(db_port), db_database, db_collection, logger, element_at_time)

    os.remove(osmfilepath)
    return True

def processOSMFile(file, logger, db_server, db_port, db_database, db_collection):
    save_packages = []
    number_of_processors = multiprocessing.cpu_count()
    CommonFunctions.addLogMessage('Process file: ' + file, logger, CommonFunctions.INFO_LOG)
    client = MongoClient(db_server, db_port)
    db = client[db_database]
    collection = db[db_collection]
    CommonFunctions.addLogMessage('Read elements from: ' + file, logger, CommonFunctions.INFO_LOG)
    osm_nodes = etree.iterparse(file, events=('start','end'))
    rowslist = []
    singlerow = {}
    childs = []

    for event, elem in osm_nodes:
        if event=='start':
            if elem.tag=='node' or elem.tag=='way' or elem.tag=='relation':
                singlerow = {}
                singlerow["osm_type"] = elem.tag
                childs = []
                for key in elem.attrib.keys():
                    singlerow[key] = elem.attrib[key]

            elif elem.tag == 'tag':
                if "k" in elem.attrib:
                    keyname = str(elem.attrib["k"]).replace(":", "_")
                    keyvalue = str(elem.attrib["v"])
                    singlerow[keyname] = keyvalue

            elif elem.tag == 'nd' or elem.tag == 'member':
                if elem.tag == 'member' and elem.attrib['type']=='relation': insertDelay = True
                singletag = {}
                for key in elem.attrib.keys():
                    singletag[key] = elem.attrib[key]
                childs.append(singletag)
                if (len(childs) > 0): singlerow[elem.tag] = childs

        elif event=='end':
            if elem.tag == 'node' or elem.tag == 'way' or elem.tag == 'relation':
                if elem.tag == 'node':
                    geom = {}
                    geom['type'] = 'Point'
                    geom['coordinates'] = [float(elem.attrib['lon']),float(elem.attrib['lat'])]
                    singlerow['geometry'] = geom

                rowslist.append(singlerow)
                if len(rowslist) == element_at_time:
                    save_packages.append(list(rowslist))
                    CommonFunctions.addLogMessage("Package " + str(len(save_packages)) + " of " + str(number_of_processors) + " prepared", logger, CommonFunctions.INFO_LOG)
                    if len(save_packages) == number_of_processors:
                        processes = []
                        for save_package in save_packages:
                            p = threading.Thread(target=OSM_Commons.saveOSMToMongo, args=(list(save_package), logger, db_server, db_port, db_database, db_collection, True))
                            p.start()
                            processes.append(p)
                            del save_package
                        for process in processes:
                            process.join()
                        del processes
                        del save_packages[:]
                    del rowslist[:]
                del childs

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    del osm_nodes
    if (len(rowslist) > 0):
        save_packages.append(list(rowslist))
        processes = []
        for save_package in save_packages:
            p = threading.Thread(target=OSM_Commons.saveOSMToMongo, args=(list(save_package), logger, db_server, db_port, db_database, db_collection, True))
            p.start()
            processes.append(p)
            del save_package
        for process in processes:
            process.join()
        del processes
        del save_packages[:]
    del rowslist
    del collection
    del db
    client.close()
    del client

def main():
    pythonPath = os.path.dirname(os.path.realpath(sys.argv[0]))
    settings = ConfigParser()
    settings.read(pythonPath + "/settings.ini")

    if not os.path.exists("log"): os.mkdir("log")
    logger = CommonFunctions.getLogger("OSMIngestion", pythonPath, "log")

    global element_at_time
    element_at_time = int(CommonFunctions.readParameter(settings, "general", 'element_at_time'))

    input_path = CommonFunctions.readParameter(settings, "directories", 'input_path')
    done_path = CommonFunctions.readParameter(settings, "directories", 'done_path')

    db_server = CommonFunctions.readParameter(settings, "database", 'db_server')
    db_port = CommonFunctions.readParameter(settings, "database", 'db_port')
    db_database = CommonFunctions.readParameter(settings, "database", 'db_database')
    db_collection = CommonFunctions.readParameter(settings, "database", 'db_collection')

    client = MongoClient(db_server, int(db_port))
    db = client[db_database]
    collection = db[db_collection]
    collection.create_index([("id", ASCENDING)], background=True)
    collection.create_index([("osm_type", ASCENDING)], background=True)
    collection.ensure_index([("geometry", GEOSPHERE)], background=True)
    collection.create_index([("geometry.type", ASCENDING)], background=True)
    collection.ensure_index([("nd.ref", ASCENDING)], background=True)
    collection.ensure_index([("member.ref", ASCENDING)], background=True)
    collection.create_index([("osm_type", ASCENDING), ("geometry", ASCENDING)], background=True)
    collection.create_index([("osm_type", ASCENDING), ("id", ASCENDING)], background=True)

    number_of_processors = multiprocessing.cpu_count()
    CommonFunctions.addLogMessage("Start OSM Ingestion with " + str(number_of_processors) + " processors", logger, CommonFunctions.INFO_LOG)

    #global tp
    #tp = ThreadPool()
    #tp.init(logger, number_of_processors)

    CommonFunctions.addLogMessage('Check Openstreetmap data in folder: ' + input_path, logger, CommonFunctions.INFO_LOG)
    for root, dirs, files in os.walk(input_path):
        for file in files:
            if (file.lower().endswith('.bz2')):
                uncompressFile(os.path.join(root, file), logger, done_path, db_server, int(db_port), db_database, db_collection)

    OSM_Commons.updateWays(db_server, int(db_port), db_database, db_collection, logger, element_at_time)
    for i in range(6):
        OSM_Commons.updateRelations(db_server, int(db_port), db_database, db_collection, logger, element_at_time)

    client.close()
    del collection
    del db
    del client

if __name__ == '__main__':
    main()