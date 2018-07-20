import gzip
import multiprocessing
import os
import shutil
import sys
import threading
import urllib.request
from configparser import ConfigParser
from pymongo import MongoClient, GEOSPHERE, ASCENDING
import CommonFunctions
from lxml import etree
import OSM_Commons

processes = []

def deleteOSMFromMongo(rowslist, logger, elemtype):
    CommonFunctions.addLogMessage('Delete ' + str(len(rowslist)) + ' ' + elemtype + ' from mongo database', logger, CommonFunctions.INFO_LOG)
    bulkop = collection.initialize_ordered_bulk_op()
    for row in rowslist:
        bulkop.find({'ID': row["id"], 'type': row["type"]}).remove()
    retval = bulkop.execute()
    CommonFunctions.addLogMessage(str(len(rowslist)) + ' ' + elemtype + ' removed from mongo database', logger, CommonFunctions.INFO_LOG)

def retrieveGeneric(singlerow, elem, stringname):
    tags = []
    for osm_tag in elem.iter(tag=stringname):
        singletag = {}
        for key in osm_tag.attrib.keys():
            singletag[key] = osm_tag.attrib[key]
        tags.append(singletag)
    if (len(tags) > 0): singlerow[stringname] = tags
    del tags

def processOSCFile(file, logger, db_server, db_port, db_database, db_collection):
    save_packages = []
    number_of_processors = multiprocessing.cpu_count()
    CommonFunctions.addLogMessage('Process file: ' + file, logger, CommonFunctions.INFO_LOG)
    osm_nodes = etree.iterparse(file, events=('start','end'))
    rowslist = []
    singlerow = {}
    childs = []
    action = ""

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

            elif  elem.tag == 'modify' or elem.tag == 'create' or elem.tag == 'delete':
                action = elem.tag

        elif event=='end':
            if elem.tag == 'node' or elem.tag == 'way' or elem.tag == 'relation':
                if action!="delete":
                    if elem.tag == 'node':
                        geom = {}
                        geom['type'] = 'Point'
                        geom['coordinates'] = [float(elem.attrib['lon']),float(elem.attrib['lat'])]
                        singlerow['geometry'] = geom
                else:
                    singlerow['___remove___'] = True

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

    OSM_Commons.updateWays(db_server, int(db_port), db_database, db_collection, logger, element_at_time)
    OSM_Commons.updateRelations(db_server, int(db_port), db_database, db_collection, logger, element_at_time)

def checkUrl(upload_url, last_page, last_package, logger, download_path, package_name, db_server, db_port, db_database, db_collection):
    CommonFunctions.addLogMessage('Check Url: ' + upload_url, logger, CommonFunctions.INFO_LOG)
    maxpage = last_page
    maxpackage = last_package
    with urllib.request.urlopen(upload_url) as conn:
        html_file = conn.read()

        checknumbers = str(html_file).split('/">')
        for checknumber in checknumbers:
            checkvalue = checknumber.split('href="')[len(checknumber.split('href="'))-1]
            if CommonFunctions.RepresentsInt(checkvalue):
                pagechk, packagechk = checkUrl(upload_url+checkvalue+"/", last_page, last_package, logger, download_path, package_name, db_server, db_port, db_database, db_collection)
                if (pagechk * 10000) + packagechk > (maxpage * 10000) + maxpackage:
                    maxpage = pagechk
                    maxpackage = packagechk
        if '.osc.gz">' in str(html_file):
            pagenumber = int(upload_url.split("/")[len(upload_url.split("/"))-2])
            checklinks = str(html_file).split('.osc.gz">')
            for checklink in checklinks:
                checkvalue = checklink.split('href="')[len(checklink.split('href="'))-1]
                if CommonFunctions.RepresentsInt(checkvalue):
                    checkvalue = int(checkvalue)
                    if (pagenumber*10000)+checkvalue > (maxpage*10000)+maxpackage:
                        package_url = upload_url + str(checkvalue) + ".osc.gz"
                        file_destination = os.path.join(download_path, str(checkvalue) + ".osc.gz").replace("\\","/")
                        CommonFunctions.addLogMessage('Download package: ' + package_url, logger, CommonFunctions.INFO_LOG)
                        if (os.path.exists(file_destination)): os.remove(file_destination)
                        urllib.request.urlretrieve(package_url, file_destination)
                        CommonFunctions.addLogMessage('Uncompress package: ' + file_destination, logger, CommonFunctions.INFO_LOG)
                        file_transformation = file_destination.split(".")
                        del file_transformation[file_transformation.__len__() - 1]
                        oscfilepath = '.'.join(file_transformation)
                        if (os.path.exists(oscfilepath)): os.remove(oscfilepath)
                        with gzip.open(file_destination, 'rb') as f_in:
                            with open(oscfilepath, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)

                        processOSCFile(oscfilepath, logger, db_server, db_port, db_database, db_collection)
                        os.remove(oscfilepath)
                        os.remove(file_destination)

                        maxpage = pagenumber
                        maxpackage = checkvalue

                        result = uploads.update({'name': package_name}, {'$set': {'last_page': maxpage, 'last_package': maxpackage}}, upsert=True)

    return int(maxpage), int(maxpackage)

def main():
    pythonPath = os.path.dirname(os.path.realpath(sys.argv[0]))
    settings = ConfigParser()
    settings.read(pythonPath + "/settings.ini")

    if not os.path.exists("log"): os.mkdir("log")
    logger = CommonFunctions.getLogger("OSMIngestion", pythonPath, "log")

    global element_at_time
    element_at_time = int(CommonFunctions.readParameter(settings, "general", 'element_at_time'))
    number_of_processors = int(CommonFunctions.readParameter(settings, "general", 'number_of_processors'))

    input_path = CommonFunctions.readParameter(settings, "directories", 'input_path')
    done_path = CommonFunctions.readParameter(settings, "directories", 'done_path')
    download_path = CommonFunctions.readParameter(settings, "directories", 'download_path')

    db_server = CommonFunctions.readParameter(settings, "database", 'db_server')
    db_port = CommonFunctions.readParameter(settings, "database", 'db_port')
    db_database = CommonFunctions.readParameter(settings, "database", 'db_database')
    db_uploads = CommonFunctions.readParameter(settings, "database", 'db_uploads')
    db_collection = CommonFunctions.readParameter(settings, "database", 'db_collection')

    client = MongoClient(db_server, int(db_port))
    db = client[db_database]
    global uploads
    uploads = db[db_uploads]
    global collection
    collection = db[db_collection]
    collection.create_index([("ID", ASCENDING)])
    collection.ensure_index([("geometry", GEOSPHERE)])

    updates_file = settings.get('database', 'updates_file')

    content = ''
    with open(updates_file) as f:
        content = f.readlines()

    for single in content:
        single = single.replace("\n", "")
        singleparams = single.split(",")
        package_name = singleparams[0]
        mongocursor = uploads.find({'name': package_name})
        upload_url = ''
        last_page = 0
        last_package = 0
        for document in mongocursor:
            upload_url = document['upload_url']
            last_page = document['last_page']
            last_package = document['last_package']
        if upload_url == '':
            upload_url = singleparams[1]
            last_page = 0
            last_package = 0
            uploads.insert({'name': package_name, 'upload_url': upload_url, 'last_page': last_page, 'last_package': last_package})

        last_page, last_package = checkUrl(upload_url, last_page, last_package, logger, download_path, package_name, db_server, db_port, db_database, db_collection)

    for p in processes:
        p.join()

    OSM_Commons.updateWays(db_server, int(db_port), db_database, db_collection, logger, element_at_time)
    for i in range(6):
        OSM_Commons.updateRelations(db_server, int(db_port), db_database, db_collection, logger, element_at_time)

    del db
    client.close()
    del client

if __name__ == '__main__':
    main()